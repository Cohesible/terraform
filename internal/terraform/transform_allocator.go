// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package terraform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/configs"
	"github.com/hashicorp/terraform/internal/dag"
	"github.com/hashicorp/terraform/internal/httpclient"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/tfdiags"
	"github.com/zclconf/go-cty/cty"
	ctyjson "github.com/zclconf/go-cty/cty/json"
)

type Allocator struct {
	Config   *configs.Allocator
	imported map[string]cty.Value
}

func (a *Allocator) ImportState() (ret *map[string]cty.Value, diags tfdiags.Diagnostics) {
	if a.imported != nil {
		return &a.imported, nil
	}

	resources := a.Config.Resources
	keys := make([]string, len(resources))
	for i, r := range resources {
		key := r.Subject.String()
		keys[i] = key
	}

	req := allocatorImportRequest{
		Module:    a.Config.Module,
		Resources: keys,
	}
	importResp := &allocatorImportResponse{}

	url, d := makeUrl(a.Config.Endpoint, a.Config.Scope, "import")
	if d.HasErrors() {
		return nil, diags.Append(d)
	}

	d = sendRequest(url, req, importResp)
	if d.HasErrors() {
		return nil, diags.Append(d)
	}

	a.imported = map[string]cty.Value{}

	for key, r := range importResp.Resources {
		log.Printf("[INFO] graphNodeAllocator: imported %s", key)
		a.imported[key] = r.Value
	}

	ret = &a.imported

	return ret, nil
}

var allocator *Allocator

func getAllocator(config *configs.Allocator) *Allocator {
	if allocator != nil {
		return allocator
	}

	allocator = &Allocator{Config: config}
	return allocator
}

type AllocatorTransformer struct {
	Config       *configs.Config
	State        *states.State
	ShouldExport bool
}

func (t *AllocatorTransformer) Transform(g *Graph) error {
	return t.transformModule(g, t.Config)
}

func (t *AllocatorTransformer) transformModule(g *Graph, c *configs.Config) error {
	if c == nil {
		return nil
	}

	allocator := c.Module.Allocator
	if allocator == nil {
		return nil
	}

	imported, diags := getAllocator(allocator).ImportState()
	if diags.HasErrors() {
		return diags.Err()
	}

	ir := make([]importedResource, 0)
	rMap := map[string]*configs.Resource{}
	for _, r := range c.Module.ManagedResources {
		addr := r.Addr()
		key := addr.String()
		rMap[key] = r

		if s, exists := (*imported)[key]; exists {
			ir = append(ir, importedResource{
				addr:     addr,
				provider: c.ResolveAbsProviderAddr(r.ProviderConfigAddr(), addrs.RootModule),
				state:    s,
			})
		}
	}

	exported := make([]exportedResource, 0)
	for _, ref := range allocator.Resources {
		key := ref.Subject.String()
		if _, exists := (*imported)[key]; !exists {
			if r, rExists := rMap[key]; rExists {
				exported = append(exported, exportedResource{
					addr:     r.Addr(),
					provider: c.ResolveAbsProviderAddr(r.ProviderConfigAddr(), addrs.RootModule),
				})

				log.Printf("[INFO] allocator: planning to export resource %s", key)
			}
		}
	}

	nodes := g.Vertices()
	for i := 0; i < len(nodes); i++ {
		// run this in a closure, so we can return early rather than
		// dealing with complex looping and labels
		func() {
			n := nodes[i]
			switch n := n.(type) {
			case GraphNodeConfigResource:
				key := n.ResourceAddr().String()
				log.Printf("[INFO] allocator: %s ", key)

				if _, exists := (*imported)[key]; !exists {
					return
				}
			default:
				return
			}

			log.Printf("[INFO] allocator: %s is no longer needed, removing", dag.VertexName(n))
			g.Remove(n)

			// remove the node from our iteration as well
			last := len(nodes) - 1
			nodes[i], nodes[last] = nodes[last], nodes[i]
			nodes = nodes[:last]
		}()
	}

	g.Add(&graphNodeAllocator{
		Resources: ir,
	})

	if len(exported) > 0 && t.ShouldExport {
		g.Add(&graphNodeAllocatorExport{
			Endpoint: allocator.Endpoint,
			Scope:    allocator.Scope,
			Module:   allocator.Module,
			Exported: exported,
		})
	}

	return nil
}

type DeallocatorTransformer struct {
	Config *configs.Config
}

func (t *DeallocatorTransformer) Transform(g *Graph) error {
	return t.transformModule(g, t.Config)
}

func (t *DeallocatorTransformer) transformModule(g *Graph, c *configs.Config) error {
	if c == nil {
		return nil
	}

	allocator := c.Module.Allocator
	if allocator == nil {
		return nil
	}

	g.Add(&graphNodeUnref{
		Module:   allocator.Module,
		Endpoint: allocator.Endpoint,
		Scope:    allocator.Scope,
	})

	return nil
}

type importedResource struct {
	addr     addrs.Resource
	provider addrs.AbsProviderConfig
	state    cty.Value
}

type exportedResource struct {
	addr     addrs.Resource
	provider addrs.AbsProviderConfig
}

type graphNodeAllocator struct {
	Resources []importedResource
}

type graphNodeAllocatorExport struct {
	Endpoint string
	Scope    string
	Module   string
	Exported []exportedResource
}

var (
	_ GraphNodeModulePath = (*graphNodeAllocatorExport)(nil)
	_ GraphNodeExecutable = (*graphNodeAllocatorExport)(nil)
	_ GraphNodeReferencer = (*graphNodeAllocatorExport)(nil)
)

func (n *graphNodeAllocatorExport) Name() string {
	return fmt.Sprintf("allocator (export)")
}

func (n *graphNodeAllocatorExport) ModulePath() addrs.Module {
	return addrs.RootModule
}

func (n *graphNodeAllocatorExport) References() []*addrs.Reference {
	refs := make([]*addrs.Reference, len(n.Exported))
	for i, ref := range n.Exported {
		refs[i] = &addrs.Reference{Subject: ref.addr}
	}

	return refs
}

func (n *graphNodeAllocatorExport) Execute(ctx EvalContext, op walkOperation) (diags tfdiags.Diagnostics) {
	if op == walkApply {
		log.Printf("[INFO] allocator: exporting resources, total count %d", len(n.Exported))
		resources := map[string]interface{}{}
		for _, r := range n.Exported {
			state := ctx.State().Resource(r.addr.Absolute(addrs.RootModuleInstance))
			is := state.Instance(addrs.NoKey)
			if is != nil && is.HasCurrent() {
				schemas, _ := ctx.ProviderSchema(r.provider)
				schema, _ := schemas.SchemaForResourceAddr(r.addr)
				// Not sure how to avoid unmarshaling here
				decoded, _ := is.Current.Decode(schema.ImpliedType())
				resources[r.addr.String()] = ctyjson.SimpleJSONValue{Value: decoded.Value}

				// Don't persist this resource
				is.Current.Imported = true
			}
		}

		return diags.Append(exportState(n.Endpoint, n.Scope, n.Module, resources))
	}

	return diags
}

var (
	_ GraphNodeExecutable = (*graphNodeAllocator)(nil)
)

func (n *graphNodeAllocator) Name() string {
	return fmt.Sprintf("allocator")
}

type allocatorImportRequest struct {
	Module    string   `json:"callerModuleId"`
	Resources []string `json:"resources"`
}

type allocatorImportResponse struct {
	Resources map[string]ctyjson.SimpleJSONValue `json:"resources"`
}

type allocatorErrorResponse struct {
	Message string `json:"message"`
}

func sendRequest[T any, R any](endpoint string, body T, ret R) (diags tfdiags.Diagnostics) {
	client := httpclient.New()
	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(body)
	if err != nil {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Bad encode",
			fmt.Sprintf("Bad encode %s", err),
		))
	}

	httpRequest, _ := http.NewRequest("POST", endpoint, buf)
	httpRequest.Header["Content-Type"] = []string{"application/json"}

	username, _ := os.LookupEnv("TF_HTTP_USERNAME")
	password, _ := os.LookupEnv("TF_HTTP_PASSWORD")
	if username != "" && password != "" {
		httpRequest.SetBasicAuth(username, password)
	}

	resp, err := client.Do(httpRequest)
	if err != nil {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Resource allocator unavailable",
			fmt.Sprintf("The resource allocator failed to respond at %s: %w", endpoint, err),
		))
	}

	if resp.StatusCode >= 300 {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Resource allocator returned non-2xx status code",
			fmt.Sprintf("The resource allocator returned non-2xx status code: %d %s", resp.StatusCode, resp.Status),
		))
	}

	if resp.StatusCode == 204 {
		return diags
	}

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(ret)
	if err != nil {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Resource allocator bad response",
			fmt.Sprintf("Malformed response: %w", err),
		))
	}

	return diags
}

func makeUrl(endpoint, scope, path string) (ret string, diags tfdiags.Diagnostics) {
	url, err := url.Parse(endpoint)
	if err != nil {
		return ret, diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Invalid allocator endpoint",
			fmt.Sprintf("Endpoint is not a url %s: %s", endpoint, err),
		))
	}

	ret = url.JoinPath(path, scope).String()
	return ret, diags
}

type allocatorExportRequest struct {
	Module    string                 `json:"callerModuleId"`
	Resources map[string]interface{} `json:"resources"`
}

type empty struct{}

func exportState(endpoint, scope, module string, resources map[string]interface{}) (diags tfdiags.Diagnostics) {
	req := allocatorExportRequest{
		Module:    module,
		Resources: resources,
	}

	url, d := makeUrl(endpoint, scope, "export")
	if d.HasErrors() {
		return diags.Append(d)
	}

	resp := &empty{}
	d = sendRequest(url, req, resp)
	if d.HasErrors() {
		return diags.Append(d)
	}

	return diags
}

type graphNodeUnref struct {
	Endpoint string
	Scope    string
	Module   string
}

var (
	_ GraphNodeExecutable = (*graphNodeUnref)(nil)
)

func (n *graphNodeUnref) Execute(ctx EvalContext, op walkOperation) (diags tfdiags.Diagnostics) {
	if op != walkDestroy {
		return diags
	}

	return diags.Append(unref(n.Endpoint, n.Scope, n.Module))
}

type allocatorUnrefRequest struct {
	Module string `json:"callerModuleId"`
}

func unref(endpoint, scope, module string) (diags tfdiags.Diagnostics) {
	req := allocatorUnrefRequest{
		Module: module,
	}

	url, d := makeUrl(endpoint, scope, "unref")
	if d.HasErrors() {
		return diags.Append(d)
	}

	resp := &empty{}
	d = sendRequest(url, req, resp)
	if d.HasErrors() {
		return diags.Append(d)
	}

	return diags
}

// GraphNodeExecutable impl.
func (n *graphNodeAllocator) Execute(ctx EvalContext, op walkOperation) (diags tfdiags.Diagnostics) {
	for _, r := range n.Resources {
		if p := ctx.Provider(r.provider); p == nil {
			ctx.InitProvider(r.provider)
		}

		addr := r.addr.Instance(addrs.NoKey).Absolute(addrs.RootModuleInstance)
		node := &NodeAbstractResourceInstance{
			Addr: addr,
			NodeAbstractResource: NodeAbstractResource{
				ResolvedProvider: r.provider,
			},
		}
		rState := &states.ResourceInstanceObject{
			Status:   states.ObjectReady,
			Value:    r.state,
			Imported: true,
		}
		diags = diags.Append(node.writeResourceInstanceState(ctx, rState, workingState))
		log.Printf("[INFO] graphNodeAllocator: wrote %s", r.addr)
	}

	return diags
}
