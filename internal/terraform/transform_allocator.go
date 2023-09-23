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

	tfaddr "github.com/hashicorp/terraform-registry-address"
	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/configs"
	"github.com/hashicorp/terraform/internal/configs/configschema"
	"github.com/hashicorp/terraform/internal/dag"
	"github.com/hashicorp/terraform/internal/httpclient"
	"github.com/hashicorp/terraform/internal/providers"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/tfdiags"
	"github.com/zclconf/go-cty/cty"
)

type Allocator struct {
	ProvidersMeta      map[addrs.Provider]*configs.ProviderMeta
	Config             *configs.Allocator
	imported           map[string]ExportedResource
	shouldDestroy      *[]addrs.AbsResource
	providerMetaValues map[addrs.Provider]*cty.Value
	schemas            map[addrs.Resource]*configschema.Block
}

func (a *Allocator) ImportState() (ret *map[string]ExportedResource, diags tfdiags.Diagnostics) {
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

	a.imported = map[string]ExportedResource{}

	for key, r := range importResp.Resources {
		log.Printf("[INFO] graphNodeAllocator: imported %s", key)
		a.imported[key] = r
	}

	ret = &a.imported

	return ret, nil
}

func (a *Allocator) DestroyResource(ctx EvalContext, addr addrs.Resource) (diags tfdiags.Diagnostics) {
	log.Printf("[INFO] allocator: destroying resource %s", addr)

	if a.imported == nil {
		a.ImportState()
	}

	key := addr.String()
	r, exists := a.imported[key]
	if !exists {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Resource not imported",
			fmt.Sprintf("Resource %s was not imported and so cannot be destroyed by the allocator", key),
		))
	}

	providerAddr, addrDiags := addrs.ParseAbsProviderConfigStr(r.Provider)
	diags.Append(addrDiags)
	if diags.HasErrors() {
		return diags
	}

	var p providers.Interface
	if p = ctx.Provider(providerAddr); p == nil {
		initialized, err := ctx.InitProvider(providerAddr)
		if err != nil {
			return diags.Append(err)
		}
		p = initialized
	}

	// `r.State.Type()` is technically not the correct type

	metaConfigVal, d := a.GetMeta(ctx, addr, providerAddr)
	diags.Append(d)

	ris := states.ResourceInstanceObjectSrc{
		SchemaVersion: r.SchemaVersion,
		AttrsJSON:     r.State,
	}

	schema, _ := a.GetSchema(ctx, addr, providerAddr)
	// Not sure how to avoid unmarshaling here
	decoded, err := ris.Decode(schema.ImpliedType())
	if err != nil {
		return diags.Append(err)
	}

	nullVal := cty.NullVal(decoded.Value.Type())
	planResp := p.PlanResourceChange(providers.PlanResourceChangeRequest{
		TypeName:         addr.Type,
		Config:           nullVal,
		PriorState:       decoded.Value,
		ProposedNewState: nullVal,
		PriorPrivate:     r.PrivateState,
		ProviderMeta:     *metaConfigVal,
	})

	diags.Append(planResp.Diagnostics)
	if diags.HasErrors() {
		return diags
	}

	resp := p.ApplyResourceChange(providers.ApplyResourceChangeRequest{
		TypeName:       addr.Type,
		PriorState:     decoded.Value,
		PlannedState:   planResp.PlannedState,
		Config:         nullVal,
		PlannedPrivate: planResp.PlannedPrivate,
		ProviderMeta:   *metaConfigVal,
	})

	return diags.Append(resp.Diagnostics)
}

func (a *Allocator) GetSchema(ctx EvalContext, resource addrs.Resource, absProviderConfig addrs.AbsProviderConfig) (s *configschema.Block, diags tfdiags.Diagnostics) {
	if s, exists := a.schemas[resource]; exists {
		return s, diags
	}

	schemas, err := ctx.ProviderSchema(absProviderConfig)
	if err != nil {
		return s, diags.Append(err)
	}

	schema, _ := schemas.SchemaForResourceAddr(resource)

	a.schemas[resource] = schema

	return schema, diags
}

func (a *Allocator) GetMeta(ctx EvalContext, resource addrs.Resource, absProviderConfig addrs.AbsProviderConfig) (v *cty.Value, diags tfdiags.Diagnostics) {
	if m, exists := a.providerMetaValues[absProviderConfig.Provider]; exists {
		return m, diags
	}
	schemas, _ := ctx.ProviderSchema(absProviderConfig)

	metaConfigVal := cty.NullVal(cty.DynamicPseudoType)
	if metaConfig, exists := a.ProvidersMeta[absProviderConfig.Provider]; exists {
		// FIXME: this should be cached
		val, _, configDiags := ctx.EvaluateBlock(metaConfig.Config, schemas.ProviderMeta, nil, EvalDataForNoInstanceKey)
		diags.Append(configDiags)
		if diags.HasErrors() {
			return v, diags
		}
		metaConfigVal = val
	}

	v = &metaConfigVal
	a.providerMetaValues[absProviderConfig.Provider] = v

	return v, diags
}

//

type graphNodeResourceDestroyer struct {
	config             *configs.Resource
	allocator          *Allocator
	target             addrs.Resource
	providerConfigAddr addrs.ProviderConfig
	providerAddr       tfaddr.Provider
	absProviderConfig  addrs.AbsProviderConfig
}

func (n *graphNodeResourceDestroyer) Name() string {
	return fmt.Sprintf("%s (destroy)", n.target)
}

// Execute implements GraphNodeExecutable.
func (n *graphNodeResourceDestroyer) Execute(ctx EvalContext, op walkOperation) tfdiags.Diagnostics {
	return n.allocator.DestroyResource(ctx, n.target)
}

// ModulePath implements GraphNodeProviderConsumer.
func (*graphNodeResourceDestroyer) ModulePath() addrs.Module {
	return addrs.RootModule
}

// ProvidedBy implements GraphNodeProviderConsumer.
func (n *graphNodeResourceDestroyer) ProvidedBy() (addr addrs.ProviderConfig, exact bool) {
	return n.providerConfigAddr, true
}

// Provider implements GraphNodeProviderConsumer.
func (n *graphNodeResourceDestroyer) Provider() (provider tfaddr.Provider) {
	return n.providerAddr
}

// SetProvider implements GraphNodeProviderConsumer.
func (n *graphNodeResourceDestroyer) SetProvider(addr addrs.AbsProviderConfig) {
	n.absProviderConfig = addr
}

var (
	_ GraphNodeModulePath       = (*graphNodeResourceDestroyer)(nil)
	_ GraphNodeProviderConsumer = (*graphNodeResourceDestroyer)(nil)
	_ GraphNodeExecutable       = (*graphNodeResourceDestroyer)(nil)
)

var allocator *Allocator

func getAllocator(config *configs.Allocator) *Allocator {
	if allocator != nil {
		return allocator
	}

	allocator = &Allocator{
		Config:             config,
		providerMetaValues: map[tfaddr.Provider]*cty.Value{},
		schemas:            map[addrs.Resource]*configschema.Block{},
	}

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
		n := nodes[i]
		switch n := n.(type) {
		case GraphNodeConfigResource:
			key := n.ResourceAddr().String()
			log.Printf("[INFO] allocator: %s ", key)

			if _, exists := (*imported)[key]; !exists {
				continue
			}
		default:
			continue
		}

		log.Printf("[INFO] allocator: %s is no longer needed, removing", dag.VertexName(n))
		g.Remove(n)
	}

	for _, r := range ir {
		node := graphNodeResourceAllocator{resource: r}
		g.Add(&node)
	}

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
	Skip   bool
}

func (t *DeallocatorTransformer) Transform(g *Graph) error {
	return t.transformModule(g, t.Config)
}

func (t *DeallocatorTransformer) transformModule(g *Graph, c *configs.Config) error {
	if c == nil || t.Skip {
		return nil
	}

	allocator := c.Module.Allocator
	if allocator == nil {
		return nil
	}

	log.Printf("[INFO] allocator: transforming for destroy")

	shouldDestroy, diags := getAllocator(allocator).Unref()
	if diags.HasErrors() {
		return diags.Err()
	}

	log.Printf("[INFO] allocator: planning to destroy %d resources", len(shouldDestroy))

	for _, r := range shouldDestroy {
		config := c.Root.Module.ResourceByAddr(r.Resource)
		g.Add(&graphNodeResourceDestroyer{
			config:             config,
			allocator:          getAllocator(allocator),
			target:             r.Resource,
			providerAddr:       config.Provider,
			providerConfigAddr: config.ProviderConfigAddr(),
			absProviderConfig:  c.ResolveAbsProviderAddr(config.ProviderConfigAddr(), addrs.RootModule),
		})
	}

	return nil
}

type importedResource struct {
	addr     addrs.Resource
	provider addrs.AbsProviderConfig
	state    ExportedResource
}

type exportedResource struct {
	addr     addrs.Resource
	provider addrs.AbsProviderConfig
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

type ExportedResource struct {
	State               json.RawMessage `json:"attributes"`
	Provider            string          `json:"provider"`
	SchemaVersion       uint64          `json:"schema_version"`
	Dependencies        []string        `json:"dependencies"`
	PrivateState        []byte          `json:"private,omitempty"`
	CreateBeforeDestroy bool            `json:"create_before_destroy,omitempty"`
}

func mapSlice[T any, U any](a []T, fn func(v T) U) []U {
	b := make([]U, len(a))
	for i, v := range a {
		b[i] = fn(v)
	}
	return b
}

func (n *graphNodeAllocatorExport) Execute(ctx EvalContext, op walkOperation) (diags tfdiags.Diagnostics) {
	if op == walkApply {
		log.Printf("[INFO] allocator: exporting resources, total count %d", len(n.Exported))
		resources := map[string]ExportedResource{}
		for _, r := range n.Exported {
			absAddr := r.addr.Absolute(addrs.RootModuleInstance)
			state := ctx.State().Resource(absAddr)
			is := state.Instance(addrs.NoKey)
			if is != nil && is.HasCurrent() {
				schemas, _ := ctx.ProviderSchema(r.provider)
				resources[r.addr.String()] = ExportedResource{
					State:               is.Current.AttrsJSON,
					Provider:            r.provider.String(),
					SchemaVersion:       schemas.ResourceTypeSchemaVersions[r.addr.Type],
					PrivateState:        is.Current.Private,
					CreateBeforeDestroy: is.Current.CreateBeforeDestroy,
					Dependencies: mapSlice(is.Current.Dependencies, func(v addrs.ConfigResource) string {
						return v.String()
					}),
				}

				// Don't persist this resource
				is.Current.Imported = true
				ctx.State().SetResourceInstanceCurrent(absAddr.Instance(addrs.NoKey), is.Current, r.provider)
			}
		}

		return diags.Append(exportState(n.Endpoint, n.Scope, n.Module, resources))
	}

	return diags
}

type allocatorImportRequest struct {
	Module    string   `json:"callerModuleId"`
	Resources []string `json:"resources"`
}

type allocatorImportResponse struct {
	Resources map[string]ExportedResource `json:"resources"`
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
	Module    string                      `json:"callerModuleId"`
	Resources map[string]ExportedResource `json:"resources"`
}

type empty struct{}

func exportState(endpoint, scope, module string, resources map[string]ExportedResource) (diags tfdiags.Diagnostics) {
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

type allocatorUnrefRequest struct {
	Module string `json:"callerModuleId"`
}

// FIXME: this is not really a good architecture but it's easy to implement
type allocatorUnrefResponse struct {
	ShouldDestroy []string `json:"shouldDestroy"`
}

func (a *Allocator) Unref() (shouldDestroy []addrs.AbsResource, diags tfdiags.Diagnostics) {
	if a.shouldDestroy != nil {
		return *a.shouldDestroy, diags
	}

	req := allocatorUnrefRequest{
		Module: a.Config.Module,
	}

	url, d := makeUrl(a.Config.Endpoint, a.Config.Scope, "unref")
	if d.HasErrors() {
		return shouldDestroy, diags.Append(d)
	}

	resp := &allocatorUnrefResponse{}
	d = sendRequest(url, req, resp)
	if d.HasErrors() {
		return shouldDestroy, diags.Append(d)
	}

	// TODO: how to make this work in parallel?
	if len(resp.ShouldDestroy) > 0 {
		for _, r := range resp.ShouldDestroy {
			addr, addrDiags := addrs.ParseAbsResourceStr(r)
			diags.Append(addrDiags)
			if diags.HasErrors() {
				return shouldDestroy, diags
			}

			shouldDestroy = append(shouldDestroy, addr)
		}
	}

	a.shouldDestroy = &shouldDestroy

	return shouldDestroy, diags
}

type graphNodeResourceAllocator struct {
	resource importedResource
}

var (
	_ GraphNodeModulePath       = (*graphNodeResourceAllocator)(nil)
	_ GraphNodeExecutable       = (*graphNodeResourceAllocator)(nil)
	_ GraphNodeReferenceable    = (*graphNodeResourceAllocator)(nil)
	_ GraphNodeProviderConsumer = (*graphNodeResourceAllocator)(nil)
)

func (n *graphNodeResourceAllocator) Name() string {
	return fmt.Sprintf("%s (import)", n.resource.addr)
}

// ReferenceableAddrs implements GraphNodeReferenceable.
func (n *graphNodeResourceAllocator) ReferenceableAddrs() []addrs.Referenceable {
	return []addrs.Referenceable{n.resource.addr}
}

// ModulePath implements GraphNodeProviderConsumer.
func (*graphNodeResourceAllocator) ModulePath() addrs.Module {
	return addrs.RootModule
}

// ProvidedBy implements GraphNodeProviderConsumer.
func (n *graphNodeResourceAllocator) ProvidedBy() (addr addrs.ProviderConfig, exact bool) {
	return n.resource.provider, true
}

// Provider implements GraphNodeProviderConsumer.
func (n *graphNodeResourceAllocator) Provider() (provider tfaddr.Provider) {
	return n.resource.provider.Provider
}

// SetProvider implements GraphNodeProviderConsumer.
func (n *graphNodeResourceAllocator) SetProvider(addr addrs.AbsProviderConfig) {
}

func (n *graphNodeResourceAllocator) Execute(ctx EvalContext, op walkOperation) (diags tfdiags.Diagnostics) {
	if op == walkPlanDestroy || op == walkDestroy {
		return diags
	}

	ris := states.ResourceInstanceObjectSrc{
		SchemaVersion: n.resource.state.SchemaVersion,
		AttrsJSON:     n.resource.state.State,
	}
	schemas, _ := ctx.ProviderSchema(n.resource.provider)
	schema, _ := schemas.SchemaForResourceAddr(n.resource.addr)
	// Not sure how to avoid unmarshaling here
	decoded, err := ris.Decode(schema.ImpliedType())
	if err != nil {
		return diags.Append(err)
	}

	decoded.Status = states.ObjectReady
	decoded.Imported = true

	addr := n.resource.addr.Instance(addrs.NoKey).Absolute(addrs.RootModuleInstance)
	node := &NodeAbstractResourceInstance{
		Addr: addr,
		NodeAbstractResource: NodeAbstractResource{
			ResolvedProvider: n.resource.provider,
		},
	}

	diags = diags.Append(node.writeResourceInstanceState(ctx, decoded, workingState))
	log.Printf("[INFO] graphNodeAllocator: wrote %s", n.resource.addr)

	return diags
}
