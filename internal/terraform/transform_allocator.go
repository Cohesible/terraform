// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package terraform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/configs"
	"github.com/hashicorp/terraform/internal/httpclient"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/tfdiags"
	"github.com/zclconf/go-cty/cty"
)

type AllocatorTransformer struct {
	Config *configs.Config
	State  *states.State
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

	imported, diags := importState(allocator.Endpoint, allocator.Scope, allocator.Resources)
	if diags.HasErrors() {
		return diags.Err()
	}

	state := t.State.RootModule()
	for _, r := range c.Module.ManagedResources {
		key := r.Addr().String()
		if s, exists := imported.Resources[key]; exists {
			rState := &states.ResourceInstanceObject{
				Status: states.ObjectReady,
				Value:  s,
			}

			encoded, err := rState.Encode(cty.DynamicPseudoType, 0)
			if err != nil {
				panic(err)
			}

			// rs := state.Resource(r.Addr())
			// if rs == nil {

			// }
			rs := &states.Resource{
				Addr:      r.Addr().Absolute(addrs.RootModuleInstance),
				Instances: map[addrs.InstanceKey]*states.ResourceInstance{},
			}
			state.Resources[key] = rs

			ri := rs.CreateInstance(addrs.NoKey)
			ri.Current = encoded
		}

	}

	// // Also populate locals for child modules
	// for _, cc := range c.Children {
	// 	if err := t.transformModule(g, cc); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

type importedResource struct {
	addr  addrs.AbsResourceInstance
	state cty.Value
}

type graphNodeAllocator struct {
	Scope     string
	Endpoint  string
	Resources []addrs.AbsResourceInstance
	states    []importedResource
}

var (
	_ GraphNodeExecutable        = (*graphNodeAllocator)(nil)
	_ GraphNodeDynamicExpandable = (*graphNodeAllocator)(nil)
)

func (n *graphNodeAllocator) Name() string {
	return fmt.Sprintf("%s (import id %s)", n.Endpoint, n.Scope)
}

type allocatorImportRequest struct {
	Resources []string `json:"resources"`
}

type allocatorImportResponse struct {
	Resources map[string]cty.Value `json:"resources"`
}

func importState(endpoint string, scope string, resources []*addrs.Reference) (ret *allocatorImportResponse, diags tfdiags.Diagnostics) {
	client := httpclient.New()
	url, err := url.Parse(endpoint)
	if err != nil {
		return nil, diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Invalid allocator endpoint",
			fmt.Sprintf("Endpoint is not a url %s: %s", endpoint, err),
		))
	}

	addrMap := make(map[string]*addrs.Reference, len(resources))
	keys := make([]string, len(resources))
	for i, r := range resources {
		key := r.Subject.String()
		keys[i] = key
		addrMap[key] = r
	}

	req := allocatorImportRequest{
		Resources: keys,
	}

	buf := new(bytes.Buffer)
	err = json.NewEncoder(buf).Encode(req)
	if err != nil {
		return nil, diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Bad encode",
			fmt.Sprintf("Bad encode %s", err),
		))
	}

	resp, err := client.Post(url.JoinPath("import", scope).String(), "application/json", buf)
	if err != nil {
		return nil, diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Resource allocator unavailable",
			fmt.Sprintf("The resource allocator failed to respond at %s: %s", endpoint, err),
		))
	}

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(ret)
	if err != nil {
		return nil, diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Resource allocator bad response",
			fmt.Sprintf("Malformed response: %s", err),
		))
	}

	//imported := make([]importedResource, len(ret.Resources))
	for key, _ := range ret.Resources {
		log.Printf("[INFO] graphNodeAllocator: imported %s", key)
		// imported = append(imported, importedResource{
		// 	addr:  addrMap[key],
		// 	state: val,
		// })
	}

	return ret, nil
}

// GraphNodeExecutable impl.
func (n *graphNodeAllocator) Execute(ctx EvalContext, op walkOperation) (diags tfdiags.Diagnostics) {
	// Reset our states
	n.states = nil

	client := httpclient.New()
	url, err := url.Parse(n.Endpoint)
	if err != nil {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Invalid allocator endpoint",
			fmt.Sprintf("Endpoint is not a url %s: %s", n.Endpoint, err),
		))
	}

	addrMap := make(map[string]addrs.AbsResourceInstance, len(n.Resources))
	resources := make([]string, len(n.Resources))
	for i, r := range n.Resources {
		key := r.Resource.String()
		resources[i] = key
		addrMap[key] = r
	}

	req := allocatorImportRequest{
		Resources: resources,
	}

	buf := new(bytes.Buffer)
	err = json.NewEncoder(buf).Encode(req)
	if err != nil {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Bad encode",
			fmt.Sprintf("Bad encode %s", err),
		))
	}

	resp, err := client.Post(url.JoinPath("import", n.Scope).String(), "application/json", buf)
	if err != nil {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Resource allocator unavailable",
			fmt.Sprintf("The resource allocator failed to respond at %s: %s", n.Endpoint, err),
		))
	}

	decoder := json.NewDecoder(resp.Body)
	var val allocatorImportResponse
	err = decoder.Decode(val)
	if err != nil {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Resource allocator bad response",
			fmt.Sprintf("Malformed response: %s", err),
		))
	}

	imported := make([]importedResource, len(val.Resources))
	for key, val := range val.Resources {
		log.Printf("[INFO] graphNodeAllocator: imported %s", key)
		imported = append(imported, importedResource{
			addr:  addrMap[key],
			state: val,
		})
	}

	n.states = imported

	return diags
}

// GraphNodeDynamicExpandable impl.
//
// We use DynamicExpand as a way to generate the subgraph of refreshes
// and state inserts we need to do for our import state. Since they're new
// resources they don't depend on anything else and refreshes are isolated
// so this is nearly a perfect use case for dynamic expand.
func (n *graphNodeAllocator) DynamicExpand(ctx EvalContext) (*Graph, error) {
	var diags tfdiags.Diagnostics

	g := &Graph{Path: ctx.Path()}

	// Verify that all the addresses are clear
	state := ctx.State()
	for _, r := range n.states {
		existing := state.ResourceInstance(r.addr)
		if existing != nil {
			diags = diags.Append(tfdiags.Sourceless(
				tfdiags.Error,
				"Resource already managed by Terraform",
				fmt.Sprintf("Terraform is already managing a remote object for %s. To import to this address you must first remove the existing object from the state.", r.addr),
			))
			continue
		}
	}
	if diags.HasErrors() {
		// Bail out early, then.
		return nil, diags.Err()
	}

	// For each of the states, we add a node to handle the refresh/add to state.
	// "n.states" is populated by our own Execute with the result of
	// ImportState. Since DynamicExpand is always called after Execute, this is
	// safe.
	for _, r := range n.states {
		g.Add(&graphNodeAllocatorStateSub{
			TargetAddr: r.addr,
			State:      r.state,
		})
	}

	addRootNodeToGraph(g)

	// Done!
	return g, diags.Err()
}

type graphNodeAllocatorStateSub struct {
	TargetAddr addrs.AbsResourceInstance
	State      cty.Value
}

var (
	_ GraphNodeModuleInstance = (*graphNodeAllocatorStateSub)(nil)
	_ GraphNodeExecutable     = (*graphNodeAllocatorStateSub)(nil)
)

func (n *graphNodeAllocatorStateSub) Name() string {
	return fmt.Sprintf("import %s result", n.TargetAddr)
}

func (n *graphNodeAllocatorStateSub) Path() addrs.ModuleInstance {
	return n.TargetAddr.Module
}

// GraphNodeExecutable impl.
func (n *graphNodeAllocatorStateSub) Execute(ctx EvalContext, op walkOperation) (diags tfdiags.Diagnostics) {
	state := &states.ResourceInstanceObject{
		Status: states.ObjectReady,
		Value:  n.State,
	}

	encoded, err := state.Encode(cty.DynamicPseudoType, 0)
	if err != nil {
		panic(err)
	}

	ctx.State().SetResourceInstanceCurrent(
		n.TargetAddr,
		encoded,
		addrs.AbsProviderConfig{},
	)

	return diags
}
