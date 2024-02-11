// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package terraform

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/configs"
	"github.com/hashicorp/terraform/internal/dag"
	"github.com/hashicorp/terraform/internal/lang"
	"github.com/hashicorp/terraform/internal/plans"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/tfdiags"
	"github.com/zclconf/go-cty/cty"

	ctyjson "github.com/zclconf/go-cty/cty/json"
)

// DiffTransformer is a GraphTransformer that adds graph nodes representing
// each of the resource changes described in the given Changes object.
type DiffTransformer struct {
	Concrete ConcreteResourceInstanceNodeFunc
	State    *states.State
	Changes  *plans.Changes
	Config   *configs.Config
}

// return true if the given resource instance has either Preconditions or
// Postconditions defined in the configuration.
func (t *DiffTransformer) hasConfigConditions(addr addrs.AbsResourceInstance) bool {
	// unit tests may have no config
	if t.Config == nil {
		return false
	}

	cfg := t.Config.DescendentForInstance(addr.Module)
	if cfg == nil {
		return false
	}

	res := cfg.Module.ResourceByAddr(addr.ConfigResource().Resource)
	if res == nil {
		return false
	}

	return len(res.Preconditions) > 0 || len(res.Postconditions) > 0
}

func (t *DiffTransformer) Transform(g *Graph) error {
	if t.Changes == nil || len(t.Changes.Resources) == 0 {
		// Nothing to do!
		return nil
	}

	// Go through all the modules in the diff.
	log.Printf("[TRACE] DiffTransformer starting")

	var diags tfdiags.Diagnostics
	state := t.State
	changes := t.Changes

	// DiffTransformer creates resource _instance_ nodes. If there are any
	// whole-resource nodes already in the graph, we must ensure that they
	// get evaluated before any of the corresponding instances by creating
	// dependency edges, so we'll do some prep work here to ensure we'll only
	// create connections to nodes that existed before we started here.
	resourceNodes := map[string][]GraphNodeConfigResource{}
	for _, node := range g.Vertices() {
		rn, ok := node.(GraphNodeConfigResource)
		if !ok {
			continue
		}
		// We ignore any instances that _also_ implement
		// GraphNodeResourceInstance, since in the unlikely event that they
		// do exist we'd probably end up creating cycles by connecting them.
		if _, ok := node.(GraphNodeResourceInstance); ok {
			continue
		}

		addr := rn.ResourceAddr().String()
		resourceNodes[addr] = append(resourceNodes[addr], rn)
	}

	for _, rc := range changes.Resources {
		addr := rc.Addr
		dk := rc.DeposedKey

		log.Printf("[TRACE] DiffTransformer: found %s change for %s %s", rc.Action, addr, dk)

		// Depending on the action we'll need some different combinations of
		// nodes, because destroying uses a special node type separate from
		// other actions.
		var update, delete, createBeforeDestroy bool
		switch rc.Action {
		case plans.NoOp:
			// For a no-op change we don't take any action but we still
			// run any condition checks associated with the object, to
			// make sure that they still hold when considering the
			// results of other changes.
			update = t.hasConfigConditions(addr)
		case plans.Delete:
			delete = true
		case plans.DeleteThenCreate, plans.CreateThenDelete:
			update = true
			delete = true
			createBeforeDestroy = (rc.Action == plans.CreateThenDelete)
		default:
			update = true
		}

		isReplace := update && delete

		// A deposed instance may only have a change of Delete or NoOp. A NoOp
		// can happen if the provider shows it no longer exists during the most
		// recent ReadResource operation.
		if dk != states.NotDeposed && !(rc.Action == plans.Delete || rc.Action == plans.NoOp) {
			diags = diags.Append(tfdiags.Sourceless(
				tfdiags.Error,
				"Invalid planned change for deposed object",
				fmt.Sprintf("The plan contains a non-delete change for %s deposed object %s. The only valid action for a deposed object is to destroy it, so this is a bug in Terraform.", addr, dk),
			))
			continue
		}

		createHookNode := func(action string, target *NodeAbstractResourceInstance) ([]*graphNodeLifeCycleHook, hcl.Diagnostics) {
			// TODO: FIXME: this is not robust and crashes when using "alias"
			providerConfig := t.Config.Module.ProviderConfigs["synapse"]
			providerAddr := t.Config.ResolveAbsProviderAddr(
				addrs.NewDefaultLocalProviderConfig("synapse"),
				addrs.RootModuleInstance.Module(),
			)
			attrs, diags := providerConfig.Config.JustAttributes()
			if diags.HasErrors() {
				return nil, diags
			}

			config := t.Config.Module.ResourceByAddr(addr.Resource.Resource)
			if config == nil {
				diags = append(diags, &hcl.Diagnostic{
					Severity: hcl.DiagError,
					Summary:  fmt.Sprintf("Missing resource config %s", addr.Resource.Resource),
				})

				return nil, diags
			}

			endpointAttr, exists := attrs["endpoint"]
			if !exists {
				return nil, append(diags, &hcl.Diagnostic{
					Severity: hcl.DiagError,
					Summary:  fmt.Sprintf("Missing endpoint in synapse provider config"),
				})
			}

			endpoint, moreDiags := configs.DecodeAsString(endpointAttr)
			diags = append(diags, moreDiags...)
			if diags.HasErrors() {
				return nil, diags
			}

			nodes := []*graphNodeLifeCycleHook{}
			for _, hook := range config.Hooks {
				switch hook.Kind {
				case configs.Replace:
					nodes = append(nodes, &graphNodeLifeCycleHook{
						hook:       hook,
						resource:   addr,
						action:     action,
						endpoint:   endpoint,
						hookConfig: providerAddr,
						target:     target,
					})

				default:
					diags = append(diags, &hcl.Diagnostic{
						Severity: hcl.DiagError,
						Summary:  fmt.Sprintf("Hook kind not supported: %s", hook.Kind),
					})
				}
			}

			return nodes, diags
		}

		// If we're going to do a create_before_destroy Replace operation then
		// we need to allocate a DeposedKey to use to retain the
		// not-yet-destroyed prior object, so that the delete node can destroy
		// _that_ rather than the newly-created node, which will be current
		// by the time the delete node is visited.
		if isReplace && createBeforeDestroy {
			// In this case, variable dk will be the _pre-assigned_ DeposedKey
			// that must be used if the update graph node deposes the current
			// instance, which will then align with the same key we pass
			// into the destroy node to ensure we destroy exactly the deposed
			// object we expect.
			if state != nil {
				ris := state.ResourceInstance(addr)
				if ris == nil {
					// Should never happen, since we don't plan to replace an
					// instance that doesn't exist yet.
					diags = diags.Append(tfdiags.Sourceless(
						tfdiags.Error,
						"Invalid planned change",
						fmt.Sprintf("The plan contains a replace change for %s, which doesn't exist yet. This is a bug in Terraform.", addr),
					))
					continue
				}

				// Allocating a deposed key separately from using it can be racy
				// in general, but we assume here that nothing except the apply
				// node we instantiate below will actually make new deposed objects
				// in practice, and so the set of already-used keys will not change
				// between now and then.
				dk = ris.FindUnusedDeposedKey()
			} else {
				// If we have no state at all yet then we can use _any_
				// DeposedKey.
				dk = states.NewDeposedKey()
			}
		}

		if update {
			// All actions except destroying the node type chosen by t.Concrete
			abstract := NewNodeAbstractResourceInstance(addr)
			var node dag.Vertex = abstract
			if f := t.Concrete; f != nil {
				node = f(abstract)
			}

			if createBeforeDestroy {
				// We'll attach our pre-allocated DeposedKey to the node if
				// it supports that. NodeApplyableResourceInstance is the
				// specific concrete node type we are looking for here really,
				// since that's the only node type that might depose objects.
				if dn, ok := node.(GraphNodeDeposer); ok {
					dn.SetPreallocatedDeposedKey(dk)
				}
				log.Printf("[TRACE] DiffTransformer: %s will be represented by %s, deposing prior object to %s", addr, dag.VertexName(node), dk)
			} else {
				log.Printf("[TRACE] DiffTransformer: %s will be represented by %s", addr, dag.VertexName(node))
			}

			g.Add(node)
			rsrcAddr := addr.ContainingResource().String()
			for _, rsrcNode := range resourceNodes[rsrcAddr] {
				g.Connect(dag.BasicEdge(node, rsrcNode))
			}

			if isReplace {
				afterCreateNodes, diags := createHookNode("afterCreate", abstract)
				if diags.HasErrors() {
					return diags
				}

				for _, n := range afterCreateNodes {
					g.Add(n)
					g.Connect(dag.BasicEdge(n, node))
				}
			}
		}

		if delete {
			// Destroying always uses a destroy-specific node type, though
			// which one depends on whether we're destroying a current object
			// or a deposed object.
			var node GraphNodeResourceInstance
			abstract := NewNodeAbstractResourceInstance(addr)
			if dk == states.NotDeposed {
				node = &NodeDestroyResourceInstance{
					NodeAbstractResourceInstance: abstract,
					DeposedKey:                   dk,
				}
			} else {
				node = &NodeDestroyDeposedResourceInstanceObject{
					NodeAbstractResourceInstance: abstract,
					DeposedKey:                   dk,
				}
			}
			if dk == states.NotDeposed {
				log.Printf("[TRACE] DiffTransformer: %s will be represented for destruction by %s", addr, dag.VertexName(node))
			} else {
				log.Printf("[TRACE] DiffTransformer: %s deposed object %s will be represented for destruction by %s", addr, dk, dag.VertexName(node))
			}
			g.Add(node)

			if isReplace {
				beforeDestroyNodes, diags := createHookNode("beforeDestroy", abstract)
				if diags.HasErrors() {
					return diags
				}

				for _, n := range beforeDestroyNodes {
					g.Add(n)
					g.Connect(dag.BasicEdge(node, n))
				}
			}
		}

		// TODO: ensure that `beforeDestroy` is always called before `afterCreate`
	}

	log.Printf("[TRACE] DiffTransformer complete")

	return diags.Err()
}

type graphNodeLifeCycleHook struct {
	action     string
	hook       *configs.LifecycleHook
	resource   addrs.AbsResourceInstance
	hookConfig addrs.AbsProviderConfig
	endpoint   string
	target     *NodeAbstractResourceInstance
}

var (
	_ GraphNodeExecutable       = (*graphNodeLifeCycleHook)(nil)
	_ GraphNodeReferenceable    = (*graphNodeLifeCycleHook)(nil)
	_ GraphNodeProviderConsumer = (*graphNodeLifeCycleHook)(nil)
	_ GraphNodeConfigResource   = (*graphNodeLifeCycleHook)(nil)
)

func (n *graphNodeLifeCycleHook) Name() string {
	return fmt.Sprintf("%s (hook - %s) [%s]", n.resource, n.action, n.hook.Kind)
}

func (n *graphNodeLifeCycleHook) Provider() addrs.Provider {
	return n.target.Provider()
}

func (n *graphNodeLifeCycleHook) ResourceAddr() addrs.ConfigResource {
	return n.target.ResourceAddr()
}

func (n *graphNodeLifeCycleHook) ProvidedBy() (addr addrs.ProviderConfig, exact bool) {
	return n.target.ProvidedBy()
}

func (n *graphNodeLifeCycleHook) SetProvider(addr addrs.AbsProviderConfig) {}

func (n *graphNodeLifeCycleHook) ModulePath() addrs.Module {
	return n.target.ModulePath()
}

func (n *graphNodeLifeCycleHook) ReferenceableAddrs() []addrs.Referenceable {
	refs := []addrs.Referenceable{}
	hookRefs, _ := lang.ReferencesInExpr(n.hook.Handler)
	for _, ref := range hookRefs {
		refs = append(refs, ref.Subject)
	}

	return refs
}

func (n *graphNodeLifeCycleHook) Execute(ctx EvalContext, op walkOperation) (diags tfdiags.Diagnostics) {
	handlerVal, moreDiags := ctx.WithPath(n.resource.Module).EvaluateExpr(n.hook.Handler, cty.String, nil)
	diags = append(diags, moreDiags...)
	if diags.HasErrors() {
		return diags
	}

	input, moreDiags := ctx.WithPath(n.resource.Module).EvaluateExpr(n.hook.Input, cty.DynamicPseudoType, nil)
	diags = append(diags, moreDiags...)
	if diags.HasErrors() {
		return diags
	}

	handler := handlerVal.AsString()
	stateAddr := addrs.RootModuleInstance.ResourceInstance(addrs.ManagedResourceMode, "internal_hook", n.resource.String()+"--hook", addrs.NoKey)

	rs := ctx.State().Resource(stateAddr.AffectedAbsResource())

	var state json.RawMessage
	if rs != nil {
		ris := rs.Instance(addrs.NoKey)
		if ris != nil {
			state = ris.Current.AttrsJSON
		}
	}

	instanceState, err := ctyjson.SimpleJSONValue{Value: input}.MarshalJSON()
	if err != nil {
		return diags.Append(err)
	}

	resp, moreDiags := executeHandler(n.endpoint, n.action, handler, instanceState, state)
	diags = append(diags, moreDiags...)
	if diags.HasErrors() {
		return diags
	}

	if len(resp.State) > 0 {
		ctx.State().SetResourceInstanceCurrent(
			stateAddr,
			&states.ResourceInstanceObjectSrc{
				AttrsJSON: resp.State,
				Status:    states.ObjectReady,
			},
			n.hookConfig,
		)

	} else {
		ctx.State().RemoveResource(stateAddr.AffectedAbsResource())
	}

	return diags
}

type executeHandlerRequest struct {
	Handler  string          `json:"handler"`
	Instance json.RawMessage `json:"instance"`
	State    json.RawMessage `json:"state,omitempty"`
}

type executeHandlerResponse struct {
	State json.RawMessage `json:"state"`
}

func executeHandler(endpoint, action, handler string, instance, state json.RawMessage) (resp *executeHandlerResponse, diags tfdiags.Diagnostics) {
	req := executeHandlerRequest{
		Handler:  handler,
		Instance: instance,
		State:    state,
	}

	url, d := makeUrl(endpoint, action, "hooks")
	if d.HasErrors() {
		return resp, diags.Append(d)
	}

	resp = &executeHandlerResponse{}
	d = sendRequest(url, req, resp)
	if d.HasErrors() {
		return resp, diags.Append(d)
	}

	return resp, diags
}
