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
	"strings"

	"github.com/hashicorp/hcl/v2"
	tfaddr "github.com/hashicorp/terraform-registry-address"
	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/configs"
	"github.com/hashicorp/terraform/internal/configs/configschema"
	"github.com/hashicorp/terraform/internal/dag"
	"github.com/hashicorp/terraform/internal/httpclient"
	"github.com/hashicorp/terraform/internal/instances"
	"github.com/hashicorp/terraform/internal/lang"
	"github.com/hashicorp/terraform/internal/plans/objchange"
	"github.com/hashicorp/terraform/internal/providers"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/tfdiags"
	"github.com/zclconf/go-cty/cty"
)

type cachedSchema struct {
	block   *configschema.Block
	version uint64
}

type pendingDestroy struct {
	TransactionId string
	Resource      addrs.AbsResource
}

type Allocator struct {
	ProvidersMeta      map[addrs.Provider]*configs.ProviderMeta
	Config             *configs.Config
	imported           *map[string]ExportedResource
	pendingDestroy     *map[string]pendingDestroy
	providerMetaValues map[addrs.Provider]*cty.Value
	schemas            map[addrs.Resource]cachedSchema
	endpoint           string
	scope              string
	module             string
	resources          []*addrs.Reference
}

func createAllocator(c *configs.Config) (*Allocator, error) {
	configBlock := c.Module.Allocator
	if configBlock == nil {
		return nil, fmt.Errorf("Missing allocator block")
	}

	allocator := &Allocator{
		Config:             c,
		providerMetaValues: map[tfaddr.Provider]*cty.Value{},
		schemas:            map[addrs.Resource]cachedSchema{},
		endpoint:           configBlock.Endpoint,
		scope:              configBlock.Scope,
		module:             configBlock.Module,
		resources:          configBlock.Resources,
	}

	return allocator, nil
}

func (a *Allocator) ImportState() (ret *map[string]ExportedResource, diags tfdiags.Diagnostics) {
	if a.imported != nil {
		return a.imported, nil
	}

	resources := a.resources
	keys := make([]string, len(resources))
	for i, r := range resources {
		key := r.Subject.String()
		keys[i] = key
	}

	req := allocatorImportRequest{
		Module:    a.module,
		Resources: keys,
	}
	importResp := &allocatorImportResponse{}

	url, d := makeUrl(a.endpoint, a.scope, "import")
	if d.HasErrors() {
		return nil, diags.Append(d)
	}

	d = sendRequest(url, req, importResp)
	if d.HasErrors() {
		return nil, diags.Append(d)
	}

	a.imported = &map[string]ExportedResource{}

	for key, r := range importResp.Resources {
		log.Printf("[INFO] graphNodeAllocator: imported %s", key)
		(*a.imported)[key] = r
	}

	return a.imported, nil
}

type ApplyResult struct {
	State         *states.ResourceInstanceObject
	Schema        *configschema.Block
	SchemaVersion uint64
}

func (a *Allocator) Apply(ctx EvalContext, addr addrs.Resource, c *configs.Resource) (*ApplyResult, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	rConfigAddr := a.Config.Module.ResourceByAddr(addr)
	if rConfigAddr == nil {
		return nil, diags.Append(fmt.Errorf("missing resource config %s", addr))
	}

	providerAddr := a.Config.ResolveAbsProviderAddr(rConfigAddr.ProviderConfigAddr(), addrs.RootModule)
	p, err := a.getProvider(ctx, providerAddr)
	if err != nil {
		return nil, diags.Append(err)
	}

	schema, version, schemaDiags := a.GetSchema(ctx, addr, providerAddr)
	diags.Append(schemaDiags)
	if diags.HasErrors() {
		return nil, diags
	}

	log.Printf("[INFO] Starting apply for %s", addr)

	configVal, _, configDiags := ctx.EvaluateBlock(c.Config, schema, nil, instances.RepetitionData{})
	diags = diags.Append(configDiags)
	if configDiags.HasErrors() {
		return nil, diags
	}

	if !configVal.IsWhollyKnown() {
		// We don't have a pretty format function for a path, but since this is
		// such a rare error, we can just drop the raw GoString values in here
		// to make sure we have something to debug with.
		var unknownPaths []string
		cty.Transform(configVal, func(p cty.Path, v cty.Value) (cty.Value, error) {
			if !v.IsKnown() {
				unknownPaths = append(unknownPaths, fmt.Sprintf("%#v", p))
			}
			return v, nil
		})

		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Configuration contains unknown value",
			fmt.Sprintf("configuration for %s still contains unknown values during apply (this is a bug in Terraform; please report it!)\n"+
				"The following paths in the resource configuration are unknown:\n%s",
				rConfigAddr,
				strings.Join(unknownPaths, "\n"),
			),
		))
		return nil, diags
	}

	// unmarkedConfigVal, _ := configVal.UnmarkDeep()
	ty := schema.ImpliedType()
	nullVal := cty.NullVal(ty)
	state := &states.ResourceInstanceObject{Value: nullVal}
	ris := ctx.State().Module(addrs.RootModuleInstance).ResourceInstance(addr.Instance(addrs.NoKey))
	if ris != nil && ris.HasCurrent() {
		decoded, err := ris.Current.Decode(ty)
		if err != nil {
			return nil, diags.Append(err)
		}
		state = decoded
	}

	proposedNewVal := objchange.ProposedNew(schema, state.Value, configVal)

	metaConfigVal, metaDiags := a.GetMeta(ctx, addr, providerAddr)
	diags = diags.Append(metaDiags)
	if diags.HasErrors() {
		return nil, diags
	}

	d := a.configureProvider(ctx, providerAddr)
	if d.HasErrors() {
		return nil, d
	}

	planResp := p.PlanResourceChange(providers.PlanResourceChangeRequest{
		TypeName:         addr.Type,
		Config:           configVal,
		PriorState:       state.Value,
		ProposedNewState: proposedNewVal,
		PriorPrivate:     state.Private,
		ProviderMeta:     *metaConfigVal,
	})

	diags = diags.Append(planResp.Diagnostics)
	if diags.HasErrors() {
		return nil, diags
	}

	afterState := planResp.PlannedState

	log.Printf("[INFO] %s: applying configuration", addr)

	// If our config, Before or After value contain any marked values,
	// ensure those are stripped out before sending
	// this to the provider
	// unmarkedConfigVal, _ := configVal.UnmarkDeep()
	// unmarkedBefore, beforePaths := change.Before.UnmarkDeepWithPaths()
	// unmarkedAfter, afterPaths := change.After.UnmarkDeepWithPaths()

	eqV := state.Value.Equals(planResp.PlannedState)
	eq := eqV.IsKnown() && eqV.True()
	if eq {
		// Copy the previous state, changing only the value
		newState := &states.ResourceInstanceObject{
			CreateBeforeDestroy: state.CreateBeforeDestroy,
			Dependencies:        state.Dependencies,
			Private:             state.Private,
			Status:              state.Status,
			Value:               afterState,
		}
		return &ApplyResult{State: newState, Schema: schema}, diags
	}

	resp := p.ApplyResourceChange(providers.ApplyResourceChangeRequest{
		TypeName:       addr.Type,
		PriorState:     state.Value,
		Config:         configVal,
		PlannedState:   planResp.PlannedState,
		PlannedPrivate: planResp.PlannedPrivate,
		ProviderMeta:   *metaConfigVal,
	})
	applyDiags := resp.Diagnostics
	applyDiags = applyDiags.InConfigBody(c.Config, addr.String())
	diags = diags.Append(applyDiags)

	// Even if there are errors in the returned diagnostics, the provider may
	// have returned a _partial_ state for an object that already exists but
	// failed to fully configure, and so the remaining code must always run
	// to completion but must be defensive against the new value being
	// incomplete.
	newVal := resp.NewState

	if newVal == cty.NilVal {
		// Providers are supposed to return a partial new value even when errors
		// occur, but sometimes they don't and so in that case we'll patch that up
		// by just using the prior state, so we'll at least keep track of the
		// object for the user to retry.
		newVal = state.Value

		// As a special case, we'll set the new value to null if it looks like
		// we were trying to execute a delete, because the provider in this case
		// probably left the newVal unset intending it to be interpreted as "null".
		if planResp.PlannedState.IsNull() {
			newVal = cty.NullVal(schema.ImpliedType())
		}

		if !diags.HasErrors() {
			diags = diags.Append(tfdiags.Sourceless(
				tfdiags.Error,
				"Provider produced invalid object",
				fmt.Sprintf(
					"Provider %q produced an invalid nil value after apply for %s.\n\nThis is a bug in the provider, which should be reported in the provider's own issue tracker.",
					providerAddr.String(), addr.String(),
				),
			))
		}
	}

	var conformDiags tfdiags.Diagnostics
	for _, err := range newVal.Type().TestConformance(schema.ImpliedType()) {
		conformDiags = conformDiags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Provider produced invalid object",
			fmt.Sprintf(
				"Provider %q produced an invalid value after apply for %s. The result cannot not be saved in the Terraform state.\n\nThis is a bug in the provider, which should be reported in the provider's own issue tracker.",
				providerAddr.String(), tfdiags.FormatErrorPrefixed(err, addr.String()),
			),
		))
	}
	diags = diags.Append(conformDiags)
	if conformDiags.HasErrors() {
		// Bail early in this particular case, because an object that doesn't
		// conform to the schema can't be saved in the state anyway -- the
		// serializer will reject it.
		return nil, diags
	}

	// After this point we have a type-conforming result object and so we
	// must always run to completion to ensure it can be saved. If n.Error
	// is set then we must not return a non-nil error, in order to allow
	// evaluation to continue to a later point where our state object will
	// be saved.

	// By this point there must not be any unknown values remaining in our
	// object, because we've applied the change and we can't save unknowns
	// in our persistent state. If any are present then we will indicate an
	// error (which is always a bug in the provider) but we will also replace
	// them with nulls so that we can successfully save the portions of the
	// returned value that are known.
	if !newVal.IsWhollyKnown() {
		// To generate better error messages, we'll go for a walk through the
		// value and make a separate diagnostic for each unknown value we
		// find.
		cty.Walk(newVal, func(path cty.Path, val cty.Value) (bool, error) {
			if !val.IsKnown() {
				pathStr := tfdiags.FormatCtyPath(path)
				diags = diags.Append(tfdiags.Sourceless(
					tfdiags.Error,
					"Provider returned invalid result object after apply",
					fmt.Sprintf(
						"After the apply operation, the provider still indicated an unknown value for %s%s. All values must be known after apply, so this is always a bug in the provider and should be reported in the provider's own repository. Terraform will still save the other known object values in the state.",
						addr, pathStr,
					),
				))
			}
			return true, nil
		})

		// NOTE: This operation can potentially be lossy if there are multiple
		// elements in a set that differ only by unknown values: after
		// replacing with null these will be merged together into a single set
		// element. Since we can only get here in the presence of a provider
		// bug, we accept this because storing a result here is always a
		// best-effort sort of thing.
		newVal = cty.UnknownAsNull(newVal)
	}

	if !diags.HasErrors() {
		// Only values that were marked as unknown in the planned value are allowed
		// to change during the apply operation. (We do this after the unknown-ness
		// check above so that we also catch anything that became unknown after
		// being known during plan.)
		//
		// If we are returning other errors anyway then we'll give this
		// a pass since the other errors are usually the explanation for
		// this one and so it's more helpful to let the user focus on the
		// root cause rather than distract with this extra problem.
		if errs := objchange.AssertObjectCompatible(schema, planResp.PlannedState, newVal); len(errs) > 0 {
			if resp.LegacyTypeSystem {
				// The shimming of the old type system in the legacy SDK is not precise
				// enough to pass this consistency check, so we'll give it a pass here,
				// but we will generate a warning about it so that we are more likely
				// to notice in the logs if an inconsistency beyond the type system
				// leads to a downstream provider failure.
				var buf strings.Builder
				fmt.Fprintf(&buf, "[WARN] Provider %q produced an unexpected new value for %s, but we are tolerating it because it is using the legacy plugin SDK.\n    The following problems may be the cause of any confusing errors from downstream operations:", providerAddr.String(), addr)
				for _, err := range errs {
					fmt.Fprintf(&buf, "\n      - %s", tfdiags.FormatError(err))
				}
				log.Print(buf.String())

				// The sort of inconsistency we won't catch here is if a known value
				// in the plan is changed during apply. That can cause downstream
				// problems because a dependent resource would make its own plan based
				// on the planned value, and thus get a different result during the
				// apply phase. This will usually lead to a "Provider produced invalid plan"
				// error that incorrectly blames the downstream resource for the change.

			} else {
				for _, err := range errs {
					diags = diags.Append(tfdiags.Sourceless(
						tfdiags.Error,
						"Provider produced inconsistent result after apply",
						fmt.Sprintf(
							"When applying changes to %s, provider %q produced an unexpected new value: %s.\n\nThis is a bug in the provider, which should be reported in the provider's own issue tracker.",
							addr, providerAddr, tfdiags.FormatError(err),
						),
					))
				}
			}
		}
	}

	// If a provider returns a null or non-null object at the wrong time then
	// we still want to save that but it often causes some confusing behaviors
	// where it seems like Terraform is failing to take any action at all,
	// so we'll generate some errors to draw attention to it.
	if !diags.HasErrors() {
		// if change.Action == plans.Delete && !newVal.IsNull() {
		// 	diags = diags.Append(tfdiags.Sourceless(
		// 		tfdiags.Error,
		// 		"Provider returned invalid result object after apply",
		// 		fmt.Sprintf(
		// 			"After applying a %s plan, the provider returned a non-null object for %s. Destroying should always produce a null value, so this is always a bug in the provider and should be reported in the provider's own repository. Terraform will still save this errant object in the state for debugging and recovery.",
		// 			change.Action, n.Addr,
		// 		),
		// 	))
		// }
		if newVal.IsNull() {
			diags = diags.Append(tfdiags.Sourceless(
				tfdiags.Error,
				"Provider returned invalid result object after apply",
				fmt.Sprintf(
					"After applying the configuration, the provider returned a null object for %s. Only destroying should always produce a null value, so this is always a bug in the provider and should be reported in the provider's own repository.",
					addr,
				),
			))
		}
	}

	switch {
	case diags.HasErrors() && newVal.IsNull():
		// Sometimes providers return a null value when an operation fails for
		// some reason, but we'd rather keep the prior state so that the error
		// can be corrected on a subsequent run. We must only do this for null
		// new value though, or else we may discard partial updates the
		// provider was able to complete. Otherwise, we'll continue using the
		// prior state as the new value, making this effectively a no-op.  If
		// the item really _has_ been deleted then our next refresh will detect
		// that and fix it up.
		return &ApplyResult{State: state, Schema: schema, SchemaVersion: version}, diags

	case diags.HasErrors() && !newVal.IsNull():
		refs, _ := lang.ReferencesInBlock(c.Config, schema)
		// diags.Append(d)
		dependsOn := mapSlice(c.DependsOn, func(t hcl.Traversal) *addrs.Reference {
			ref, d := addrs.ParseRef(t)
			diags = diags.Append(d)

			return ref
		})

		if diags.HasErrors() {
			panic(diags.Err())
		}

		refs = append(refs, dependsOn...)
		deps := make([]addrs.ConfigResource, 0)
		for _, r := range refs {
			switch t := r.Subject.(type) {
			case addrs.Resource:
				deps = append(deps, addrs.RootModule.Resource(t.Mode, t.Type, t.Name))
			}
		}

		// if we have an error, make sure we restore the object status in the new state
		newState := &states.ResourceInstanceObject{
			Status:              state.Status,
			Value:               newVal,
			Private:             resp.Private,
			CreateBeforeDestroy: state.CreateBeforeDestroy,
			Dependencies:        deps,
		}

		return &ApplyResult{State: newState, Schema: schema, SchemaVersion: version}, diags

	case !newVal.IsNull():
		// Non error case with a new state
		newState := &states.ResourceInstanceObject{
			Status:              states.ObjectReady,
			Value:               newVal,
			Private:             resp.Private,
			CreateBeforeDestroy: state.CreateBeforeDestroy,
		}
		return &ApplyResult{State: newState, Schema: schema, SchemaVersion: version}, diags

	default:
		// Non error case, were the object was deleted
		return nil, diags
	}
}

func (a *Allocator) getProvider(ctx EvalContext, addr addrs.AbsProviderConfig) (providers.Interface, error) {
	var p providers.Interface
	if p = ctx.Provider(addr); p == nil {
		initialized, err := ctx.InitProvider(addr)
		if err != nil {
			return nil, err
		}
		p = initialized
	}

	return p, nil
}

func (a *Allocator) configureProvider(ctx EvalContext, addr addrs.AbsProviderConfig) (diags tfdiags.Diagnostics) {
	log.Printf("[INFO] configuring provider %s", addr)

	key := a.Config.Module.LocalNameForProvider(addr.Provider)
	config, exists := a.Config.Module.ProviderConfigs[key]
	if !exists {
		if addr.Provider.Type != "terraform" {
			return diags.Append(tfdiags.Sourceless(
				tfdiags.Error,
				"Missing provider configuration",
				fmt.Sprintf("Provider %s is missing a configuration block", key),
			))
		}

		return diags
	}

	schemas, err := ctx.ProviderSchema(addr)
	if err != nil {
		return diags.Append(err)
	}

	val, _, configDiags := ctx.EvaluateBlock(config.Config, schemas.Provider, nil, EvalDataForNoInstanceKey)
	diags = diags.Append(configDiags)
	if diags.HasErrors() {
		return diags
	}

	p, err := a.getProvider(ctx, addr)
	if err != nil {
		return diags.Append(err)
	}

	validateResp := p.ValidateProviderConfig(providers.ValidateProviderConfigRequest{Config: val})
	diags = diags.Append(validateResp.Diagnostics)
	if diags.HasErrors() {
		return diags
	}

	diags = diags.Append(ctx.ConfigureProvider(addr, validateResp.PreparedConfig))

	return diags
}

type ackRequest struct {
	TransactionId string `json:"transactionId"`
}

func (a *Allocator) ackDestroy(txid string) (diags tfdiags.Diagnostics) {
	url, d := makeUrl(a.endpoint, a.scope, "ack")
	if d.HasErrors() {
		return diags.Append(d)
	}

	req := ackRequest{TransactionId: txid}
	resp := &empty{}

	return sendRequest(url, req, resp)
}

func (a *Allocator) destroyResource(ctx EvalContext, addr addrs.Resource) (diags tfdiags.Diagnostics) {
	log.Printf("[INFO] allocator: destroying resource %s", addr)

	if a.imported == nil {
		a.ImportState()
	}

	key := addr.String()
	r, exists := (*a.imported)[key]
	if !exists {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Resource not imported",
			fmt.Sprintf("Resource %s was not imported and so cannot be destroyed by the allocator", key),
		))
	}

	providerAddr, addrDiags := addrs.ParseAbsProviderConfigStr(r.Provider)
	diags = diags.Append(addrDiags)
	if diags.HasErrors() {
		return diags
	}

	p, err := a.getProvider(ctx, providerAddr)
	if err != nil {
		return diags.Append(err)
	}

	// `r.State.Type()` is technically not the correct type

	metaConfigVal, d := a.GetMeta(ctx, addr, providerAddr)
	diags = diags.Append(d)
	if diags.HasErrors() {
		return diags
	}

	ris := states.ResourceInstanceObjectSrc{
		SchemaVersion: r.SchemaVersion,
		AttrsJSON:     r.State,
	}

	schema, _, _ := a.GetSchema(ctx, addr, providerAddr)
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

	diags = diags.Append(planResp.Diagnostics)
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

func (a *Allocator) GetSchema(ctx EvalContext, resource addrs.Resource, absProviderConfig addrs.AbsProviderConfig) (s *configschema.Block, version uint64, diags tfdiags.Diagnostics) {
	if s, exists := a.schemas[resource]; exists {
		return s.block, s.version, diags
	}

	schemas, err := ctx.ProviderSchema(absProviderConfig)
	if err != nil {
		return s, version, diags.Append(err)
	}

	schema, version := schemas.SchemaForResourceAddr(resource)
	if schema == nil {
		return nil, version, diags.Append(fmt.Errorf("provider does not have schema for resource %s", resource))
	}

	a.schemas[resource] = cachedSchema{block: schema, version: version}

	return schema, version, diags
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
		diags = diags.Append(configDiags)
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
	txid               string
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
	diags := n.allocator.destroyResource(ctx, n.target)
	if diags.HasErrors() {
		return diags
	}

	log.Printf("[INFO] %s (destroy): sending ack", n.target)

	return n.allocator.ackDestroy(n.txid)
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

func GetAllocator(config *configs.Config) *Allocator {
	if allocator != nil {
		return allocator
	}

	a, err := createAllocator(config)
	if err != nil {
		panic(err)
	}

	allocator = a

	return a
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

	imported, diags := GetAllocator(c).ImportState()
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

	shouldDestroy, diags := GetAllocator(c).Unref()
	if diags.HasErrors() {
		return diags.Err()
	}

	log.Printf("[INFO] allocator: planning to destroy %d resources", len(shouldDestroy))

	for _, tx := range shouldDestroy {
		config := c.Root.Module.ResourceByAddr(tx.Resource.Resource)
		g.Add(&graphNodeResourceDestroyer{
			txid:               tx.TransactionId,
			config:             config,
			allocator:          GetAllocator(c),
			target:             tx.Resource.Resource,
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
	TransactionId string `json:"transactionId"`
	// Maps resource -> txid
	ShouldDestroy map[string]string `json:"shouldDestroy"`
}

func (a *Allocator) Unref() (shouldDestroy map[string]pendingDestroy, diags tfdiags.Diagnostics) {
	if a.pendingDestroy != nil {
		return *a.pendingDestroy, diags
	}

	a.pendingDestroy = &map[string]pendingDestroy{}

	req := allocatorUnrefRequest{
		Module: a.module,
	}

	url, d := makeUrl(a.endpoint, a.scope, "unref")
	if d.HasErrors() {
		return shouldDestroy, diags.Append(d)
	}

	resp := &allocatorUnrefResponse{}
	d = sendRequest(url, req, resp)
	if d.HasErrors() {
		return shouldDestroy, diags.Append(d)
	}

	// TODO: how to make this work in parallel?
	for r, txid := range resp.ShouldDestroy {
		addr, addrDiags := addrs.ParseAbsResourceStr(r)
		diags = diags.Append(addrDiags)
		if diags.HasErrors() {
			return shouldDestroy, diags
		}

		(*a.pendingDestroy)[r] = pendingDestroy{
			TransactionId: txid,
			Resource:      addr,
		}
	}

	return *a.pendingDestroy, diags
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
