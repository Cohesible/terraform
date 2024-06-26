// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package terraform

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/hashicorp/terraform/internal/hcl"
	"github.com/zclconf/go-cty/cty"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/checks"
	"github.com/hashicorp/terraform/internal/configs/configschema"
	"github.com/hashicorp/terraform/internal/instances"
	"github.com/hashicorp/terraform/internal/lang"
	"github.com/hashicorp/terraform/internal/plans"
	"github.com/hashicorp/terraform/internal/providers"
	"github.com/hashicorp/terraform/internal/provisioners"
	"github.com/hashicorp/terraform/internal/refactoring"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/tfdiags"
	"github.com/hashicorp/terraform/version"
)

// BuiltinEvalContext is an EvalContext implementation that is used by
// Terraform by default.
type BuiltinEvalContext struct {
	// StopContext is the context used to track whether we're complete
	StopContext context.Context

	// PathValue is the Path that this context is operating within.
	PathValue addrs.ModuleInstance

	// pathSet indicates that this context was explicitly created for a
	// specific path, and can be safely used for evaluation. This lets us
	// differentiate between PathValue being unset, and the zero value which is
	// equivalent to RootModuleInstance.  Path and Evaluation methods will
	// panic if this is not set.
	pathSet bool

	// Evaluator is used for evaluating expressions within the scope of this
	// eval context.
	Evaluator *Evaluator

	// VariableValues contains the variable values across all modules. This
	// structure is shared across the entire containing context, and so it
	// may be accessed only when holding VariableValuesLock.
	// The keys of the first level of VariableValues are the string
	// representations of addrs.ModuleInstance values. The second-level keys
	// are variable names within each module instance.
	VariableValues     map[string]map[string]cty.Value
	VariableValuesLock *sync.Mutex

	// Plugins is a library of plugin components (providers and provisioners)
	// available for use during a graph walk.
	Plugins *contextPlugins

	Hooks               []Hook
	InputValue          UIInput
	ProviderCache       *ProviderCache
	ProviderInputConfig map[string]map[string]cty.Value
	ProviderInputLock   *sync.Mutex
	ProviderLocks       map[string]*sync.Mutex

	ProvisionerCache      map[string]provisioners.Interface
	ProvisionerLock       *sync.Mutex
	ChangesValue          *plans.ChangesSync
	StateValue            *states.SyncState
	ChecksValue           *checks.State
	RefreshStateValue     *states.SyncState
	PrevRunStateValue     *states.SyncState
	InstanceExpanderValue *instances.Expander
	MoveResultsValue      refactoring.MoveResults

	EncodeCache *EncodeCache
}

// BuiltinEvalContext implements EvalContext
var _ EvalContext = (*BuiltinEvalContext)(nil)

func (ctx *BuiltinEvalContext) WithPath(path addrs.ModuleInstance) EvalContext {
	newCtx := *ctx
	newCtx.pathSet = true
	newCtx.PathValue = path
	return &newCtx
}

func (ctx *BuiltinEvalContext) Stopped() <-chan struct{} {
	// This can happen during tests. During tests, we just block forever.
	if ctx.StopContext == nil {
		return nil
	}

	return ctx.StopContext.Done()
}

func (ctx *BuiltinEvalContext) Hook(fn func(Hook) (HookAction, error)) error {
	for _, h := range ctx.Hooks {
		action, err := fn(h)
		if err != nil {
			return err
		}

		switch action {
		case HookActionContinue:
			continue
		case HookActionHalt:
			// Return an early exit error to trigger an early exit
			log.Printf("[WARN] Early exit triggered by hook: %T", h)
			return nil
		}
	}

	return nil
}

func (ctx *BuiltinEvalContext) Input() UIInput {
	return ctx.InputValue
}

func (ctx *BuiltinEvalContext) InitProvider(addr addrs.AbsProviderConfig) (providers.Interface, error) {
	if p := ctx.Provider(addr); p != nil {
		return p, nil
	}

	key := addr.String()

	p, err := ctx.Plugins.NewProviderInstance(addr.Provider)
	if err != nil {
		return nil, err
	}

	log.Printf("[TRACE] BuiltinEvalContext: Initialized %q provider for %s", addr.String(), addr)

	ctx.ProviderCache.mu.Lock()
	defer ctx.ProviderCache.mu.Unlock()

	// Check again in case the lock was held during configuration
	if cached, exists := ctx.ProviderCache.providers[key]; exists {
		return cached.provider, nil
	}

	ctx.ProviderCache.providers[key] = &CachedProvider{
		provider:   p,
		configured: false,
	}

	return p, nil
}

func (ctx *BuiltinEvalContext) Provider(addr addrs.AbsProviderConfig) providers.Interface {
	ctx.ProviderCache.mu.Lock()
	defer ctx.ProviderCache.mu.Unlock()

	cached := ctx.ProviderCache.providers[addr.String()]
	if cached == nil {
		return nil
	}

	return cached.provider
}

func (ctx *BuiltinEvalContext) ProviderSchema(addr addrs.AbsProviderConfig) (*ProviderSchema, error) {
	return ctx.Plugins.ProviderSchema(addr.Provider)
}

func (ctx *BuiltinEvalContext) ResourceSchema(provider addrs.AbsProviderConfig, addr addrs.ConfigResource) (*configschema.Block, uint64, error) {
	return ctx.Plugins.ResourceTypeSchema(provider.Provider, addr.Resource.Mode, addr.Resource.Type)
}

func (ctx *BuiltinEvalContext) CloseProvider(addr addrs.AbsProviderConfig) error {
	ctx.ProviderCache.mu.Lock()
	defer ctx.ProviderCache.mu.Unlock()

	key := addr.String()
	cached := ctx.ProviderCache.providers[key]
	if cached != nil {
		log.Printf("[INFO] BuiltinEvalContext: closing provider %s", addr)

		delete(ctx.ProviderCache.providers, key)
		return cached.provider.Close()
	}

	return nil
}

func (ctx *BuiltinEvalContext) ConfigureProvider(addr addrs.AbsProviderConfig, cfg cty.Value) tfdiags.Diagnostics {
	var diags tfdiags.Diagnostics
	if !addr.Module.Equal(ctx.Path().Module()) {
		// This indicates incorrect use of ConfigureProvider: it should be used
		// only from the module that the provider configuration belongs to.
		panic(fmt.Sprintf("%s configured by wrong module %s", addr, ctx.Path()))
	}

	p := ctx.Provider(addr)
	if p == nil {
		diags = diags.Append(fmt.Errorf("%s not initialized", addr))
		return diags
	}

	key := addr.String()

	ctx.ProviderCache.mu.Lock()

	if _, exists := ctx.ProviderLocks[key]; !exists {
		ctx.ProviderLocks[key] = &sync.Mutex{}
	}
	l := ctx.ProviderLocks[key]

	l.Lock()
	defer l.Unlock()
	cached := ctx.ProviderCache.providers[key]
	configured := cached.configured

	ctx.ProviderCache.mu.Unlock()

	if configured {
		return diags
	}

	log.Printf("[INFO] BuiltinEvalContext: configuring provider %s", addr)
	providerSchema, err := ctx.ProviderSchema(addr)
	if err != nil {
		diags = diags.Append(fmt.Errorf("failed to read schema for %s: %s", addr, err))
		return diags
	}
	if providerSchema == nil {
		diags = diags.Append(fmt.Errorf("schema for %s is not available", addr))
		return diags
	}

	req := providers.ConfigureProviderRequest{
		TerraformVersion: version.String(),
		Config:           cfg,
	}

	resp := p.ConfigureProvider(req)
	cached.configured = true

	return resp.Diagnostics
}

func (ctx *BuiltinEvalContext) ProviderInput(pc addrs.AbsProviderConfig) map[string]cty.Value {
	ctx.ProviderInputLock.Lock()
	defer ctx.ProviderInputLock.Unlock()

	if !pc.Module.Equal(ctx.Path().Module()) {
		// This indicates incorrect use of InitProvider: it should be used
		// only from the module that the provider configuration belongs to.
		panic(fmt.Sprintf("%s initialized by wrong module %s", pc, ctx.Path()))
	}

	if !ctx.Path().IsRoot() {
		// Only root module provider configurations can have input.
		return nil
	}

	return ctx.ProviderInputConfig[pc.String()]
}

func (ctx *BuiltinEvalContext) SetProviderInput(pc addrs.AbsProviderConfig, c map[string]cty.Value) {
	absProvider := pc
	if !pc.Module.IsRoot() {
		// Only root module provider configurations can have input.
		log.Printf("[WARN] BuiltinEvalContext: attempt to SetProviderInput for non-root module")
		return
	}

	// Save the configuration
	ctx.ProviderInputLock.Lock()
	ctx.ProviderInputConfig[absProvider.String()] = c
	ctx.ProviderInputLock.Unlock()
}

func (ctx *BuiltinEvalContext) Provisioner(n string) (provisioners.Interface, error) {
	ctx.ProvisionerLock.Lock()
	defer ctx.ProvisionerLock.Unlock()

	p, ok := ctx.ProvisionerCache[n]
	if !ok {
		var err error
		p, err = ctx.Plugins.NewProvisionerInstance(n)
		if err != nil {
			return nil, err
		}

		ctx.ProvisionerCache[n] = p
	}

	return p, nil
}

func (ctx *BuiltinEvalContext) ProvisionerSchema(n string) (*configschema.Block, error) {
	return ctx.Plugins.ProvisionerSchema(n)
}

func (ctx *BuiltinEvalContext) CloseProvisioners() error {
	var diags tfdiags.Diagnostics
	ctx.ProvisionerLock.Lock()
	defer ctx.ProvisionerLock.Unlock()

	for name, prov := range ctx.ProvisionerCache {
		err := prov.Close()
		if err != nil {
			diags = diags.Append(fmt.Errorf("provisioner.Close %s: %s", name, err))
		}
	}

	return diags.Err()
}

func (ctx *BuiltinEvalContext) EvaluateBlock(body hcl.Body, schema *configschema.Block, self addrs.Referenceable, keyData InstanceKeyEvalData) (cty.Value, hcl.Body, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics
	scope := ctx.EvaluationScope(self, nil, keyData)
	body, evalDiags := scope.ExpandBlock(body, schema)
	diags = diags.Append(evalDiags)
	val, evalDiags := scope.EvalBlock(body, schema)
	diags = diags.Append(evalDiags)
	return val, body, diags
}

func (ctx *BuiltinEvalContext) EvaluateExpr(expr hcl.Expression, wantType cty.Type, self addrs.Referenceable) (cty.Value, tfdiags.Diagnostics) {
	scope := ctx.EvaluationScope(self, nil, EvalDataForNoInstanceKey)
	return scope.EvalExpr(expr, wantType)
}

func (ctx *BuiltinEvalContext) EvaluateReplaceTriggeredBy(expr hcl.Expression, repData instances.RepetitionData) (*addrs.Reference, bool, tfdiags.Diagnostics) {

	// get the reference to lookup changes in the plan
	ref, diags := evalReplaceTriggeredByExpr(expr, repData)
	if diags.HasErrors() {
		return nil, false, diags
	}

	var changes []*plans.ResourceInstanceChangeSrc
	// store the address once we get it for validation
	var resourceAddr addrs.Resource

	// The reference is either a resource or resource instance
	switch sub := ref.Subject.(type) {
	case addrs.Resource:
		resourceAddr = sub
		rc := sub.Absolute(ctx.Path())
		changes = ctx.Changes().GetChangesForAbsResource(rc)
	case addrs.ResourceInstance:
		resourceAddr = sub.ContainingResource()
		rc := sub.Absolute(ctx.Path())
		change := ctx.Changes().GetResourceInstanceChange(rc, states.CurrentGen)
		if change != nil {
			// we'll generate an error below if there was no change
			changes = append(changes, change)
		}
	}

	// Do some validation to make sure we are expecting a change at all
	cfg := ctx.Evaluator.Config.Descendent(ctx.Path().Module())
	resCfg := cfg.Module.ResourceByAddr(resourceAddr)
	if resCfg == nil {
		diags = diags.Append(&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  `Reference to undeclared resource`,
			Detail:   fmt.Sprintf(`A resource %s has not been declared in %s`, ref.Subject, moduleDisplayAddr(ctx.Path())),
			Subject:  expr.Range().Ptr(),
		})
		return nil, false, diags
	}

	if len(changes) == 0 {
		// If the resource is valid there should always be at least one change.
		diags = diags.Append(fmt.Errorf("no change found for %s in %s", ref.Subject, moduleDisplayAddr(ctx.Path())))
		return nil, false, diags
	}

	// If we don't have a traversal beyond the resource, then we can just look
	// for any change.
	if len(ref.Remaining) == 0 {
		for _, c := range changes {
			switch c.ChangeSrc.Action {
			// Only immediate changes to the resource will trigger replacement.
			case plans.Update, plans.DeleteThenCreate, plans.CreateThenDelete:
				return ref, true, diags
			}
		}

		// no change triggered
		return nil, false, diags
	}

	// This must be an instances to have a remaining traversal, which means a
	// single change.
	change := changes[0]

	// Make sure the change is actionable. A create or delete action will have
	// a change in value, but are not valid for our purposes here.
	switch change.ChangeSrc.Action {
	case plans.Update, plans.DeleteThenCreate, plans.CreateThenDelete:
		// OK
	default:
		return nil, false, diags
	}

	// Since we have a traversal after the resource reference, we will need to
	// decode the changes, which means we need a schema.
	providerAddr := change.ProviderAddr
	schema, err := ctx.ProviderSchema(providerAddr)
	if err != nil {
		diags = diags.Append(err)
		return nil, false, diags
	}

	resAddr := change.Addr.ContainingResource().Resource
	resSchema, _ := schema.SchemaForResourceType(resAddr.Mode, resAddr.Type)
	ty := resSchema.ImpliedType()

	before, err := change.ChangeSrc.Before.Decode(ty)
	if err != nil {
		diags = diags.Append(err)
		return nil, false, diags
	}

	after, err := change.ChangeSrc.After.Decode(ty)
	if err != nil {
		diags = diags.Append(err)
		return nil, false, diags
	}

	path := traversalToPath(ref.Remaining)
	attrBefore, _ := path.Apply(before)
	attrAfter, _ := path.Apply(after)

	if attrBefore == cty.NilVal || attrAfter == cty.NilVal {
		replace := attrBefore != attrAfter
		return ref, replace, diags
	}

	replace := !attrBefore.RawEquals(attrAfter)

	return ref, replace, diags
}

func (ctx *BuiltinEvalContext) EvaluationScope(self addrs.Referenceable, source addrs.Referenceable, keyData InstanceKeyEvalData) *lang.Scope {
	if !ctx.pathSet {
		panic("context path not set")
	}
	data := &evaluationStateData{
		Evaluator:       ctx.Evaluator,
		ModulePath:      ctx.PathValue,
		InstanceKeyData: keyData,
		Operation:       ctx.Evaluator.Operation,
		EncodeCache:     ctx.EncodeCache,
	}
	scope := ctx.Evaluator.Scope(data, self, source)

	// ctx.PathValue is the path of the module that contains whatever
	// expression the caller will be trying to evaluate, so this will
	// activate only the experiments from that particular module, to
	// be consistent with how experiment checking in the "configs"
	// package itself works. The nil check here is for robustness in
	// incompletely-mocked testing situations; mc should never be nil in
	// real situations.
	if mc := ctx.Evaluator.Config.DescendentForInstance(ctx.PathValue); mc != nil {
		scope.SetActiveExperiments(mc.Module.ActiveExperiments)
	}
	return scope
}

func (ctx *BuiltinEvalContext) Path() addrs.ModuleInstance {
	if !ctx.pathSet {
		panic("context path not set")
	}
	return ctx.PathValue
}

func (ctx *BuiltinEvalContext) SetRootModuleArgument(addr addrs.InputVariable, v cty.Value) {
	ctx.VariableValuesLock.Lock()
	defer ctx.VariableValuesLock.Unlock()

	log.Printf("[TRACE] BuiltinEvalContext: Storing final value for variable %s", addr.Absolute(addrs.RootModuleInstance))
	key := addrs.RootModuleInstance.String()
	args := ctx.VariableValues[key]
	if args == nil {
		args = make(map[string]cty.Value)
		ctx.VariableValues[key] = args
	}
	args[addr.Name] = v
}

func (ctx *BuiltinEvalContext) SetModuleCallArgument(callAddr addrs.ModuleCallInstance, varAddr addrs.InputVariable, v cty.Value) {
	ctx.VariableValuesLock.Lock()
	defer ctx.VariableValuesLock.Unlock()

	if !ctx.pathSet {
		panic("context path not set")
	}

	childPath := callAddr.ModuleInstance(ctx.PathValue)
	log.Printf("[TRACE] BuiltinEvalContext: Storing final value for variable %s", varAddr.Absolute(childPath))
	key := childPath.String()
	args := ctx.VariableValues[key]
	if args == nil {
		args = make(map[string]cty.Value)
		ctx.VariableValues[key] = args
	}
	args[varAddr.Name] = v
}

func (ctx *BuiltinEvalContext) GetVariableValue(addr addrs.AbsInputVariableInstance) cty.Value {
	ctx.VariableValuesLock.Lock()
	defer ctx.VariableValuesLock.Unlock()

	modKey := addr.Module.String()
	modVars := ctx.VariableValues[modKey]
	val, ok := modVars[addr.Variable.Name]
	if !ok {
		return cty.DynamicVal
	}
	return val
}

func (ctx *BuiltinEvalContext) Changes() *plans.ChangesSync {
	return ctx.ChangesValue
}

func (ctx *BuiltinEvalContext) State() *states.SyncState {
	return ctx.StateValue
}

func (ctx *BuiltinEvalContext) Checks() *checks.State {
	return ctx.ChecksValue
}

func (ctx *BuiltinEvalContext) RefreshState() *states.SyncState {
	return ctx.RefreshStateValue
}

func (ctx *BuiltinEvalContext) PrevRunState() *states.SyncState {
	return ctx.PrevRunStateValue
}

func (ctx *BuiltinEvalContext) InstanceExpander() *instances.Expander {
	return ctx.InstanceExpanderValue
}

func (ctx *BuiltinEvalContext) MoveResults() refactoring.MoveResults {
	return ctx.MoveResultsValue
}

func (ctx *BuiltinEvalContext) EncodeResource(addr addrs.AbsResourceInstance, obj *states.ResourceInstanceObject, ty cty.Type, schemaVersion uint64) (*states.ResourceInstanceObjectSrc, error) {
	key := addr.Resource.String()

	ctx.EncodeCache.Lock.RLock()
	var item *EncodeCacheItem
	if item, ok := ctx.EncodeCache.Items[key]; ok {
		if item.value == &obj.Value && item.src != nil {
			ctx.EncodeCache.Lock.RUnlock()
			log.Printf("[INFO] BuiltinEvalContext: EncodeResource cache hit %s", addr)
			return item.src, nil
		}
	}
	ctx.EncodeCache.Lock.RUnlock()

	src, err := obj.Encode(ty, schemaVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to encode %s in state: %s", addr, err)
	}

	if item != nil {
		item.value = &obj.Value
		item.src = src
	} else {
		ctx.EncodeCache.Lock.Lock()
		ctx.EncodeCache.Items[key] = &EncodeCacheItem{value: &obj.Value, src: src}
		ctx.EncodeCache.Lock.Unlock()
	}

	return src, nil
}

func (ctx *BuiltinEvalContext) SetData(addr addrs.AbsResourceInstance, obj *states.ResourceInstanceObject) {
	key := addr.Resource.String()

	// FIXME: use https://pkg.go.dev/sync#Map instead
	ctx.EncodeCache.Lock.Lock()
	ctx.EncodeCache.Items[key] = &EncodeCacheItem{value: &obj.Value}
	ctx.EncodeCache.Lock.Unlock()
}

func (ctx *BuiltinEvalContext) ClearCachedResource(addr addrs.AbsResourceInstance) {
	key := addr.Resource.String()

	ctx.EncodeCache.Lock.Lock()
	delete(ctx.EncodeCache.Items, key)
	ctx.EncodeCache.Lock.Unlock()
}
