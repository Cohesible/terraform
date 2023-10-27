// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package http

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/hashicorp/terraform/internal/backend"
	"github.com/hashicorp/terraform/internal/command/clistate"
	"github.com/hashicorp/terraform/internal/configs"
	"github.com/hashicorp/terraform/internal/configs/configload"
	"github.com/hashicorp/terraform/internal/states/statemgr"
	"github.com/hashicorp/terraform/internal/terraform"
	"github.com/hashicorp/terraform/internal/tfdiags"
	"github.com/zclconf/go-cty/cty"
)

// backend.Local implementation.
func (b *Backend) LocalRun(op *backend.Operation) (*backend.LocalRun, statemgr.Full, tfdiags.Diagnostics) {
	// Make sure the type is invalid. We use this as a way to know not
	// to ask for input/validate. We're modifying this through a pointer,
	// so we're mutating an object that belongs to the caller here, which
	// seems bad but we're preserving it for now until we have time to
	// properly design this API, vs. just preserving whatever it currently
	// happens to do.
	op.Type = backend.OperationTypeInvalid

	op.StateLocker = clistate.NewNoopLocker()

	lr, _, stateMgr, diags := b.localRun(op)
	return lr, stateMgr, diags
}

func (b *Backend) localRun(op *backend.Operation) (*backend.LocalRun, *configload.Snapshot, statemgr.Full, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	s, err := op.InitStateManager(b)
	if err != nil {
		diags = diags.Append(fmt.Errorf("error loading state: %w", err))
		return nil, nil, nil, diags
	}

	ret := &backend.LocalRun{}

	// Initialize our context options
	var coreOpts terraform.ContextOpts
	if v := b.ContextOpts; v != nil {
		coreOpts = *v
	}
	coreOpts.UIInput = op.UIIn
	coreOpts.Hooks = op.Hooks

	if op.KeepAlive {
		// For keeping providers alive across commands
		coreOpts.KeepAlive = true
		coreOpts.ProviderCache = op.ProviderCache
	}

	var ctxDiags tfdiags.Diagnostics
	var configSnap *configload.Snapshot
	log.Printf("[TRACE] backend/local: populating backend.LocalRun for current working directory")
	ret, configSnap, ctxDiags = b.localRunDirect(op, ret, &coreOpts, s)
	diags = diags.Append(ctxDiags)
	if diags.HasErrors() {
		return nil, nil, nil, diags
	}

	return ret, configSnap, s, diags
}

func (b *Backend) localRunDirect(op *backend.Operation, run *backend.LocalRun, coreOpts *terraform.ContextOpts, s statemgr.Full) (*backend.LocalRun, *configload.Snapshot, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	// Load the configuration using the caller-provided configuration loader.
	config, configSnap, configDiags := op.ConfigLoader.LoadConfigWithSnapshot(op.ConfigDir)
	diags = diags.Append(configDiags)
	if configDiags.HasErrors() {
		return nil, nil, diags
	}
	run.Config = config

	if errs := config.VerifyDependencySelections(op.DependencyLocks); len(errs) > 0 {
		var buf strings.Builder
		for _, err := range errs {
			fmt.Fprintf(&buf, "\n  - %s", err.Error())
		}
		var suggestion string
		switch {
		case op.DependencyLocks == nil:
			// If we get here then it suggests that there's a caller that we
			// didn't yet update to populate DependencyLocks, which is a bug.
			suggestion = "This run has no dependency lock information provided at all, which is a bug in Terraform; please report it!"
		case op.DependencyLocks.Empty():
			suggestion = "To make the initial dependency selections that will initialize the dependency lock file, run:\n  terraform init"
		default:
			suggestion = "To update the locked dependency selections to match a changed configuration, run:\n  terraform init -upgrade"
		}
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Inconsistent dependency lock file",
			fmt.Sprintf(
				"The following dependency selections recorded in the lock file are inconsistent with the current configuration:%s\n\n%s",
				buf.String(), suggestion,
			),
		))
	}

	var rawVariables map[string]backend.UnparsedVariableValue
	if op.AllowUnsetVariables {
		// Rather than prompting for input, we'll just stub out the required
		// but unset variables with unknown values to represent that they are
		// placeholders for values the user would need to provide for other
		// operations.
		rawVariables = b.stubUnsetRequiredVariables(op.Variables, config.Module.Variables)
	} else {
		// If interactive input is enabled, we might gather some more variable
		// values through interactive prompts.
		// TODO: Need to route the operation context through into here, so that
		// the interactive prompts can be sensitive to its timeouts/etc.
		rawVariables = b.interactiveCollectVariables(context.TODO(), op.Variables, config.Module.Variables, op.UIIn)
	}

	variables, varDiags := backend.ParseVariableValues(rawVariables, config.Module.Variables)
	diags = diags.Append(varDiags)
	if diags.HasErrors() {
		return nil, nil, diags
	}

	planOpts := &terraform.PlanOpts{
		Mode:               op.PlanMode,
		Targets:            op.Targets,
		ForceReplace:       op.ForceReplace,
		SetVariables:       variables,
		SkipRefresh:        op.Type != backend.OperationTypeRefresh && !op.PlanRefresh,
		GenerateConfigPath: op.GenerateConfigOut,
		Cache:              op.Cache,
	}
	run.PlanOpts = planOpts

	// For a "direct" local run, the input state is the most recently stored
	// snapshot, from the previous run.
	run.InputState = s.State()

	tfCtx, moreDiags := terraform.NewContext(coreOpts)
	diags = diags.Append(moreDiags)
	if moreDiags.HasErrors() {
		return nil, nil, diags
	}
	run.Core = tfCtx

	return run, configSnap, diags
}

// interactiveCollectVariables attempts to complete the given existing
// map of variables by interactively prompting for any variables that are
// declared as required but not yet present.
//
// If interactive input is disabled for this backend instance then this is
// a no-op. If input is enabled but fails for some reason, the resulting
// map will be incomplete. For these reasons, the caller must still validate
// that the result is complete and valid.
//
// This function does not modify the map given in "existing", but may return
// it unchanged if no modifications are required. If modifications are required,
// the result is a new map with all of the elements from "existing" plus
// additional elements as appropriate.
//
// Interactive prompting is a "best effort" thing for first-time user UX and
// not something we expect folks to be relying on for routine use. Terraform
// is primarily a non-interactive tool and so we prefer to report in error
// messages that variables are not set rather than reporting that input failed:
// the primary resolution to missing variables is to provide them by some other
// means.
func (b *Backend) interactiveCollectVariables(ctx context.Context, existing map[string]backend.UnparsedVariableValue, vcs map[string]*configs.Variable, uiInput terraform.UIInput) map[string]backend.UnparsedVariableValue {
	var needed []string
	// if b.OpInput && uiInput != nil {
	// 	for name, vc := range vcs {
	// 		if !vc.Required() {
	// 			continue // We only prompt for required variables
	// 		}
	// 		if _, exists := existing[name]; !exists {
	// 			needed = append(needed, name)
	// 		}
	// 	}
	// } else {
	// 	log.Print("[DEBUG] backend/local: Skipping interactive prompts for variables because input is disabled")
	// }
	if len(needed) == 0 {
		return existing
	}

	log.Printf("[DEBUG] backend/local: will prompt for input of unset required variables %s", needed)

	// If we get here then we're planning to prompt for at least one additional
	// variable's value.
	sort.Strings(needed) // prompt in lexical order
	ret := make(map[string]backend.UnparsedVariableValue, len(vcs))
	for k, v := range existing {
		ret[k] = v
	}
	for _, name := range needed {
		vc := vcs[name]
		rawValue, err := uiInput.Input(ctx, &terraform.InputOpts{
			Id:          fmt.Sprintf("var.%s", name),
			Query:       fmt.Sprintf("var.%s", name),
			Description: vc.Description,
			Secret:      vc.Sensitive,
		})
		if err != nil {
			// Since interactive prompts are best-effort, we'll just continue
			// here and let subsequent validation report this as a variable
			// not specified.
			log.Printf("[WARN] backend/local: Failed to request user input for variable %q: %s", name, err)
			continue
		}
		ret[name] = unparsedInteractiveVariableValue{Name: name, RawValue: rawValue}
	}
	return ret
}

// stubUnsetVariables ensures that all required variables defined in the
// configuration exist in the resulting map, by adding new elements as necessary.
//
// The stubbed value of any additions will be an unknown variable conforming
// to the variable's configured type constraint, meaning that no particular
// value is known and that one must be provided by the user in order to get
// a complete result.
//
// Unset optional attributes (those with default values) will not be populated
// by this function, under the assumption that a later step will handle those.
// In this sense, stubUnsetRequiredVariables is essentially a non-interactive,
// non-error-producing variant of interactiveCollectVariables that creates
// placeholders for values the user would be prompted for interactively on
// other operations.
//
// This function should be used only in situations where variables values
// will not be directly used and the variables map is being constructed only
// to produce a complete Terraform context for some ancillary functionality
// like "terraform console", "terraform state ...", etc.
//
// This function is guaranteed not to modify the given map, but it may return
// the given map unchanged if no additions are required. If additions are
// required then the result will be a new map containing everything in the
// given map plus additional elements.
func (b *Backend) stubUnsetRequiredVariables(existing map[string]backend.UnparsedVariableValue, vcs map[string]*configs.Variable) map[string]backend.UnparsedVariableValue {
	var missing bool // Do we need to add anything?
	for name, vc := range vcs {
		if !vc.Required() {
			continue // We only stub required variables
		}
		if _, exists := existing[name]; !exists {
			missing = true
		}
	}
	if !missing {
		return existing
	}

	// If we get down here then there's at least one variable value to add.
	ret := make(map[string]backend.UnparsedVariableValue, len(vcs))
	for k, v := range existing {
		ret[k] = v
	}
	for name, vc := range vcs {
		if !vc.Required() {
			continue
		}
		if _, exists := existing[name]; !exists {
			ret[name] = unparsedUnknownVariableValue{Name: name, WantType: vc.Type}
		}
	}
	return ret
}

type unparsedInteractiveVariableValue struct {
	Name, RawValue string
}

var _ backend.UnparsedVariableValue = unparsedInteractiveVariableValue{}

func (v unparsedInteractiveVariableValue) ParseVariableValue(mode configs.VariableParsingMode) (*terraform.InputValue, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics
	val, valDiags := mode.Parse(v.Name, v.RawValue)
	diags = diags.Append(valDiags)
	if diags.HasErrors() {
		return nil, diags
	}
	return &terraform.InputValue{
		Value:      val,
		SourceType: terraform.ValueFromInput,
	}, diags
}

type unparsedUnknownVariableValue struct {
	Name     string
	WantType cty.Type
}

var _ backend.UnparsedVariableValue = unparsedUnknownVariableValue{}

func (v unparsedUnknownVariableValue) ParseVariableValue(mode configs.VariableParsingMode) (*terraform.InputValue, tfdiags.Diagnostics) {
	return &terraform.InputValue{
		Value:      cty.UnknownVal(v.WantType),
		SourceType: terraform.ValueFromInput,
	}, nil
}
