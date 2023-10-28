// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package command

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/backend"
	"github.com/hashicorp/terraform/internal/command/arguments"
	"github.com/hashicorp/terraform/internal/command/views"
	"github.com/hashicorp/terraform/internal/plans"
	"github.com/hashicorp/terraform/internal/states/statefile"
	"github.com/hashicorp/terraform/internal/states/statemgr"
	"github.com/hashicorp/terraform/internal/terraform"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

type StartSessionCommand struct {
	Meta
	ProviderCache map[string]*terraform.CachedProvider
	StateManager  statemgr.Full
	Cache         *terraform.Cache
}

func (c *StartSessionCommand) Run(rawArgs []string) int {
	var diags tfdiags.Diagnostics

	// Parse and apply global view arguments
	common, rawArgs := arguments.ParseView(rawArgs)
	c.View.Configure(common)

	// Propagate -no-color for legacy use of Ui.  The remote backend and
	// cloud package use this; it should be removed when/if they are
	// migrated to views.
	c.Meta.color = !common.NoColor
	c.Meta.Color = c.Meta.color

	// Parse and validate flags
	var args *arguments.StartSession
	args, diags = arguments.ParseStartSession(rawArgs)

	// Instantiate the view, even if there are flag errors, so that we render
	// diagnostics according to the desired view
	view := views.NewStartSession(args.ViewType, c.View)

	if diags.HasErrors() {
		view.Diagnostics(diags)
		view.HelpPrompt()
		return 1
	}

	// Check for user-supplied plugin path
	var err error
	if c.pluginPath, err = c.loadPluginPath(); err != nil {
		diags = diags.Append(err)
		view.Diagnostics(diags)
		return 1
	}

	// FIXME: the -input flag value is needed to initialize the backend and the
	// operation, but there is no clear path to pass this value down, so we
	// continue to mutate the Meta object state for now.
	c.Meta.input = args.InputEnabled

	// FIXME: the -parallelism flag is used to control the concurrency of
	// Terraform operations. At the moment, this value is used both to
	// initialize the backend via the ContextOpts field inside CLIOpts, and to
	// set a largely unused field on the Operation request. Again, there is no
	// clear path to pass this value down, so we continue to mutate the Meta
	// object state for now.
	c.Meta.parallelism = args.Operation.Parallelism

	// Prepare the backend, passing the plan file if present, and the
	// backend-specific arguments
	be, beDiags := c.PrepareBackend(args.State, args.ViewType)
	diags = diags.Append(beDiags)
	if diags.HasErrors() {
		view.Diagnostics(diags)
		return 1
	}

	// Get the latest state.
	workspace := backend.DefaultStateName
	log.Printf("[TRACE] backend/local: requesting state manager for workspace %q", workspace)
	_, err = be.StateMgr(workspace)
	if err != nil {
		diags = diags.Append(fmt.Errorf("error loading state: %w", err))
		view.Diagnostics(diags)
		return 1
	}

	// stateLockerView := views.NewStateLocker(arguments.ViewHuman, c.View)
	// locker := clistate.NewLocker(0, stateLockerView).WithContext(context.Background())
	// if diags := locker.Lock(s, "session"); diags.HasErrors() {
	// 	view.Diagnostics(diags)
	// 	return 1
	// }

	view.Ready()
	c.Cache = terraform.NewCache()
	// defer func() {
	// 	diags := locker.Unlock()
	// 	if diags.HasErrors() {
	// 		view.Diagnostics(diags)
	// 		// runningOp.Result = backend.OperationFailure
	// 	}
	// }()

	return c.modePiped(view, be, args)
}

func (c *StartSessionCommand) PrepareBackend(args *arguments.State, viewType arguments.ViewType) (backend.Enhanced, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	// FIXME: we need to apply the state arguments to the meta object here
	// because they are later used when initializing the backend. Carving a
	// path to pass these arguments to the functions that need them is
	// difficult but would make their use easier to understand.
	c.Meta.applyStateArguments(args)

	// Load the backend
	var be backend.Enhanced
	var beDiags tfdiags.Diagnostics
	backendConfig, configDiags := c.loadBackendConfig(".")
	diags = diags.Append(configDiags)
	if configDiags.HasErrors() {
		return nil, diags
	}

	be, beDiags = c.Backend(&BackendOpts{
		Config:   backendConfig,
		ViewType: viewType,
	})

	diags = diags.Append(beDiags)
	if beDiags.HasErrors() {
		return nil, diags
	}
	return be, diags
}

func (c *StartSessionCommand) OperationRequest(
	be backend.Enhanced,
	view views.StartSession,
	viewType arguments.ViewType,
	args *arguments.Operation,
	autoApprove bool,
) (*backend.Operation, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	// Applying changes with dev overrides in effect could make it impossible
	// to switch back to a release version if the schema isn't compatible,
	// so we'll warn about it.
	diags = diags.Append(c.providerDevOverrideRuntimeWarnings())

	// Build the operation
	opReq := c.Operation(be, viewType)
	opReq.AutoApprove = autoApprove
	opReq.ConfigDir = "."
	opReq.PlanMode = args.PlanMode
	opReq.Hooks = view.Hooks()
	opReq.PlanRefresh = args.Refresh
	opReq.Targets = args.Targets
	opReq.ForceReplace = args.ForceReplace
	opReq.Type = backend.OperationTypeApply
	opReq.View = view.Operation()
	opReq.KeepAlive = c.KeepAlive
	opReq.ProviderCache = c.ProviderCache
	opReq.StateManager = c.StateManager
	opReq.Cache = c.Cache

	var err error
	opReq.ConfigLoader, err = c.initConfigLoader()
	if err != nil {
		diags = diags.Append(fmt.Errorf("failed to initialize config loader: %s", err))
		return nil, diags
	}

	return opReq, diags
}

func (c *StartSessionCommand) handleInput(be backend.Enhanced, args *arguments.StartSession, applyArgs *arguments.Apply) tfdiags.Diagnostics {
	diags := tfdiags.Diagnostics{}

	view := views.NewStartSession(args.ViewType, c.View)

	// Build the operation request
	opReq, opDiags := c.OperationRequest(be, view, args.ViewType, applyArgs.Operation, applyArgs.AutoApprove)
	diags = diags.Append(opDiags)

	isDestroy := applyArgs.Operation.PlanMode == plans.DestroyMode
	// TODO: if destroy _and_ `useTests` then we should only destroy test resources
	if len(opReq.Targets) == 0 && !isDestroy && (c.Meta.useTests || len(c.modules) > 0) {
		targets, targetsDiags := c.Meta.loadTargets(".")
		diags = diags.Append(targetsDiags)
		opReq.Targets = targets

		if len(targets) == 0 {
			// No changes needed
			view.ResourceCount(args.State.StateOutPath)

			return diags
		}
	}

	// Collect variable value and add them to the operation request
	diags = diags.Append(c.GatherVariables(opReq, args.Vars))
	if diags.HasErrors() {
		return diags
	}

	// Run the operation
	op, err := c.RunOperation(be, opReq)
	if err != nil {
		return diags.Append(err)
	}

	if op.Result != backend.OperationSuccess {
		return diags
	}

	if opReq.ProviderCache != nil {
		c.ProviderCache = opReq.ProviderCache
	}

	if opReq.StateManager != nil {
		c.StateManager = opReq.StateManager
	}

	// Render the resource count and outputs, unless those counts are being
	// rendered already in a remote Terraform process.
	if rb, isRemoteBackend := be.(BackendWithRemoteTerraformVersion); !isRemoteBackend || rb.IsLocalOperations() {
		view.ResourceCount(args.State.StateOutPath)
		if op.State != nil {
			view.Outputs(op.State.RootModule().OutputValues)
		}
	}

	return diags
}

func (c *StartSessionCommand) modePiped(view views.StartSession, be backend.Enhanced, args *arguments.StartSession) int {
	needsRefresh := args.Operation.Refresh
	scanner := bufio.NewScanner(os.Stdin)
	c.KeepAlive = true
	ops, _ := c.contextOpts()
	tfCtx, _ := terraform.NewContext(ops)

	for scanner.Scan() {
		token := scanner.Text()
		line := strings.TrimSpace(token)
		parts := strings.Split(line, " ") // FIXME: this is not robust for args with spaces

		log.Printf("[INFO] got command %s", parts[0])

		switch parts[0] {
		case "apply":
			common, rawArgs := arguments.ParseView(parts[1:])
			c.Meta.useTests = common.UseTests
			c.Meta.modules = common.Modules

			applyArgs, d := arguments.ParseApply(rawArgs)
			if d.HasErrors() {
				view.Diagnostics(d)
				continue
			}

			applyArgs.Operation.Refresh = needsRefresh
			d = c.handleInput(be, args, applyArgs)
			needsRefresh = false
			if d.HasErrors() {
				view.Diagnostics(d)
				continue
			}
		case "reload-config":
			// Dump the config cache
			c.configLoader = nil
		case "get-state":
			if c.StateManager == nil {
				view.Diagnostics(tfdiags.Diagnostics{}.Append(fmt.Errorf("No state manager available")))
				continue
			}

			s := c.StateManager.State()
			f := statefile.New(s, "", 0)
			var buf bytes.Buffer
			err := statefile.Write(f, &buf)
			if err != nil {
				view.Diagnostics(tfdiags.Diagnostics{}.Append(err))
				continue
			}

			view.PrintData(json.RawMessage(buf.Bytes()))
		case "apply-config":
			rAddr, d := addrs.ParseAbsResourceStr(parts[1])
			if d.HasErrors() {
				view.Diagnostics(d)
				continue
			}

			config, d := c.loadConfig(".")
			if d.HasErrors() {
				view.Diagnostics(d)
				continue
			}

			sm, _ := be.StateMgr(backend.DefaultStateName)
			err := sm.RefreshState()
			if err != nil {
				view.Diagnostics(tfdiags.Diagnostics{}.Append(err))
				continue
			}
			s := sm.State()
			walker := tfCtx.GraphWalker(s, config)
			a := terraform.GetAllocator(config)
			ctx := walker.EvalContext().WithPath(addrs.RootModuleInstance)
			rConfig := config.Module.ResourceByAddr(rAddr.Resource)
			result, d := a.Apply(ctx, rAddr.Resource, rConfig)
			if d.HasErrors() {
				view.Diagnostics(d)
				continue
			}

			encoded, err := result.State.Encode(result.Schema.ImpliedType(), result.SchemaVersion)
			if err != nil {
				view.Diagnostics(tfdiags.Diagnostics{}.Append(err))
				continue
			}
			providerAddr := config.ResolveAbsProviderAddr(rConfig.ProviderConfigAddr(), addrs.RootModule)
			s.RootModule().SetResourceInstanceCurrent(rAddr.Resource.Instance(addrs.NoKey), encoded, providerAddr)
			sm.WriteState(s)
			sm.RefreshState()

			// Dump the config cache
			c.configLoader = nil
		case "get-refs":
			rAddr, d := addrs.ParseAbsResourceStr(parts[1])
			if d.HasErrors() {
				view.Diagnostics(d)
				continue
			}

			config, d := c.loadConfig(".")
			if d.HasErrors() {
				view.Diagnostics(d)
				continue
			}

			sm, _ := be.StateMgr(backend.DefaultStateName)
			err := sm.RefreshState()
			if err != nil {
				view.Diagnostics(tfdiags.Diagnostics{}.Append(err))
				continue
			}
			s := sm.State()
			walker := tfCtx.GraphWalker(s, config)
			a := terraform.GetAllocator(config)
			ctx := walker.EvalContext().WithPath(addrs.RootModuleInstance)
			refs, d := a.GetRefs(ctx, rAddr.Resource)
			if d.HasErrors() {
				view.Diagnostics(d)
				continue
			}

			err = view.PrintRefs(refs)
			if err != nil {
				view.Diagnostics(tfdiags.Diagnostics{}.Append(err))
				continue
			}
		case "exit":
			return 0
		}

		view.Ready()
	}

	return 0
}

func (c *StartSessionCommand) GatherVariables(opReq *backend.Operation, args *arguments.Vars) tfdiags.Diagnostics {
	var diags tfdiags.Diagnostics

	// FIXME the arguments package currently trivially gathers variable related
	// arguments in a heterogenous slice, in order to minimize the number of
	// code paths gathering variables during the transition to this structure.
	// Once all commands that gather variables have been converted to this
	// structure, we could move the variable gathering code to the arguments
	// package directly, removing this shim layer.

	varArgs := args.All()
	items := make([]rawFlag, len(varArgs))
	for i := range varArgs {
		items[i].Name = varArgs[i].Name
		items[i].Value = varArgs[i].Value
	}
	c.Meta.variableArgs = rawFlags{items: &items}
	opReq.Variables, diags = c.collectVariableValues()

	return diags
}

func (c *StartSessionCommand) Synopsis() string {
	return "Create or update infrastructure"
}

func (c *StartSessionCommand) Help() string {
	helpText := `
Usage: terraform [global options] apply [options] [PLAN]

  Creates or updates infrastructure according to Terraform configuration
  files in the current directory.

  By default, Terraform will generate a new plan and present it for your
  approval before taking any action. You can optionally provide a plan
  file created by a previous call to "terraform plan", in which case
  Terraform will take the actions described in that plan without any
  confirmation prompt.

Options:

  -auto-approve          Skip interactive approval of plan before applying.

  -backup=path           Path to backup the existing state file before
                         modifying. Defaults to the "-state-out" path with
                         ".backup" extension. Set to "-" to disable backup.

  -compact-warnings      If Terraform produces any warnings that are not
                         accompanied by errors, show them in a more compact
                         form that includes only the summary messages.

  -destroy               Destroy Terraform-managed infrastructure.
                         The command "terraform destroy" is a convenience alias
                         for this option.

  -lock=false            Don't hold a state lock during the operation. This is
                         dangerous if others might concurrently run commands
                         against the same workspace.

  -lock-timeout=0s       Duration to retry a state lock.

  -input=true            Ask for input for variables if not directly set.

  -no-color              If specified, output won't contain any color.

  -parallelism=n         Limit the number of parallel resource operations.
                         Defaults to 10.

  -state=path            Path to read and save state (unless state-out
                         is specified). Defaults to "terraform.tfstate".

  -state-out=path        Path to write state to that is different than
                         "-state". This can be used to preserve the old
                         state.

  If you don't provide a saved plan file then this command will also accept
  all of the plan-customization options accepted by the terraform plan command.
  For more information on those options, run:
      terraform plan -help
`
	return strings.TrimSpace(helpText)
}
