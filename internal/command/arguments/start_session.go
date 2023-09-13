// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package arguments

import (
	"github.com/hashicorp/terraform/internal/tfdiags"
)

type StartSession struct {
	// State, Operation, and Vars are the common extended flags
	State     *State
	Operation *Operation
	Vars      *Vars

	// AutoApprove skips the manual verification step for the apply operation.
	AutoApprove bool

	// InputEnabled is used to disable interactive input for unspecified
	// variable and backend config values. Default is true.
	InputEnabled bool

	// ViewType specifies which output format to use
	ViewType ViewType
}

func ParseStartSession(args []string) (*StartSession, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics
	start_session := &StartSession{
		State:     &State{},
		Operation: &Operation{},
		Vars:      &Vars{},
	}

	cmdFlags := extendedFlagSet("apply", start_session.State, start_session.Operation, start_session.Vars)
	cmdFlags.BoolVar(&start_session.AutoApprove, "auto-approve", false, "auto-approve")
	cmdFlags.BoolVar(&start_session.InputEnabled, "input", true, "input")

	var json bool
	cmdFlags.BoolVar(&json, "json", false, "json")

	if err := cmdFlags.Parse(args); err != nil {
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Failed to parse command-line flags",
			err.Error(),
		))
	}

	args = cmdFlags.Args()
	if len(args) > 0 {
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Too many command line arguments",
			"Expected no positional arguments.",
		))
	}

	// JSON view currently does not support input, so we disable it here.
	if json {
		start_session.InputEnabled = false
	}

	// JSON view cannot confirm apply, so we require either a plan file or
	// auto-approve to be specified. We intentionally fail here rather than
	// override auto-approve, which would be dangerous.
	if json && !start_session.AutoApprove {
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Auto-approve required",
			"Terraform cannot ask for interactive approval when -json is set. You must enable the -auto-approve option.",
		))
	}

	diags = diags.Append(start_session.Operation.Parse())

	switch {
	case json:
		start_session.ViewType = ViewJSON
	default:
		start_session.ViewType = ViewHuman
	}

	return start_session, diags
}
