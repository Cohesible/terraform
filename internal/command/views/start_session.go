// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package views

import (
	jsonencoding "encoding/json"
	"fmt"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/command/arguments"
	"github.com/hashicorp/terraform/internal/command/format"
	"github.com/hashicorp/terraform/internal/command/views/json"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/terraform"
	"github.com/hashicorp/terraform/internal/tfdiags"
	"github.com/zclconf/go-cty/cty"
)

type StartSession interface {
	ResourceCount(stateOutPath string)
	Outputs(outputValues map[string]*states.OutputValue)

	Operation() Operation
	Hooks() []terraform.Hook

	Diagnostics(diags tfdiags.Diagnostics)
	HelpPrompt()

	Ready()

	PrintRefs(refs []*addrs.Reference) error
}

func NewStartSession(vt arguments.ViewType, view *View) StartSession {
	switch vt {
	case arguments.ViewJSON:
		return &StartSessionJSON{
			view:      NewJSONView(view),
			countHook: &countHook{},
		}
	case arguments.ViewHuman:
		return &StartSessionHuman{
			view:         view,
			inAutomation: view.RunningInAutomation(),
			countHook:    &countHook{},
		}
	default:
		panic(fmt.Sprintf("unknown view type %v", vt))
	}
}

type StartSessionHuman struct {
	view *View

	inAutomation bool

	countHook *countHook
}

var _ StartSession = (*StartSessionHuman)(nil)

func (v *StartSessionHuman) ResourceCount(stateOutPath string) {
	if v.countHook.Imported > 0 {
		v.view.streams.Printf(
			v.view.colorize.Color("[reset][bold][green]\nApply complete! Resources: %d imported, %d added, %d changed, %d destroyed.\n"),
			v.countHook.Imported,
			v.countHook.Added,
			v.countHook.Changed,
			v.countHook.Removed,
		)
	} else {
		v.view.streams.Printf(
			v.view.colorize.Color("[reset][bold][green]\nApply complete! Resources: %d added, %d changed, %d destroyed.\n"),
			v.countHook.Added,
			v.countHook.Changed,
			v.countHook.Removed,
		)
	}
	if (v.countHook.Added > 0 || v.countHook.Changed > 0) && stateOutPath != "" {
		v.view.streams.Printf("\n%s\n\n", format.WordWrap(stateOutPathPostApply, v.view.outputColumns()))
		v.view.streams.Printf("State path: %s\n", stateOutPath)
	}
}

func (v *StartSessionHuman) Outputs(outputValues map[string]*states.OutputValue) {
	if len(outputValues) > 0 {
		v.view.streams.Print(v.view.colorize.Color("[reset][bold][green]\nOutputs:\n\n"))
		NewOutput(arguments.ViewHuman, v.view).Output("", outputValues)
	}
}

func (v *StartSessionHuman) Operation() Operation {
	return NewOperation(arguments.ViewHuman, v.inAutomation, v.view)
}

func (v *StartSessionHuman) Hooks() []terraform.Hook {
	return []terraform.Hook{
		v.countHook,
		NewUiHook(v.view),
	}
}

func (v *StartSessionHuman) Diagnostics(diags tfdiags.Diagnostics) {
	v.view.Diagnostics(diags)
}

func (v *StartSessionHuman) HelpPrompt() {
	v.view.HelpPrompt("start-session")
}

func (v *StartSessionHuman) Ready() {
	v.view.streams.Print(v.view.colorize.Color("[reset][bold][green]Ready\n\n"))
}

func (v *StartSessionHuman) PrintRefs(refs []*addrs.Reference) error {
	for _, ref := range refs {
		v.view.streams.Println(ref.DisplayString())
	}
	return nil
}

// The ApplyJSON implementation renders streaming JSON logs, suitable for
// integrating with other software.
type StartSessionJSON struct {
	view *JSONView

	countHook *countHook
}

var _ StartSession = (*StartSessionJSON)(nil)

func (v *StartSessionJSON) ResourceCount(stateOutPath string) {
	operation := json.OperationApplied
	v.view.ChangeSummary(&json.ChangeSummary{
		Add:       v.countHook.Added,
		Change:    v.countHook.Changed,
		Remove:    v.countHook.Removed,
		Import:    v.countHook.Imported,
		Operation: operation,
	})
}

func (v *StartSessionJSON) Outputs(outputValues map[string]*states.OutputValue) {
	outputs, diags := json.OutputsFromMap(outputValues)
	if diags.HasErrors() {
		v.Diagnostics(diags)
	} else {
		v.view.Outputs(outputs)
	}
}

func (v *StartSessionJSON) Operation() Operation {
	return &OperationJSON{view: v.view}
}

func (v *StartSessionJSON) Hooks() []terraform.Hook {
	return []terraform.Hook{
		v.countHook,
		newJSONHook(v.view),
	}
}

func (v *StartSessionJSON) Diagnostics(diags tfdiags.Diagnostics) {
	v.view.Diagnostics(diags)
}

func (v *StartSessionJSON) HelpPrompt() {
}

func (v *StartSessionJSON) Ready() {
	v.view.Ready()
}

type reference struct {
	Subject     string       `json:"subject"`
	Expressions []expression `json:"expressions"`
}

type expression struct {
	Type  string                  `json:"type"`
	Value jsonencoding.RawMessage `json:"value"`
}

func (v *StartSessionJSON) PrintRefs(refs []*addrs.Reference) error {
	diags := tfdiags.Diagnostics{}
	output := make([]reference, 0)
	for _, ref := range refs {
		expressions := make([]expression, 0)

		marshal := func(t string, v interface{}) {
			value, err := jsonencoding.Marshal(v)
			if err != nil {
				diags = diags.Append(err)
			} else {
				expressions = append(expressions, expression{Type: t, Value: value})
			}
		}

		for _, step := range ref.Remaining {
			switch tStep := step.(type) {
			case hcl.TraverseRoot:
				marshal("root", tStep.Name)
			case hcl.TraverseAttr:
				marshal("property", tStep.Name)
			case hcl.TraverseIndex:
				switch tStep.Key.Type() {
				case cty.String:
					val := fmt.Sprintf("%q", tStep.Key.AsString())
					marshal("element", val)
				case cty.Number:
					val, _ := tStep.Key.AsBigFloat().Uint64()
					marshal("element", val)
				}
			}
		}

		subject := ref.Subject.String()
		if subject != "" {
			output = append(output, reference{
				Subject:     ref.Subject.String(),
				Expressions: expressions,
			})
		}
	}

	bytes, err := jsonencoding.Marshal(output)
	if err != nil {
		return err
	}

	v.view.Result(bytes)

	return nil
}
