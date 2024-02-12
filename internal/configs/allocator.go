// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package configs

import (
	"fmt"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/hcl"
	"github.com/hashicorp/terraform/internal/hcl/hclsyntax"
	hcljson "github.com/hashicorp/terraform/internal/hcl/json"
	"github.com/hashicorp/terraform/internal/lang"
	"github.com/hashicorp/terraform/internal/tfdiags"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/convert"
)

type Allocator struct {
	Scope     string
	Module    string
	Endpoint  string
	Resources []*addrs.Reference
	DeclRange hcl.Range
}

func decodeAllocatorBlock(block *hcl.Block, override bool) (*Allocator, hcl.Diagnostics) {
	var diags hcl.Diagnostics
	r := &Allocator{
		DeclRange: block.DefRange,
	}

	content, moreDiags := block.Body.Content(AllocatorBlockSchema)
	diags = append(diags, moreDiags...)

	if attr, exists := content.Attributes["scope"]; exists {
		scope, hclDiags := DecodeAsString(attr)
		diags = diags.Extend(hclDiags)
		r.Scope = scope
	}

	if attr, exists := content.Attributes["module"]; exists {
		module, hclDiags := DecodeAsString(attr)
		diags = diags.Extend(hclDiags)
		r.Module = module
	}

	if attr, exists := content.Attributes["endpoint"]; exists {
		endpoint, hclDiags := DecodeAsString(attr)
		diags = diags.Extend(hclDiags)
		r.Endpoint = endpoint
	}

	if attr, exists := content.Attributes["resources"]; exists {
		refs, hclDiags := decodeResources(attr.Expr)
		diags = diags.Extend(hclDiags)
		r.Resources = refs
	}

	return r, diags
}

func DecodeAsString(attr *hcl.Attribute) (string, hcl.Diagnostics) {
	val, diags := attr.Expr.Value(nil)
	if diags.HasErrors() {
		return "", diags
	}
	var err error
	val, err = convert.Convert(val, cty.String)
	if err != nil {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "Invalid attribute",
			Detail:   fmt.Sprintf("A string value is required for %s.", attr.Name),
			Subject:  attr.Expr.Range().Ptr(),
		})
		return "", diags
	}

	if val.IsNull() {
		// A null version constraint is strange, but we'll just treat it
		// like an empty constraint set.
		return "", diags
	}

	if !val.IsWhollyKnown() {
		// If there is a syntax error, HCL sets the value of the given attribute
		// to cty.DynamicVal. A diagnostic for the syntax error will already
		// bubble up, so we will move forward gracefully here.
		return "", diags
	}

	return val.AsString(), nil
}

func decodeResources(expr hcl.Expression) ([]*addrs.Reference, hcl.Diagnostics) {
	// Since we are manually parsing the replace_triggered_by argument, we
	// need to specially handle json configs, in which case the values will
	// be json strings rather than hcl. To simplify parsing however we will
	// decode the individual list elements, rather than the entire expression.
	isJSON := hcljson.IsJSONExpression(expr)

	exprs, diags := hcl.ExprList(expr)
	ret := make([]*addrs.Reference, 0)

	for i, expr := range exprs {
		if isJSON {
			// We can abuse the hcl json api and rely on the fact that calling
			// Value on a json expression with no EvalContext will return the
			// raw string. We can then parse that as normal hcl syntax, and
			// continue with the decoding.
			v, ds := expr.Value(nil)
			diags = diags.Extend(ds)
			if diags.HasErrors() {
				continue
			}

			expr, ds = hclsyntax.ParseExpression([]byte(v.AsString()), "", expr.Range().Start)
			diags = diags.Extend(ds)
			if diags.HasErrors() {
				continue
			}
			// make sure to swap out the expression we're returning too
			exprs[i] = expr
		}

		refs, refDiags := lang.ReferencesInExpr(expr)
		for _, diag := range refDiags {
			severity := hcl.DiagError
			if diag.Severity() == tfdiags.Warning {
				severity = hcl.DiagWarning
			}

			desc := diag.Description()

			diags = append(diags, &hcl.Diagnostic{
				Severity: severity,
				Summary:  desc.Summary,
				Detail:   desc.Detail,
				Subject:  expr.Range().Ptr(),
			})
		}

		if refDiags.HasErrors() {
			continue
		}

		resourceCount := 0
		for _, ref := range refs {
			switch sub := ref.Subject.(type) {
			case addrs.Resource, addrs.ResourceInstance:
				ret = append(ret, ref)
				resourceCount++

			case addrs.ForEachAttr:
				if sub.Name != "key" {
					diags = append(diags, &hcl.Diagnostic{
						Severity: hcl.DiagError,
						Summary:  "Invalid each reference in replace_triggered_by expression",
						Detail:   "Only each.key may be used in replace_triggered_by.",
						Subject:  expr.Range().Ptr(),
					})
				}
			case addrs.CountAttr:
				if sub.Name != "index" {
					diags = append(diags, &hcl.Diagnostic{
						Severity: hcl.DiagError,
						Summary:  "Invalid count reference in replace_triggered_by expression",
						Detail:   "Only count.index may be used in replace_triggered_by.",
						Subject:  expr.Range().Ptr(),
					})
				}
			default:
				// everything else should be simple traversals
				diags = append(diags, &hcl.Diagnostic{
					Severity: hcl.DiagError,
					Summary:  "Invalid reference in replace_triggered_by expression",
					Detail:   "Only resources, count.index, and each.key may be used in replace_triggered_by.",
					Subject:  expr.Range().Ptr(),
				})
			}
		}

		switch {
		case resourceCount == 0:
			diags = append(diags, &hcl.Diagnostic{
				Severity: hcl.DiagError,
				Summary:  "Invalid replace_triggered_by expression",
				Detail:   "Missing resource reference in replace_triggered_by expression.",
				Subject:  expr.Range().Ptr(),
			})
		case resourceCount > 1:
			diags = append(diags, &hcl.Diagnostic{
				Severity: hcl.DiagError,
				Summary:  "Invalid replace_triggered_by expression",
				Detail:   "Multiple resource references in replace_triggered_by expression.",
				Subject:  expr.Range().Ptr(),
			})
		}
	}
	return ret, diags
}

var AllocatorBlockSchema = &hcl.BodySchema{
	Attributes: []hcl.AttributeSchema{
		{
			Name:     "scope",
			Required: true,
		},
		{
			Name:     "module",
			Required: true,
		},
		{
			Name:     "endpoint",
			Required: true,
		},
		{
			Name:     "resources",
			Required: true,
		},
	},
	Blocks: []hcl.BlockHeaderSchema{},
}
