// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package jsonprovider

import (
	"encoding/json"

	"github.com/hashicorp/terraform/internal/configs/configschema"
	"github.com/zclconf/go-cty/cty"
)

type Attribute struct {
	AttributeType       json.RawMessage `json:"type,omitempty"`
	AttributeNestedType *NestedType     `json:"nested_type,omitempty"`
	Description         string          `json:"description,omitempty"`
	DescriptionKind     string          `json:"description_kind,omitempty"`
	Deprecated          bool            `json:"deprecated,omitempty"`
	Required            bool            `json:"required,omitempty"`
	Optional            bool            `json:"optional,omitempty"`
	Computed            bool            `json:"computed,omitempty"`
	Sensitive           bool            `json:"sensitive,omitempty"`
}

type NestedType struct {
	Attributes  map[string]*Attribute `json:"attributes,omitempty"`
	NestingMode string                `json:"nesting_mode,omitempty"`
}

func marshalStringKind(sk configschema.StringKind) string {
	switch sk {
	default:
		return "plain"
	case configschema.StringMarkdown:
		return "markdown"
	}
}

func unmarshalStringKind(sk string) configschema.StringKind {
	switch sk {
	case "markdown":
		return configschema.StringMarkdown
	}
	return configschema.StringPlain
}

func marshalAttribute(attr *configschema.Attribute) *Attribute {
	ret := &Attribute{
		Description:     attr.Description,
		DescriptionKind: marshalStringKind(attr.DescriptionKind),
		Required:        attr.Required,
		Optional:        attr.Optional,
		Computed:        attr.Computed,
		Sensitive:       attr.Sensitive,
		Deprecated:      attr.Deprecated,
	}

	// we're not concerned about errors because at this point the schema has
	// already been checked and re-checked.
	if attr.Type != cty.NilType {
		attrTy, _ := attr.Type.MarshalJSON()
		ret.AttributeType = attrTy
	}

	if attr.NestedType != nil {
		nestedTy := NestedType{
			NestingMode: nestingModeString(attr.NestedType.Nesting),
		}
		attrs := make(map[string]*Attribute, len(attr.NestedType.Attributes))
		for k, attr := range attr.NestedType.Attributes {
			attrs[k] = marshalAttribute(attr)
		}
		nestedTy.Attributes = attrs
		ret.AttributeNestedType = &nestedTy
	}

	return ret
}

func unmarshalAttribute(attr *Attribute) *configschema.Attribute {
	ret := &configschema.Attribute{
		Description:     attr.Description,
		DescriptionKind: unmarshalStringKind(attr.DescriptionKind),
		Required:        attr.Required,
		Optional:        attr.Optional,
		Computed:        attr.Computed,
		Sensitive:       attr.Sensitive,
		Deprecated:      attr.Deprecated,
	}

	if attr.AttributeType != nil {
		var attrTy cty.Type
		attrTy.UnmarshalJSON(attr.AttributeType) // XXX: check errors!
		ret.Type = attrTy
	}

	if attr.AttributeNestedType != nil {
		nestedTy := configschema.Object{
			Nesting: stringToNestingMode(attr.AttributeNestedType.NestingMode),
		}
		attrs := make(map[string]*configschema.Attribute, len(attr.AttributeNestedType.Attributes))
		for k, attr := range attr.AttributeNestedType.Attributes {
			attrs[k] = unmarshalAttribute(attr)
		}
		nestedTy.Attributes = attrs
		ret.NestedType = &nestedTy
	}

	return ret
}
