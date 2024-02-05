// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package jsonprovider

import (
	"encoding/json"

	tfaddr "github.com/hashicorp/terraform-registry-address"
	"github.com/hashicorp/terraform/internal/configs/configschema"
	"github.com/hashicorp/terraform/internal/providers"
)

// FormatVersion represents the version of the json format and will be
// incremented for any change to this format that requires changes to a
// consuming parser.
const FormatVersion = "1.0"

// serializedProviders is the top-level object returned when exporting provider schemas
type serializedProviders struct {
	FormatVersion string               `json:"format_version"`
	Schemas       map[string]*Provider `json:"provider_schemas,omitempty"`
}

type Provider struct {
	Provider          *Schema            `json:"provider,omitempty"`
	ProviderMeta      *Schema            `json:"provider_meta,omitempty"`
	ResourceSchemas   map[string]*Schema `json:"resource_schemas,omitempty"`
	DataSourceSchemas map[string]*Schema `json:"data_source_schemas,omitempty"`
}

type Schemas struct {
	Providers    map[tfaddr.Provider]*providers.Schemas
	Provisioners map[string]*configschema.Block
}

func newProviders() *serializedProviders {
	schemas := make(map[string]*Provider)
	return &serializedProviders{
		FormatVersion: FormatVersion,
		Schemas:       schemas,
	}
}

// MarshalForRenderer converts the provided internation representation of the
// schema into the public structured JSON versions.
//
// This is a format that can be read by the structured plan renderer.
func MarshalForRenderer(s *Schemas) map[string]*Provider {
	schemas := make(map[string]*Provider, len(s.Providers))
	for k, v := range s.Providers {
		schemas[k.String()] = marshalProvider(v)
	}
	return schemas
}

func Marshal(s *Schemas) ([]byte, error) {
	providers := newProviders()
	providers.Schemas = MarshalForRenderer(s)
	ret, err := json.Marshal(providers)
	return ret, err
}

func Unmarshal(data []byte) (*Schemas, error) {
	var schemas map[string]*Provider
	err := json.Unmarshal(data, &schemas)
	ret := Schemas{Providers: map[tfaddr.Provider]*providers.Schemas{}}

	for k, v := range schemas {
		addr, err := tfaddr.ParseProviderSource(k)
		if err != nil {
			return nil, err
		}
		ret.Providers[addr] = unmarshalProvider(v)
	}

	return &ret, err
}

func MarshalProvider(s *providers.Schemas) ([]byte, error) {
	return json.Marshal(marshalProvider(s))
}

func UnmarshalProvider(data []byte) (*providers.Schemas, error) {
	var p Provider
	err := json.Unmarshal(data, &p)
	if err != nil {
		return nil, err
	}

	return unmarshalProvider(&p), nil
}

func marshalProvider(tps *providers.Schemas) *Provider {
	if tps == nil {
		return &Provider{}
	}

	var ps *Schema
	var meta *Schema
	var rs, ds map[string]*Schema

	if tps.Provider != nil {
		ps = marshalSchema(tps.Provider)
	}

	if tps.ProviderMeta != nil {
		meta = marshalSchema(tps.ProviderMeta)
	}

	if tps.ResourceTypes != nil {
		rs = marshalSchemas(tps.ResourceTypes, tps.ResourceTypeSchemaVersions)
	}

	if tps.DataSources != nil {
		ds = marshalSchemas(tps.DataSources, tps.ResourceTypeSchemaVersions)
	}

	return &Provider{
		Provider:          ps,
		ProviderMeta:      meta,
		ResourceSchemas:   rs,
		DataSourceSchemas: ds,
	}
}

func unmarshalProvider(tps *Provider) *providers.Schemas {
	if tps == nil {
		return &providers.Schemas{}
	}

	ret := &providers.Schemas{
		ResourceTypeSchemaVersions: map[string]uint64{},
	}

	if tps.Provider != nil {
		ps, _ := unmarshalSchema(tps.Provider)
		ret.Provider = ps
	}

	if tps.ProviderMeta != nil {
		meta, _ := unmarshalSchema(tps.ProviderMeta)
		ret.ProviderMeta = meta
	}

	if tps.ResourceSchemas != nil {
		rs, rv := unmarshalSchemas(tps.ResourceSchemas)
		ret.ResourceTypes = rs
		for k, v := range rv {
			ret.ResourceTypeSchemaVersions[k] = v
		}
	}

	if tps.DataSourceSchemas != nil {
		ds, dv := unmarshalSchemas(tps.DataSourceSchemas)
		ret.DataSources = ds
		for k, v := range dv {
			ret.ResourceTypeSchemaVersions[k] = v
		}
	}

	return ret
}
