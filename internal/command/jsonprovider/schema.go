// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package jsonprovider

import (
	"github.com/hashicorp/terraform/internal/configs/configschema"
)

type Schema struct {
	Version uint64 `json:"version"`
	Block   *Block `json:"block,omitempty"`
}

// marshalSchema is a convenience wrapper around mashalBlock. Schema version
// should be set by the caller.
func marshalSchema(block *configschema.Block) *Schema {
	if block == nil {
		return &Schema{}
	}

	var ret Schema
	ret.Block = marshalBlock(block)

	return &ret
}

func unmarshalSchema(schema *Schema) (*configschema.Block, uint64) {
	if schema.Block == nil {
		return &configschema.Block{}, schema.Version
	}

	return unmarshalBlock(schema.Block), schema.Version
}

func marshalSchemas(blocks map[string]*configschema.Block, rVersions map[string]uint64) map[string]*Schema {
	if blocks == nil {
		return map[string]*Schema{}
	}
	ret := make(map[string]*Schema, len(blocks))
	for k, v := range blocks {
		ret[k] = marshalSchema(v)
		version, ok := rVersions[k]
		if ok {
			ret[k].Version = version
		}
	}
	return ret
}

func unmarshalSchemas(schemas map[string]*Schema) (map[string]*configschema.Block, map[string]uint64) {
	blocks := make(map[string]*configschema.Block, len(schemas))
	rVersions := make(map[string]uint64, len(schemas))
	for k, v := range schemas {
		block, version := unmarshalSchema(v)
		blocks[k] = block
		rVersions[k] = version
	}
	return blocks, rVersions
}
