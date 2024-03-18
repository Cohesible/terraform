// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package statefile

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/zclconf/go-cty/cty"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/lang/marks"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

func readStateV5(src []byte) (*File, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics
	sV4 := &stateV5{}
	err := json.Unmarshal(src, sV4)
	if err != nil {
		diags = diags.Append(jsonUnmarshalDiags(err))
		return nil, diags
	}

	file, prepDiags := prepareStateV5(sV4)
	diags = diags.Append(prepDiags)
	return file, diags
}

func prepareStateV5(sV5 *stateV5) (*File, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	file := &File{
		Serial:  sV5.Serial,
		Lineage: sV5.Lineage,
	}

	state := states.NewState()

	for _, rsV5 := range sV5.Resources {
		rAddr := addrs.Resource{
			Type: rsV5.Type,
			Name: rsV5.Name,
			Mode: addrs.ManagedResourceMode,
		}

		moduleAddr := addrs.RootModuleInstance
		providerAddr, addrDiags := addrs.ParseAbsProviderConfigStr(rsV5.ProviderConfig)
		diags.Append(addrDiags)
		if addrDiags.HasErrors() {
			// If ParseAbsProviderConfigStr returns an error, the state may have
			// been written before Provider FQNs were introduced and the
			// AbsProviderConfig string format will need normalization. If so,
			// we treat it like a legacy provider (namespace "-") and let the
			// provider installer handle detecting the FQN.
			var legacyAddrDiags tfdiags.Diagnostics
			providerAddr, legacyAddrDiags = addrs.ParseLegacyAbsProviderConfigStr(rsV5.ProviderConfig)
			if legacyAddrDiags.HasErrors() {
				continue
			}
		}

		ms := state.EnsureModule(moduleAddr)

		// Ensure the resource container object is present in the state.
		ms.SetResourceProvider(rAddr, providerAddr)

		instAddr := rAddr.Instance(addrs.NoKey)

		s := rsV5.State
		obj := &states.ResourceInstanceObjectSrc{
			SchemaVersion:       s.SchemaVersion,
			CreateBeforeDestroy: s.CreateBeforeDestroy,
		}

		obj.AttrsJSON = s.AttributesRaw

		// Paths
		var pvm []cty.PathValueMarks
		addMarks := func(data json.RawMessage, mark interface{}) tfdiags.Diagnostics {
			paths, pathsDiags := unmarshalPaths([]byte(data))
			if pathsDiags.HasErrors() {
				return pathsDiags
			}

			for _, path := range paths {
				pvm = append(pvm, cty.PathValueMarks{
					Path:  path,
					Marks: cty.NewValueMarks(mark),
				})
			}

			return pathsDiags
		}

		if s.AttributeSensitivePaths != nil {
			diags = diags.Append(addMarks(s.AttributeSensitivePaths, marks.Sensitive))
			if diags.HasErrors() {
				continue
			}
		}

		if len(pvm) > 0 {
			obj.AttrMarks = pvm
		}

		// Attributes
		if s.Pointers != nil {
			x, d := unmarshalPointers(s.Pointers)
			diags = diags.Append(d)
			if diags.HasErrors() {
				continue
			}
			obj.Pointers = x
		}

		{
			// Status
			raw := s.Status
			switch raw {
			case "":
				obj.Status = states.ObjectReady
			case "tainted":
				obj.Status = states.ObjectTainted
			default:
				diags = diags.Append(tfdiags.Sourceless(
					tfdiags.Error,
					"Invalid resource instance metadata in state",
					fmt.Sprintf("Instance %s has invalid status %q.", instAddr.Absolute(moduleAddr), raw),
				))
				continue
			}
		}

		if raw := s.PrivateRaw; len(raw) > 0 {
			obj.Private = raw
		}

		{
			depsRaw := s.Dependencies
			deps := make([]addrs.ConfigResource, 0, len(depsRaw))
			for _, depRaw := range depsRaw {
				addr, addrDiags := addrs.ParseAbsResourceStr(depRaw)
				diags = diags.Append(addrDiags)
				if addrDiags.HasErrors() {
					continue
				}
				deps = append(deps, addr.Config())
			}
			obj.Dependencies = deps
		}

		switch {
		case s.Deposed != "":
			dk := states.DeposedKey(s.Deposed)
			if len(dk) != 8 {
				diags = diags.Append(tfdiags.Sourceless(
					tfdiags.Error,
					"Invalid resource instance metadata in state",
					fmt.Sprintf("Instance %s has an object with deposed key %q, which is not correctly formatted.", instAddr.Absolute(moduleAddr), s.Deposed),
				))
				continue
			}
			is := ms.ResourceInstance(instAddr)
			if is.HasDeposed(dk) {
				diags = diags.Append(tfdiags.Sourceless(
					tfdiags.Error,
					"Duplicate resource instance in state",
					fmt.Sprintf("Instance %s deposed object %q appears multiple times in the state file.", instAddr.Absolute(moduleAddr), dk),
				))
				continue
			}

			ms.SetResourceInstanceDeposed(instAddr, dk, obj, providerAddr)
		default:
			is := ms.ResourceInstance(instAddr)
			if is.HasCurrent() {
				diags = diags.Append(tfdiags.Sourceless(
					tfdiags.Error,
					"Duplicate resource instance in state",
					fmt.Sprintf("Instance %s appears multiple times in the state file.", instAddr.Absolute(moduleAddr)),
				))
				continue
			}

			ms.SetResourceInstanceCurrent(instAddr, obj, providerAddr)
		}
		// We repeat this after creating the instances because
		// SetResourceInstanceCurrent automatically resets this metadata based
		// on the incoming objects. That behavior is useful when we're making
		// piecemeal updates to the state during an apply, but when we're
		// reading the state file we want to reflect its contents exactly.
		ms.SetResourceProvider(rAddr, providerAddr)
	}

	file.State = state
	return file, diags
}

func writeStateV5(file *File, w io.Writer) tfdiags.Diagnostics {
	var diags tfdiags.Diagnostics
	if file == nil || file.State == nil {
		panic("attempt to write nil state to file")
	}

	sV5 := &stateV5{
		Serial:    file.Serial,
		Lineage:   file.Lineage,
		Resources: []resourceStateV5{},
	}

	for _, ms := range file.State.Modules {
		for _, rs := range ms.Resources {
			res, encodeDiags := encodeResourceV5(rs)
			diags = append(diags, encodeDiags...)
			if len(res) > 0 {
				sV5.Resources = append(sV5.Resources, res...)
			}
		}
	}

	sV5.normalize()

	src, err := json.MarshalIndent(sV5, "", "  ")
	if err != nil {
		// Shouldn't happen if we do our conversion to *stateV5 correctly above.
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Failed to serialize state",
			fmt.Sprintf("An error occured while serializing the state to save it. This is a bug in Synapse and should be reported: %s.", err),
		))
		return diags
	}
	src = append(src, '\n')

	_, err = w.Write(src)
	if err != nil {
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Failed to write state",
			fmt.Sprintf("An error occured while writing the serialized state: %s.", err),
		))
		return diags
	}

	return diags
}

func encodeResourceV5(rs *states.Resource) ([]resourceStateV5, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics
	resourceAddr := rs.Addr.Resource

	inst, ok := rs.Instances[addrs.NoKey]
	if !ok {
		return nil, nil
	}

	res := make([]resourceStateV5, 0)
	for k, v := range inst.Deposed {
		state, encodeDiags := encodeObjectStateV5(rs, inst, v, k)
		diags = diags.Append(encodeDiags)
		res = append(res, resourceStateV5{
			Type:           resourceAddr.Type,
			Name:           resourceAddr.Name,
			ProviderConfig: rs.ProviderConfig.String(),
			State:          state,
		})
	}

	if inst.HasCurrent() {
		state, encodeDiags := encodeObjectStateV5(rs, inst, inst.Current, "")
		diags = diags.Append(encodeDiags)
		res = append(res, resourceStateV5{
			Type:           resourceAddr.Type,
			Name:           resourceAddr.Name,
			ProviderConfig: rs.ProviderConfig.String(),
			State:          state,
		})
	}

	return res, diags
}

// func EncodeResource(rs *states.Resource) ([]byte, error) {
// 	r, diags := encodeResourceV5(rs)
// 	if diags.HasErrors() {
// 		return nil, diags.Err()
// 	}

// 	return json.Marshal(r)
// }

func encodeObjectStateV5(rs *states.Resource, is *states.ResourceInstance, obj *states.ResourceInstanceObjectSrc, deposed states.DeposedKey) (instanceObjectStateV5, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	var status string
	switch obj.Status {
	case states.ObjectReady:
		status = ""
	case states.ObjectTainted:
		status = "tainted"
	default:
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Failed to serialize resource instance in state",
			fmt.Sprintf("Instance %s has status %s, which cannot be saved in state.", rs.Addr, obj.Status),
		))
	}

	var privateRaw []byte
	if len(obj.Private) > 0 {
		privateRaw = obj.Private
	}

	var deps []string
	for _, depAddr := range obj.Dependencies {
		// Skip adding data sources to the dependencies
		if depAddr.Resource.Mode == addrs.DataResourceMode {
			continue
		}

		deps = append(deps, depAddr.String())
	}

	// Extract sensitivePaths from path value marks
	var sensitivePaths []cty.Path
	for _, vm := range obj.AttrMarks {
		if _, ok := vm.Marks[marks.Sensitive]; ok {
			sensitivePaths = append(sensitivePaths, vm.Path)
		}
	}

	// Marshal paths to JSON
	attributeSensitivePaths, pathsDiags := marshalPaths(sensitivePaths)
	diags = diags.Append(pathsDiags)

	var pointers []PointerAnnotation
	if len(obj.Pointers) > 0 {
		x, d := MarshalPointers(obj.Pointers)
		pointers = x
		diags = diags.Append(d)
	}

	return instanceObjectStateV5{
		Deposed:                 string(deposed),
		Status:                  status,
		SchemaVersion:           obj.SchemaVersion,
		AttributesRaw:           obj.AttrsJSON,
		AttributeSensitivePaths: attributeSensitivePaths,
		PrivateRaw:              privateRaw,
		Dependencies:            deps,
		CreateBeforeDestroy:     obj.CreateBeforeDestroy,
		Pointers:                pointers,
	}, diags
}

type stateV5 struct {
	Version   stateVersionV5    `json:"version"`
	Serial    uint64            `json:"serial"`
	Lineage   string            `json:"lineage"`
	Resources []resourceStateV5 `json:"resources"`

	// TerraformVersion string                   `json:"terraform_version"`
}

// normalize makes some in-place changes to normalize the way items are
// stored to ensure that two functionally-equivalent states will be stored
// identically.
func (s *stateV5) normalize() {
	sort.Stable(sortResourcesV5(s.Resources))
}

type resourceStateV5 struct {
	Name           string                `json:"name"`
	Type           string                `json:"type"`
	ProviderConfig string                `json:"provider"`
	State          instanceObjectStateV5 `json:"state"`
}

type instanceObjectStateV5 struct {
	Status  string `json:"status,omitempty"`
	Deposed string `json:"deposed,omitempty"` // TODO: delete this and add `disposed` to status

	// Provider string                `json:"provider"`
	// AttributesMetadata `json:"attributes_metadata,omitempty"`

	SchemaVersion           uint64          `json:"schema_version"`
	AttributesRaw           json.RawMessage `json:"attributes,omitempty"`
	AttributeSensitivePaths json.RawMessage `json:"sensitive_attributes,omitempty"`

	PrivateRaw []byte `json:"private,omitempty"`

	Dependencies []string `json:"dependencies,omitempty"`

	// Not sure if this really needs to be in the state
	// This flag affects _replacement_
	// But replacement can only happen by an external force
	// That's when this flag should be determined
	//
	// What we're really doing here is deferring deletion until
	// the last possible moment. Deletion can wait up until a
	// dependency is updated/destroyed.
	CreateBeforeDestroy bool `json:"create_before_destroy,omitempty"`

	Pointers []PointerAnnotation `json:"pointer_annotations,omitempty"`
}

type stateVersionV5 struct{}

func (sv stateVersionV5) MarshalJSON() ([]byte, error) {
	return []byte{'5'}, nil
}

func (sv stateVersionV5) UnmarshalJSON([]byte) error {
	return nil
}

type sortResourcesV5 []resourceStateV5

func (sr sortResourcesV5) Len() int      { return len(sr) }
func (sr sortResourcesV5) Swap(i, j int) { sr[i], sr[j] = sr[j], sr[i] }
func (sr sortResourcesV5) Less(i, j int) bool {
	switch {
	case sr[i].Type != sr[j].Type:
		return sr[i].Type < sr[j].Type
	case sr[i].Name != sr[j].Name:
		return sr[i].Name < sr[j].Name
	case sr[i].State.Deposed != sr[j].State.Deposed:
		return sr[i].State.Deposed < sr[j].State.Deposed
	default:
		return false
	}
}
