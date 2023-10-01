package cloudscript

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/hashicorp/terraform/internal/configs/configschema"
	"github.com/hashicorp/terraform/internal/providers"
	"github.com/zclconf/go-cty/cty"
	ctyjson "github.com/zclconf/go-cty/cty/json"
)

// Ensure cloudscriptProvider satisfies various provider interfaces.
var _ providers.Interface = &CloudScriptProvider{}

// CloudScriptProvider defines the provider implementation.
type CloudScriptProvider struct {
	// version is set to the provider version on release, "dev" when the
	// provider is built and ran locally, and "test" when running acceptance
	// testing.
	version string
	client  *ExampleClient
}

// ImportResourceState implements providers.Interface.
func (*CloudScriptProvider) ImportResourceState(providers.ImportResourceStateRequest) (resp providers.ImportResourceStateResponse) {
	return resp
}

func (p *CloudScriptProvider) sendRequest(req ClientRequest) (cty.Value, error) {
	serialized, err := json.Marshal(req)
	if err != nil {
		return cty.NilVal, err
	}

	data, err := p.client.sendRequest("/handle", serialized)
	if err != nil {
		return cty.NilVal, err
	}

	jsonVal := ctyjson.SimpleJSONValue{}
	err = jsonVal.UnmarshalJSON(data)
	if err != nil {
		return cty.NilVal, err
	}

	return jsonVal.Value, nil
}

// ReadDataSource implements providers.Interface.
func (p *CloudScriptProvider) ReadDataSource(req providers.ReadDataSourceRequest) (resp providers.ReadDataSourceResponse) {
	ty := getIoSchema().Block.ImpliedType()
	nullVal := cty.NullVal(ty)

	prior, err := ctyjson.SimpleJSONValue{Value: nullVal}.MarshalJSON()
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	planned, err := ctyjson.SimpleJSONValue{Value: req.Config}.MarshalJSON()
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	clientReq := ClientRequest{
		TypeName:     req.TypeName,
		Operation:    "read",
		PriorState:   prior,
		PlannedState: planned,
	}

	val, err := p.sendRequest(clientReq)
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	state := req.Config.AsValueMap()
	state["output"] = val
	resp.State = cty.ObjectVal(state)

	return resp
}

// ReadResource implements providers.Interface.
func (*CloudScriptProvider) ReadResource(req providers.ReadResourceRequest) (resp providers.ReadResourceResponse) {
	resp.NewState = req.PriorState
	return resp
}

// Stop implements providers.Interface.
func (*CloudScriptProvider) Stop() error {
	return nil
}

// Close implements providers.Interface.
func (*CloudScriptProvider) Close() error {
	return nil
}

// UpgradeResourceState implements providers.Interface.
func (p *CloudScriptProvider) UpgradeResourceState(req providers.UpgradeResourceStateRequest) (resp providers.UpgradeResourceStateResponse) {
	ty := getIoSchema().Block.ImpliedType()
	val, err := ctyjson.Unmarshal(req.RawStateJSON, ty)
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	resp.UpgradedState = val
	return resp
}

// ValidateDataResourceConfig implements providers.Interface.
func (*CloudScriptProvider) ValidateDataResourceConfig(providers.ValidateDataResourceConfigRequest) (resp providers.ValidateDataResourceConfigResponse) {
	return resp
}

// ValidateProviderConfig implements providers.Interface.
func (*CloudScriptProvider) ValidateProviderConfig(providers.ValidateProviderConfigRequest) (resp providers.ValidateProviderConfigResponse) {
	return resp
}

// ValidateResourceConfig implements providers.Interface.
func (*CloudScriptProvider) ValidateResourceConfig(providers.ValidateResourceConfigRequest) (resp providers.ValidateResourceConfigResponse) {
	return resp
}

// cloudscriptProviderModel describes the provider data model.
type cloudscriptProviderModel struct {
	Endpoint         string
	WorkingDirectory string
	OutputDirectory  string
	BuildDirectory   string
}

func getProviderSchema() providers.Schema {
	return providers.Schema{
		Block: &configschema.Block{
			Attributes: map[string]*configschema.Attribute{
				"endpoint":          {Type: cty.String, Required: true},
				"working_directory": {Type: cty.String, Required: true},
				"output_directory":  {Type: cty.String, Required: true},
				"build_directory":   {Type: cty.String, Optional: true},
			},
		},
	}
}

func getResourceInstanceSchema() providers.Schema {
	return providers.Schema{
		Block: &configschema.Block{
			Attributes: map[string]*configschema.Attribute{
				"type":    {Type: cty.String, Required: true},
				"handler": {Type: cty.String, Required: true},
				"state":   {Type: cty.DynamicPseudoType, Computed: true},
				"plan":    {Type: cty.DynamicPseudoType, Required: true},
				"context": {Type: cty.DynamicPseudoType, Required: true},
			},
		},
	}
}

func getAssetSchema() providers.Schema {
	return providers.Schema{
		Block: &configschema.Block{
			Attributes: map[string]*configschema.Attribute{
				"path":      {Type: cty.String, Required: true},
				"type":      {Type: cty.Number, Optional: true},
				"hash":      {Type: cty.DynamicPseudoType, Computed: true},
				"file_path": {Type: cty.DynamicPseudoType, Optional: true, Computed: true},
			},
		},
	}
}

func getClosureSchema() providers.Schema {
	return providers.Schema{
		Block: &configschema.Block{
			Attributes: map[string]*configschema.Attribute{
				"location":    {Type: cty.String, Optional: true, Computed: true},
				"destination": {Type: cty.String, Computed: true},
				"captured":    {Type: cty.DynamicPseudoType, Optional: true},
				"globals":     {Type: cty.DynamicPseudoType, Optional: true},
				"options":     {Type: cty.DynamicPseudoType, Optional: true},
			},
		},
	}
}

func getObjectSchema() providers.Schema {
	return providers.Schema{
		Block: &configschema.Block{
			Attributes: map[string]*configschema.Attribute{
				"input":  {Type: cty.DynamicPseudoType, Required: true},
				"output": {Type: cty.DynamicPseudoType, Computed: true},
			},
		},
	}
}

func getIoSchema() providers.Schema {
	return providers.Schema{
		Version: 0,
		Block: &configschema.Block{
			Attributes: map[string]*configschema.Attribute{
				"input":  {Type: cty.DynamicPseudoType, Required: true},
				"output": {Type: cty.DynamicPseudoType, Computed: true},
			},
		},
	}
}

func (p *CloudScriptProvider) GetProviderSchema() providers.GetProviderSchemaResponse {
	return providers.GetProviderSchemaResponse{
		Provider: getProviderSchema(),
		DataSources: map[string]providers.Schema{
			"cloudscript_resource": getIoSchema(),
		},
		ResourceTypes: map[string]providers.Schema{
			"cloudscript_resource": getIoSchema(),
		},
	}
}

func getIntValue(config cty.Value, attr string) int {
	if config.IsNull() {
		return 0
	}

	if val := config.GetAttr(attr); !val.IsNull() {
		i, _ := val.AsBigFloat().Int64()

		return int(i)
	}

	panic(fmt.Sprintf("expected a string value at attribute %s", attr))
}

func getStringValue(config cty.Value, attr string) string {
	if config.IsNull() {
		return ""
	}

	if val := config.GetAttr(attr); !val.IsNull() {
		return val.AsString()
	}

	panic(fmt.Sprintf("expected a string value at attribute %s", attr))
}

func getJsonObjectValue(config cty.Value, attr string) ctyjson.SimpleJSONValue {
	if config.IsNull() {
		return ctyjson.SimpleJSONValue{}
	}

	if val := config.GetAttr(attr); !val.IsNull() {
		return ctyjson.SimpleJSONValue{Value: val}
	}

	panic(fmt.Sprintf("expected a JSON value at attribute %s", attr))
}

// "/assets"
// "/provider"
// "/provider/data"
// "/closure"

func (p *CloudScriptProvider) ConfigureProvider(req providers.ConfigureProviderRequest) (resp providers.ConfigureProviderResponse) {
	config := req.Config
	client := ExampleClient{
		Endpoint:         getStringValue(config, "endpoint"),
		WorkingDirectory: getStringValue(config, "working_directory"),
		OutputDirectory:  getStringValue(config, "output_directory"),
		BuildDirectory:   getStringValue(config, "build_directory"),
		HttpClient:       http.DefaultClient,
	}

	p.client = &client

	return resp
}

func (p *CloudScriptProvider) PlanResourceChange(req providers.PlanResourceChangeRequest) (resp providers.PlanResourceChangeResponse) {
	planned := req.ProposedNewState.AsValueMap()
	input := req.ProposedNewState.GetAttr("input")

	switch {
	case req.PriorState.IsNull():
		planned["output"] = cty.UnknownVal(cty.DynamicPseudoType)
	case !req.PriorState.GetAttr("input").RawEquals(input):
		planned["output"] = cty.UnknownVal(cty.DynamicPseudoType)
	default:

	}

	resp.PlannedState = cty.ObjectVal(planned)

	return resp
}

type ClientRequest struct {
	TypeName     string          `json:"type"`
	Operation    string          `json:"operation"`
	PriorState   json.RawMessage `json:"priorState"`
	PlannedState json.RawMessage `json:"plannedState"`
}

// ApplyResourceChange takes the planned state for a resource, which may
// yet contain unknown computed values, and applies the changes returning
// the final state.
func (p *CloudScriptProvider) ApplyResourceChange(req providers.ApplyResourceChangeRequest) (resp providers.ApplyResourceChangeResponse) {
	prior, err := ctyjson.SimpleJSONValue{Value: req.PriorState}.MarshalJSON()
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	planned, err := ctyjson.SimpleJSONValue{Value: cty.UnknownAsNull(req.PlannedState)}.MarshalJSON()
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	var op string
	if req.PlannedState.IsNull() {
		op = "destroy"
	} else if req.PriorState.IsNull() {
		op = "create"
	} else {
		op = "update"
	}

	clientReq := ClientRequest{
		TypeName:     req.TypeName,
		Operation:    op,
		PriorState:   prior,
		PlannedState: planned,
	}

	val, err := p.sendRequest(clientReq)
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	newState := req.PlannedState.AsValueMap()
	newState["output"] = val
	resp.NewState = cty.ObjectVal(newState)

	return resp
}

// switch req.TypeName {

// default:
// 	resp.Diagnostics.Append(fmt.Errorf("invalid resource type %s", req.TypeName))
// }
// case "cloudscript_asset":
// 	// r := AssetRequest{
// 	// 	Operation:        op,
// 	// 	WorkingDirectory: p.client.WorkingDirectory,
// 	// 	OutputDirectory:  p.client.OutputDirectory,
// 	// 	BuildDirectory:   p.client.BuildDirectory,
// 	// 	Type:             getIntValue(req.PlannedState, "type"),
// 	// 	Path:             getStringValue(req.PlannedState, "path"),
// 	// 	FilePath:         getStringValue(req.PlannedState, "file_path"),
// 	// 	Hash:             getStringValue(req.PlannedState, "hash"),
// 	// }
// case "cloudscript_closure":
// case "cloudscript_example":

func NewProvider() providers.Interface {
	return &CloudScriptProvider{}
}
