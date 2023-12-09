package cloudscript

import (
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/hashicorp/terraform/internal/configs/configschema"
	"github.com/hashicorp/terraform/internal/httpclient"
	"github.com/hashicorp/terraform/internal/providers"

	"github.com/zclconf/go-cty/cty"
	ctyjson "github.com/zclconf/go-cty/cty/json"
)

// Ensure cloudscriptProvider satisfies various provider interfaces.
var _ providers.Interface = &CloudScriptProvider{}

// CloudScriptProvider defines the provider implementation.
type CloudScriptProvider struct {
	client *ExampleClient
}

// ImportResourceState implements providers.Interface.
func (*CloudScriptProvider) ImportResourceState(providers.ImportResourceStateRequest) (resp providers.ImportResourceStateResponse) {
	return resp
}

func (p *CloudScriptProvider) sendRequest(req ClientRequest) (cty.Value, error) {
	req.ProviderConfig = ProviderConfig{
		BuildDirectory:   p.client.BuildDirectory,
		OutputDirectory:  p.client.OutputDirectory,
		WorkingDirectory: p.client.WorkingDirectory,
	}

	serialized, err := json.Marshal(req)
	if err != nil {
		return cty.NilVal, err
	}

	data, err := p.client.sendRequest("/handle", serialized)
	if err != nil {
		return cty.NilVal, err
	}

	// TODO: https://pkg.go.dev/github.com/go-json-experiment/json#UnmarshalerV2
	jsonVal := ctyjson.SimpleJSONValue{}
	err = jsonVal.UnmarshalJSON(data)
	if err != nil {
		return cty.NilVal, err
	}

	return jsonVal.Value, nil
}

// ReadDataSource implements providers.Interface.
func (p *CloudScriptProvider) ReadDataSource(req providers.ReadDataSourceRequest) (resp providers.ReadDataSourceResponse) {
	input := req.Config.GetAttr("input")
	planned, err := ctyjson.SimpleJSONValue{Value: input}.MarshalJSON()
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	clientReq := ClientRequest{
		TypeName:     req.Config.GetAttr("type").AsString(),
		ResourceName: req.ResourceName,
		Dependencies: req.Dependencies,
		Operation:    "data",
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
func (p *CloudScriptProvider) ReadResource(req providers.ReadResourceRequest) (resp providers.ReadResourceResponse) {
	input := req.PriorState.GetAttr("input")
	priorInput, err := ctyjson.SimpleJSONValue{Value: input}.MarshalJSON()
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	output := req.PriorState.GetAttr("output")
	priorOutput, err := ctyjson.SimpleJSONValue{Value: output}.MarshalJSON()
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	clientReq := ClientRequest{
		TypeName:     req.PriorState.GetAttr("type").AsString(),
		ResourceName: req.ResourceName,
		Dependencies: req.Dependencies,
		Operation:    "read",
		PriorInput:   priorInput,
		PriorState:   priorOutput,
	}

	val, err := p.sendRequest(clientReq)
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	state := req.PriorState.AsValueMap()
	state["output"] = val
	resp.NewState = cty.ObjectVal(state)

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

func getProviderSchema() providers.Schema {
	return providers.Schema{
		Block: &configschema.Block{
			Attributes: map[string]*configschema.Attribute{
				"endpoint":         {Type: cty.String},
				"workingDirectory": {Type: cty.String},
				"outputDirectory":  {Type: cty.String, Required: true},
				"buildDirectory":   {Type: cty.String, Required: true},
			},
		},
	}
}

func getIoSchema() providers.Schema {
	return providers.Schema{
		Version: 0,
		Block: &configschema.Block{
			Attributes: map[string]*configschema.Attribute{
				"type":   {Type: cty.String, Required: true},
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

func getStringValue(config cty.Value, attr string) string {
	if config.IsNull() {
		return ""
	}

	if val := config.GetAttr(attr); !val.IsNull() {
		return val.AsString()
	}

	panic(fmt.Sprintf("expected a string value at attribute %s", attr))
}

// "/assets"
// "/provider"
// "/provider/data"
// "/closure"

func (p *CloudScriptProvider) ConfigureProvider(req providers.ConfigureProviderRequest) (resp providers.ConfigureProviderResponse) {
	config := req.Config
	workingDirectory := getWorkingDirectory(config)
	resolvePath := func(p string) string {
		if path.IsAbs(p) {
			return p
		}
		return path.Join(workingDirectory, p)
	}

	client := ExampleClient{
		HttpClient:       httpclient.NewRetryableClient(),
		Endpoint:         getEndpoint(config),
		WorkingDirectory: workingDirectory,
		OutputDirectory:  resolvePath(getStringValue(config, "outputDirectory")),
		BuildDirectory:   resolvePath(getStringValue(config, "buildDirectory")),
	}

	p.client = &client

	return resp
}

func getEndpoint(config cty.Value) string {
	if endpoint, exists := os.LookupEnv("TF_CLOUDSCRIPT_PROVIDER_ENDPOINT"); exists {
		return endpoint
	}

	return getStringValue(config, "endpoint")
}

func getWorkingDirectory(config cty.Value) string {
	if dir, exists := os.LookupEnv("TF_CLOUDSCRIPT_PROVIDER_WORKING_DIRECTORY"); exists {
		return dir
	}

	return getStringValue(config, "workingDirectory")
}

func (p *CloudScriptProvider) PlanResourceChange(req providers.PlanResourceChangeRequest) (resp providers.PlanResourceChangeResponse) {
	if req.ProposedNewState.IsNull() {
		// destroy op
		resp.PlannedState = req.ProposedNewState
		return resp
	}

	planned := req.ProposedNewState.AsValueMap()
	input := req.ProposedNewState.GetAttr("input")

	switch {
	case req.PriorState.IsNull():
		planned["output"] = cty.UnknownVal(cty.DynamicPseudoType)
	case !req.PriorState.GetAttr("input").RawEquals(input):
		planned["output"] = cty.UnknownVal(cty.DynamicPseudoType)
		// TODO: check if `update` exists in the handler definition. If not then this should be set to `true`
		// resp.RequiresReplace = append(resp.RequiresReplace, cty.GetAttrPath("input"))
	default:

	}

	resp.PlannedState = cty.ObjectVal(planned)

	return resp
}

type ProviderConfig struct {
	WorkingDirectory string `json:"workingDirectory"`
	OutputDirectory  string `json:"outputDirectory"`
	BuildDirectory   string `json:"buildDirectory"`
	// 	programId   string `json:"programId"` // env var ?
	// 	processId   string `json:"processId"` // env var ?
}

type ClientRequest struct {
	TypeName       string          `json:"type"`
	ResourceName   string          `json:"resourceName"`
	Dependencies   []string        `json:"dependencies"`
	Operation      string          `json:"operation"`
	PriorInput     json.RawMessage `json:"priorInput,omitempty"`
	PriorState     json.RawMessage `json:"priorState,omitempty"`
	PlannedState   json.RawMessage `json:"plannedState"`
	ProviderConfig ProviderConfig  `json:"providerConfig"`
}

// ApplyResourceChange takes the planned state for a resource, which may
// yet contain unknown computed values, and applies the changes returning
// the final state.
func (p *CloudScriptProvider) ApplyResourceChange(req providers.ApplyResourceChangeRequest) (resp providers.ApplyResourceChangeResponse) {
	priorInput := cty.NilVal
	priorOutput := cty.NilVal

	if !req.PriorState.IsNull() {
		priorInput = req.PriorState.GetAttr("input")
		priorOutput = req.PriorState.GetAttr("output")
	}

	input, err := ctyjson.SimpleJSONValue{Value: priorInput}.MarshalJSON()
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	output, err := ctyjson.SimpleJSONValue{Value: priorOutput}.MarshalJSON()
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	plannedInput := cty.NilVal
	if !req.PlannedState.IsNull() {
		plannedInput = req.PlannedState.GetAttr("input")
	}

	planned, err := ctyjson.SimpleJSONValue{Value: plannedInput}.MarshalJSON()
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	var op string
	if req.PlannedState.IsNull() {
		op = "delete"
	} else if req.PriorState.IsNull() {
		op = "create"
	} else {
		op = "update"
	}

	var resourceType string
	if req.PlannedState.IsNull() {
		resourceType = req.PriorState.GetAttr("type").AsString()
	} else {
		resourceType = req.PlannedState.GetAttr("type").AsString()
	}

	clientReq := ClientRequest{
		TypeName:     resourceType,
		ResourceName: req.ResourceName,
		Dependencies: req.Dependencies,
		Operation:    op,
		PriorInput:   input,
		PriorState:   output,
		PlannedState: planned,
	}

	val, err := p.sendRequest(clientReq)
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	if op == "delete" {
		resp.NewState = req.PlannedState
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
