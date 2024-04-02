package synapse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"

	"github.com/hashicorp/terraform/internal/configs/configschema"
	"github.com/hashicorp/terraform/internal/httpclient"
	"github.com/hashicorp/terraform/internal/lang/marks"
	"github.com/hashicorp/terraform/internal/providers"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/states/statefile"

	"github.com/zclconf/go-cty/cty"
	ctyjson "github.com/zclconf/go-cty/cty/json"
)

var _ providers.Interface = &SynapseProvider{}

// SynapseProvider defines the provider implementation.
type SynapseProvider struct {
	client *ExampleClient
}

// ImportResourceState implements providers.Interface.
func (*SynapseProvider) ImportResourceState(providers.ImportResourceStateRequest) (resp providers.ImportResourceStateResponse) {
	return resp
}

type ClientResponse struct {
	State    json.RawMessage `json:"state"`
	Pointers json.RawMessage `json:"pointers,omitempty"`
}

func (p *SynapseProvider) sendRequest(req ClientRequest) (cty.Value, []cty.PathValueMarks, error) {
	req.ProviderConfig = ProviderConfig{
		BuildDirectory:   p.client.BuildDirectory,
		OutputDirectory:  p.client.OutputDirectory,
		WorkingDirectory: p.client.WorkingDirectory,
	}

	serialized, err := json.Marshal(req)
	if err != nil {
		return cty.NilVal, nil, err
	}

	data, err := p.client.sendRequest("/handle", serialized)
	if err != nil {
		return cty.NilVal, nil, err
	}

	var resp ClientResponse
	err = json.Unmarshal(data, &resp)
	if err != nil {
		return cty.NilVal, nil, err
	}

	// TODO: https://pkg.go.dev/github.com/go-json-experiment/json#UnmarshalerV2
	stateVal := ctyjson.SimpleJSONValue{}
	err = stateVal.UnmarshalJSON(resp.State)
	if err != nil {
		return cty.NilVal, nil, err
	}

	if resp.Pointers != nil {
		val := ctyjson.SimpleJSONValue{}
		err = val.UnmarshalJSON(resp.Pointers)
		if err != nil {
			return cty.NilVal, nil, err
		}

		m := make([]cty.PathValueMarks, 0)
		cty.Walk(val.Value, func(k cty.Path, v cty.Value) (bool, error) {
			if v.Type() == cty.String {
				vm := cty.NewValueMarks(marks.NewPointerAnnotation(v.AsString(), ""))
				m = append(m, cty.PathValueMarks{
					Path:  append([]cty.PathStep{cty.GetAttrPath("output")[0]}, k...),
					Marks: vm,
				})
			}

			return true, nil
		})

		return stateVal.Value, m, nil
	}

	return stateVal.Value, nil, nil
}

func getPointers(m []cty.PathValueMarks) (json.RawMessage, error) {
	pointers := states.ExtractPointers(m)
	encoded, d := statefile.MarshalPointers(pointers)
	if d != nil {
		return nil, d.Err()
	}

	return json.Marshal(encoded)
}

type pointers struct {
	Prior   json.RawMessage `json:"prior,omitempty"`
	Planned json.RawMessage `json:"planned,omitempty"`
}

// ReadDataSource implements providers.Interface.
func (p *SynapseProvider) ReadDataSource(req providers.ReadDataSourceRequest) (resp providers.ReadDataSourceResponse) {
	input := req.Config.GetAttr("input")
	planned, err := ctyjson.SimpleJSONValue{Value: input}.MarshalJSON()
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	pointers := pointers{}
	if req.Marks != nil {
		plannedPointers, err := getPointers(req.Marks)
		if err != nil {
			resp.Diagnostics = resp.Diagnostics.Append(err)
			return resp
		}
		pointers.Planned = plannedPointers
	}

	clientReq := ClientRequest{
		TypeName:     req.Config.GetAttr("type").AsString(),
		ResourceName: req.ResourceName,
		Dependencies: req.Dependencies,
		Operation:    "data",
		PlannedState: planned,
		Pointers:     pointers,
	}

	val, marks, err := p.sendRequest(clientReq)
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	state := req.Config.AsValueMap()
	state["output"] = val
	resp.State = cty.ObjectVal(state)
	resp.Marks = marks

	return resp
}

// ReadResource implements providers.Interface.
func (p *SynapseProvider) ReadResource(req providers.ReadResourceRequest) (resp providers.ReadResourceResponse) {
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

	pointers := pointers{}

	if req.Marks != nil {
		priorPointers, err := getPointers(req.Marks)
		if err != nil {
			resp.Diagnostics = resp.Diagnostics.Append(err)
			return resp
		}
		pointers.Prior = priorPointers
	}

	clientReq := ClientRequest{
		TypeName:     req.PriorState.GetAttr("type").AsString(),
		ResourceName: req.ResourceName,
		Dependencies: req.Dependencies,
		Operation:    "read",
		PriorInput:   priorInput,
		PriorState:   priorOutput,
		Pointers:     pointers,
	}

	val, marks, err := p.sendRequest(clientReq)
	if err != nil {
		resp.Diagnostics = resp.Diagnostics.Append(err)
		return resp
	}

	state := req.PriorState.AsValueMap()
	state["output"] = val
	resp.NewState = cty.ObjectVal(state)
	resp.Marks = marks

	return resp
}

// Stop implements providers.Interface.
func (*SynapseProvider) Stop() error {
	return nil
}

// Close implements providers.Interface.
func (*SynapseProvider) Close() error {
	return nil
}

// UpgradeResourceState implements providers.Interface.
func (p *SynapseProvider) UpgradeResourceState(req providers.UpgradeResourceStateRequest) (resp providers.UpgradeResourceStateResponse) {
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
func (*SynapseProvider) ValidateDataResourceConfig(providers.ValidateDataResourceConfigRequest) (resp providers.ValidateDataResourceConfigResponse) {
	return resp
}

// ValidateProviderConfig implements providers.Interface.
func (*SynapseProvider) ValidateProviderConfig(providers.ValidateProviderConfigRequest) (resp providers.ValidateProviderConfigResponse) {
	return resp
}

// ValidateResourceConfig implements providers.Interface.
func (*SynapseProvider) ValidateResourceConfig(providers.ValidateResourceConfigRequest) (resp providers.ValidateResourceConfigResponse) {
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

func (p *SynapseProvider) GetProviderSchema() providers.GetProviderSchemaResponse {
	return providers.GetProviderSchemaResponse{
		Provider: getProviderSchema(),
		DataSources: map[string]providers.Schema{
			"synapse_resource": getIoSchema(),
		},
		ResourceTypes: map[string]providers.Schema{
			"synapse_resource": getIoSchema(),
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

func (p *SynapseProvider) ConfigureProvider(req providers.ConfigureProviderRequest) (resp providers.ConfigureProviderResponse) {
	config := req.Config
	workingDirectory := getWorkingDirectory(config)
	resolvePath := func(p string) string {
		if path.IsAbs(p) {
			return p
		}
		return path.Join(workingDirectory, p)
	}

	// Can't remember why this client is used instead of the net/http one
	var httpClient = httpclient.NewRetryableClient()
	httpClient.RetryMax = 0
	httpClient.ErrorHandler = maxRetryErrorHandler

	client := ExampleClient{
		HttpClient:       httpClient,
		Endpoint:         getEndpoint(config),
		WorkingDirectory: workingDirectory,
		OutputDirectory:  resolvePath(getStringValue(config, "outputDirectory")),
		BuildDirectory:   resolvePath(getStringValue(config, "buildDirectory")),
	}

	p.client = &client

	return resp
}

func maxRetryErrorHandler(resp *http.Response, err error, numTries int) (*http.Response, error) {
	if resp != nil {
		resp.Body.Close()
	}

	var errMsg string
	if resp != nil {
		var requestId = resp.Header.Get("x-synapse-request-id")
		if requestId != "" {
			errMsg = fmt.Sprintf(": x-synapse-request-id: %s", requestId)
		} else {
			errMsg = fmt.Sprintf(": %s returned from local server", resp.Status)
		}
	} else if err != nil {
		errMsg = fmt.Sprintf(": %s", err)
	}

	if numTries > 1 {
		return resp, fmt.Errorf("request failed after %d attempts%s", numTries, errMsg)
	}
	return resp, fmt.Errorf("request failed%s", errMsg)
}

func getEndpoint(config cty.Value) string {
	if endpoint, exists := os.LookupEnv("TF_SYNAPSE_PROVIDER_ENDPOINT"); exists {
		return endpoint
	}

	return getStringValue(config, "endpoint")
}

func getWorkingDirectory(config cty.Value) string {
	if dir, exists := os.LookupEnv("TF_SYNAPSE_PROVIDER_WORKING_DIRECTORY"); exists {
		return dir
	}

	return getStringValue(config, "workingDirectory")
}

func (p *SynapseProvider) PlanResourceChange(req providers.PlanResourceChangeRequest) (resp providers.PlanResourceChangeResponse) {
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
	Pointers       pointers        `json:"pointers,omitempty"`
	PriorInput     json.RawMessage `json:"priorInput,omitempty"`
	PriorState     json.RawMessage `json:"priorState,omitempty"`
	PlannedState   json.RawMessage `json:"plannedState"`
	ProviderConfig ProviderConfig  `json:"providerConfig"`
}

// ApplyResourceChange takes the planned state for a resource, which may
// yet contain unknown computed values, and applies the changes returning
// the final state.
func (p *SynapseProvider) ApplyResourceChange(req providers.ApplyResourceChangeRequest) (resp providers.ApplyResourceChangeResponse) {
	priorInput := cty.NilVal
	priorOutput := cty.NilVal
	pointers := pointers{}

	if req.PriorMarks != nil {
		priorPointers, err := getPointers(req.PriorMarks)
		if err != nil {
			resp.Diagnostics = resp.Diagnostics.Append(err)
			return resp
		}
		pointers.Prior = priorPointers
	}

	if req.PlannedMarks != nil {
		plannedPointers, err := getPointers(req.PlannedMarks)
		if err != nil {
			resp.Diagnostics = resp.Diagnostics.Append(err)
			return resp
		}
		pointers.Planned = plannedPointers
	}

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
		Pointers:     pointers,
	}

	val, marks, err := p.sendRequest(clientReq)
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
	resp.Marks = marks

	return resp
}

func NewProvider() providers.Interface {
	return &SynapseProvider{}
}
