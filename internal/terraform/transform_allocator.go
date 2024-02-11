// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package terraform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/hcl/v2"
	tfaddr "github.com/hashicorp/terraform-registry-address"
	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/configs"
	"github.com/hashicorp/terraform/internal/configs/configschema"
	"github.com/hashicorp/terraform/internal/dag"
	"github.com/hashicorp/terraform/internal/httpclient"
	"github.com/hashicorp/terraform/internal/instances"
	"github.com/hashicorp/terraform/internal/lang"
	"github.com/hashicorp/terraform/internal/plans/objchange"
	"github.com/hashicorp/terraform/internal/providers"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/tfdiags"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/gocty"
	ctyjson "github.com/zclconf/go-cty/cty/json"
)

type cachedSchema struct {
	block   *configschema.Block
	version uint64
}

type pendingDestroy struct {
	TransactionId string
	Resource      addrs.AbsResource
}

type Allocator struct {
	ProvidersMeta      map[addrs.Provider]*configs.ProviderMeta
	Config             *configs.Config
	imported           *map[string]ExportedResource
	pendingDestroy     *map[string]pendingDestroy
	providerMetaValues map[addrs.Provider]*cty.Value
	schemas            map[addrs.Resource]cachedSchema
	endpoint           string
	scope              string
	module             string
	resources          []*addrs.Reference
}

func createAllocator(c *configs.Config) (*Allocator, error) {
	configBlock := c.Module.Allocator
	if configBlock == nil {
		allocator := &Allocator{
			Config:             c,
			providerMetaValues: map[tfaddr.Provider]*cty.Value{},
			schemas:            map[addrs.Resource]cachedSchema{},
		}

		return allocator, nil
	}

	allocator := &Allocator{
		Config:             c,
		providerMetaValues: map[tfaddr.Provider]*cty.Value{},
		schemas:            map[addrs.Resource]cachedSchema{},
		endpoint:           configBlock.Endpoint,
		scope:              configBlock.Scope,
		module:             configBlock.Module,
		resources:          configBlock.Resources,
	}

	return allocator, nil
}

func (a *Allocator) ImportState() (ret *map[string]ExportedResource, diags tfdiags.Diagnostics) {
	if a.imported != nil {
		return a.imported, nil
	}

	resources := a.resources
	keys := make([]string, len(resources))
	for i, r := range resources {
		key := r.Subject.String()
		keys[i] = key
	}

	req := allocatorImportRequest{
		Module:    a.module,
		Resources: keys,
	}
	importResp := &allocatorImportResponse{}

	url, d := makeUrl(a.endpoint, a.scope, "import")
	if d.HasErrors() {
		return nil, diags.Append(d)
	}

	d = sendRequest(url, req, importResp)
	if d.HasErrors() {
		return nil, diags.Append(d)
	}

	a.imported = &map[string]ExportedResource{}

	for key, r := range importResp.Resources {
		log.Printf("[INFO] graphNodeAllocator: imported %s", key)
		(*a.imported)[key] = r
	}

	return a.imported, nil
}

func (a *Allocator) GetRefs(ctx EvalContext, addr addrs.Resource) ([]*addrs.Reference, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	rConfig := a.Config.Module.ResourceByAddr(addr)
	if rConfig == nil {
		return nil, diags.Append(fmt.Errorf("missing resource config %s", addr))
	}

	providerAddr := a.Config.ResolveAbsProviderAddr(rConfig.ProviderConfigAddr(), addrs.RootModule)
	schema, _, schemaDiags := a.GetSchema(ctx, addr, providerAddr)
	diags.Append(schemaDiags)
	if diags.HasErrors() {
		return nil, diags
	}

	return lang.ReferencesInBlock(rConfig.Config, schema)
}

func (a *Allocator) GetLocalRefs(ctx EvalContext, addr addrs.LocalValue) ([]*addrs.Reference, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	lConfig := a.Config.Module.Locals[addr.Name]
	if lConfig == nil {
		return nil, diags.Append(fmt.Errorf("missing config %s", addr))
	}

	return lang.ReferencesInExpr(lConfig.Expr)
}

type ResolveResult struct {
	Provider      providers.Interface
	ProviderAddr  addrs.AbsProviderConfig
	Config        cty.Value
	Schema        *configschema.Block
	SchemaVersion uint64
}

type configBlock struct {
	moduleName   string
	attributes   map[string]*hcl.Attribute
	dependencies []string
}

type configVertex struct {
	config  *configs.Resource
	subtype string
	//schema        *configschema.Block
	//schemaVersion uint64
	//providerAddr  addrs.AbsProviderConfig
	block  *configBlock
	isLeaf bool
}

func getBlock(c *configs.Resource) (*configBlock, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	block := &configBlock{}
	attrs, moreDiags := c.Config.JustAttributes()
	diags = diags.Append(moreDiags)
	if diags.HasErrors() {
		return block, diags
	}

	block.attributes = map[string]*hcl.Attribute{}
	block.dependencies = make([]string, 0)
	seen := map[string]bool{}
	for k, v := range attrs {
		block.attributes[k] = v

		refs, refDiags := lang.ReferencesInExpr(v.Expr)
		diags = diags.Append(refDiags)
		if refDiags.HasErrors() {
			continue
		}

		for _, r := range refs {
			var key string
			switch ref := r.Subject.(type) {
			case addrs.Resource:
				key = ref.Instance(addrs.NoKey).String()
			case addrs.ResourceInstance:
				key = ref.Resource.String()
			default:
				diags = diags.Append(fmt.Errorf("Bad ref: %s", ref))
			}

			if key != "" {
				if alreadySeen := seen[key]; !alreadySeen {
					seen[key] = true
					block.dependencies = append(block.dependencies, key)
				}
			}
		}
	}

	block.moduleName = c.ModuleName

	return block, diags
}

func toSlice(val cty.Value) []cty.Value {
	var vals []cty.Value
	it := val.ElementIterator()
	for it.Next() {
		_, v := it.Element()
		vals = append(vals, v)
	}
	return vals
}

func toMap(val cty.Value) map[string]cty.Value {
	vals := map[string]cty.Value{}
	it := val.ElementIterator()
	for it.Next() {
		k, v := it.Element()
		vals[k.AsString()] = v
	}
	return vals
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func min3(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

func levenshteinDistance(a, b string) int {
	if a == b {
		return 0
	} else if a == "" {
		return len(b)
	} else if b == "" {
		return len(a)
	}

	n := len(a)
	m := len(b)
	dists := make([][]int, n)

	for i := 0; i < n; i++ {
		dists[i] = make([]int, m)
		dists[i][0] = i
	}

	for j := 1; j < m; j++ {
		dists[0][j] = j
	}

	for i := 1; i < n; i++ {
		for j := 1; j < m; j++ {
			var c int
			if a[i] != b[j] {
				c = 1
			}
			dists[i][j] = min3(
				dists[i-1][j]+1,
				dists[i][j-1]+1,
				dists[i-1][j-1]+c,
			)
		}
	}

	return dists[n-1][m-1]
}

func ord(v cty.Value) int {
	if v == cty.NilVal || !v.IsKnown() || v.IsNull() {
		return 0
	}

	ty := v.Type()

	switch {
	case ty.IsListType() || ty.IsTupleType() || ty.IsSetType():
		return distSlice(toSlice(v), []cty.Value{})
	case ty.IsMapType() || ty.IsObjectType():
		return distMap(toMap(v), map[string]cty.Value{})
	case ty.IsPrimitiveType():
		switch {
		case ty == cty.String:
			return len(v.AsString())
		case ty == cty.Number:
			var x int
			_ = gocty.FromCtyValue(v, &x)
			return abs(x)
		case ty == cty.Bool:
			return 1
		default:
			// Should never happen, since the above should cover all types
			panic(fmt.Sprintf("omitUnknowns cannot handle %#v", v))
		}
	default:
		// Should never happen, since the above should cover all types
		panic(fmt.Sprintf("omitUnknowns cannot handle %#v", v))
	}
}

func distSlice(a, b []cty.Value) int {
	la := len(a)
	lb := len(b)
	if lb > la {
		return distSlice(b, a)
	}

	var c, i int
	for ; i < lb; i++ {
		c += dist(a[i], b[i])
	}

	for ; i < la; i++ {
		c += ord(a[i])
	}

	return c
}

func distMap(a, b map[string]cty.Value) int {
	var c int

	for k, v := range a {
		if u, exists := b[k]; exists {
			c += dist(v, u)
		} else {
			c += ord(v)
		}
	}

	for k, u := range b {
		if v, exists := a[k]; exists {
			c += dist(v, u)
		} else {
			c += ord(u)
		}
	}

	return c
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func dist(a, b cty.Value) int {
	if a == cty.NilVal || !a.IsKnown() || a.IsNull() {
		return ord(b)
	}

	if b == cty.NilVal || !b.IsKnown() || b.IsNull() {
		return ord(a)
	}

	aty := a.Type()
	bty := b.Type()

	switch {
	case aty.IsListType() || aty.IsTupleType() || aty.IsSetType():
		if !(bty.IsListType() || bty.IsTupleType() || bty.IsSetType()) {
			return ord(a) + ord(b)
		}
		return distSlice(toSlice(a), toSlice(b))
	case aty.IsMapType() || aty.IsObjectType():
		if !(bty.IsMapType() || bty.IsObjectType()) {
			return ord(a) + ord(b)
		}
		return distMap(toMap(a), toMap(b))
	case aty.IsPrimitiveType():
		if aty != bty {
			// fmt.Printf("a:: %s\nb:: %s\n", aty.FriendlyName(), bty.FriendlyName())
			return ord(a) + ord(b)
		}
		switch {
		case aty == cty.String:
			return levenshteinDistance(a.AsString(), b.AsString())
		case aty == cty.Number:
			var x, y int
			_ = gocty.FromCtyValue(a, &x)
			_ = gocty.FromCtyValue(b, &y)
			return abs(x - y)
		case aty == cty.Bool:
			if a.True() == b.True() {
				return 0
			}
			return 1
		default:
			// Should never happen, since the above should cover all types
			panic(fmt.Sprintf("omitUnknowns cannot handle %#v", a))
		}
	default:
		// Should never happen, since the above should cover all types
		panic(fmt.Sprintf("omitUnknowns cannot handle %#v", a))
	}
}

func evalContext(data evalData) (*hcl.EvalContext, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics
	vals := make(map[string]cty.Value)
	funcs := lang.MakeFunctions(".")
	ctx := &hcl.EvalContext{
		Variables: vals,
		Functions: funcs,
	}

	// Managed resources are exposed in two different locations. The primary
	// is at the top level where the resource type name is the root of the
	// traversal, but we also expose them under "resource" as an escaping
	// technique if we add a reserved name in a future language edition which
	// conflicts with someone's existing provider.
	for k, v := range buildResourceObjects(data.resources) {
		vals[k] = v
	}
	//vals["resource"] = cty.ObjectVal(buildResourceObjects(managedResources))

	vals["data"] = cty.ObjectVal(buildResourceObjects(data.data))
	vals["local"] = cty.ObjectVal(data.locals)
	//vals["path"] = cty.ObjectVal(pathAttrs)
	//vals["terraform"] = cty.ObjectVal(terraformAttrs)

	return ctx, diags
}

func buildResourceObjects(resources map[string]map[string]cty.Value) map[string]cty.Value {
	vals := make(map[string]cty.Value)
	for typeName, nameVals := range resources {
		vals[typeName] = cty.ObjectVal(nameVals)
	}
	return vals
}

func applyMoves(vals map[string]cty.Value, moves map[string]string) map[string]cty.Value {
	if len(moves) == 0 {
		return map[string]cty.Value{}
	}

	// applied := map[string]bool{}
	result := make(map[string]cty.Value)
	for k, v := range vals {
		if alt, exists := moves[k]; exists {
			result[alt] = v
			// applied[alt] = true
		}
	}

	return result
}

func buildResourceObjectsWithMoves(resources map[string]map[string]cty.Value, moves map[string]map[string]string) map[string]cty.Value {
	vals := make(map[string]cty.Value)
	for typeName, nameVals := range resources {
		if m, exists := moves[typeName]; exists {
			//fmt.Printf("applied moves %s; len: %v", typeName, len(m))
			vals[typeName] = cty.ObjectVal(applyMoves(nameVals, m))
		} else {
			// vals[typeName] = cty.ObjectVal(nameVals)
		}
	}
	return vals
}

func evaluateBlock(ctx *hcl.EvalContext, block *configBlock) (cty.Value, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics
	result := map[string]cty.Value{}
	for k, v := range block.attributes {
		val, evalDiags := v.Expr.Value(ctx)
		diags = diags.Append(evalDiags)
		result[k] = val
	}
	result["module_name"] = cty.StringVal(block.moduleName)
	return cty.ObjectVal(result), diags
}

func buildGraph(c *configs.Config) (*Graph, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	g := &Graph{Path: addrs.RootModuleInstance}
	vertices := map[string]*configVertex{}

	for k, v := range c.Module.ManagedResources {
		// providerAddr := c.ResolveAbsProviderAddr(v.ProviderConfigAddr(), addrs.RootModule)
		// schema, schemaVersion, schemaDiags := a.GetSchema(ctx, v.Addr(), providerAddr)
		// diags = diags.Append(schemaDiags)
		block, blockDiags := getBlock(v)
		diags = diags.Append(blockDiags)

		vert := &configVertex{
			config: v,
			// schema:        schema,
			// schemaVersion: schemaVersion,
			// providerAddr:  providerAddr,
			block:  block,
			isLeaf: true,
		}

		if v.Type == "synapse_resource" {
			subtype, _ := configs.DecodeAsString(block.attributes["type"])
			vert.subtype = subtype
		}

		vertices[k] = vert
		g.Add(vert)
	}

	for _, v := range vertices {
		for _, d := range v.block.dependencies {
			if u, exists := vertices[d]; exists {
				u.isLeaf = false
				g.Connect(dag.BasicEdge(u, v))
			}
		}
	}

	return g, diags
}

func getVertices(g *Graph) []*configVertex {
	n := g.TopologicalOrder()
	a := make([]*configVertex, len(n))
	for i, x := range n {
		a[i] = x.(*configVertex)
	}
	return a
}

type resourceAddress struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

type moveOp struct {
	From resourceAddress `json:"from"`
	To   resourceAddress `json:"to"`
}

type searchNode struct {
	key         string
	state       *evalData
	score       int
	previous    *searchNode
	currentMove []int
	pos         int
}

type evalData struct {
	resources map[string]map[string]cty.Value
	data      map[string]map[string]cty.Value
	locals    map[string]cty.Value
}

func buildMoves(moves []int, v1, v2 []*configVertex) map[string]map[string]string {
	result := map[string]map[string]string{}
	for k := 0; k < len(moves); k += 2 {
		u := v1[moves[k]]
		v := v2[moves[k+1]]
		if u.config.Name != v.config.Name {
			m := result[u.config.Type]
			if m == nil {
				m = map[string]string{}
				result[u.config.Type] = m
			}
			m[u.config.Name] = v.config.Name
		}
	}
	return result
}

func createSearchContext(node *searchNode, v1, v2 []*configVertex) *hcl.EvalContext {
	vals := make(map[string]cty.Value)
	funcs := lang.MakeFunctions(".")
	ctx := &hcl.EvalContext{
		Variables: vals,
		Functions: funcs,
	}

	moves := buildMoves(getMoves(node), v1, v2)

	for k, v := range buildResourceObjectsWithMoves(node.state.resources, moves) {
		vals[k] = v
	}

	vals["data"] = cty.ObjectVal(buildResourceObjects(node.state.data))
	vals["local"] = cty.ObjectVal(node.state.locals)

	return ctx
}

func getMoves(n *searchNode) []int {
	moves := make([]int, 0)
	if n.currentMove != nil && n.currentMove[0] != -1 {
		moves = append(moves, n.currentMove...)
	}
	p := n.previous
	for p != nil {
		if p.currentMove != nil && p.currentMove[0] != -1 {
			moves = append(moves, p.currentMove...)
		}
		p = p.previous
	}
	return moves
}

func buildNodeKey(n *searchNode) string {
	moves := getMoves(n)
	parts := make([]string, 0)
	pq := PriorityQueue[string]{}
	for k := 0; k < len(moves); k += 2 {
		part := (moves[k] << 16) | moves[k+1]
		pq.Insert(strconv.Itoa(part), moves[k+1])
	}
	for len(pq) > 0 {
		part, _ := pq.Extract()
		parts = append(parts, part)
	}
	return strings.Join(parts, ":")
}

func getNodeKey(n *searchNode) string {
	if n.key == "" && len(n.currentMove) > 0 {
		n.key = buildNodeKey(n)
	}
	return n.key
}

type MovesResult struct {
	Moves []moveOp `json:"moves"`
	Score int      `json:"score"`
}

// TODO: impl. zhang-shasha TED algorithm over source maps
// This should be much faster than path finding over pairs

func FindMoves(s *states.State, c1, c2 *configs.Config) MovesResult {
	g1, _ := buildGraph(c1)
	g2, _ := buildGraph(c2)

	myData := evalData{
		locals:    map[string]cty.Value{},
		resources: map[string]map[string]cty.Value{},
		data:      map[string]map[string]cty.Value{},
	}
	ms := s.Module(addrs.RootModuleInstance)

	for _, v := range ms.Resources {
		inst := v.Instance(addrs.NoKey)
		if inst == nil || !inst.HasCurrent() {
			fmt.Printf("missing resource: %s\n", v.Addr)
			continue
		}

		d := ctyjson.SimpleJSONValue{}
		err := d.UnmarshalJSON(inst.Current.AttrsJSON)
		if err != nil {
			fmt.Printf("bad parse: %s: %s\n", v.Addr, err)
			continue
		}

		var t map[string]map[string]cty.Value
		if v.Addr.Resource.Mode == addrs.DataResourceMode {
			t = myData.data
		} else {
			t = myData.resources
		}
		x, exists := t[v.Addr.Resource.Type]
		if !exists {
			x = map[string]cty.Value{}
			t[v.Addr.Resource.Type] = x
		}
		x[v.Addr.Resource.Name] = d.Value
	}

	for k, v := range ms.LocalValues {
		myData.locals[k] = v
	}

	ctx, _ := evalContext(myData)

	v1 := getVertices(g1)
	v2 := getVertices(g2)

	baseline := make([]cty.Value, len(v1))
	for i, v := range v1 {
		x, _ := evaluateBlock(ctx, v.block)
		baseline[i] = x
		// b, _ := x.Type().MarshalJSON()
		// fmt.Printf("evaluated %s: %s\n", v.config.Addr(), b)
	}

	pairs := make([][]int, len(v2))
	for j, u := range v2 {
		pairs[j] = make([]int, 0)
		for i, v := range v1 {
			if u.config.Type == v.config.Type && u.subtype == v.subtype {
				pairs[j] = append(pairs[j], i)
			}
		}
	}

	// Sparse 3d matrix
	dists := map[string]map[int]map[int]int{}
	getDist := func(n *searchNode, i, j int) int {
		k := getNodeKey(n)
		m := dists[k]
		if m == nil {
			m = map[int]map[int]int{}
			dists[k] = m
		}

		if m2, exists := m[i]; exists {
			if d, exists := m2[j]; exists {
				return d
			}
		}

		return -1
	}

	setDist := func(n *searchNode, i, j, d int) {
		k := getNodeKey(n)
		m := dists[k]
		if m == nil {
			m = map[int]map[int]int{}
			dists[k] = m
		}
		m2 := m[i]
		if m2 == nil {
			m2 = map[int]int{}
			m[i] = m2
		}
		m2[j] = d
	}

	contexts := map[string]*hcl.EvalContext{}
	getContext := func(n *searchNode) *hcl.EvalContext {
		k := getNodeKey(n)
		ctx := contexts[k]
		if ctx == nil {
			ctx = createSearchContext(n, v1, v2)
			contexts[k] = ctx
		}
		return ctx
	}

	configs := map[string]map[int]cty.Value{}
	getConfig := func(n *searchNode, j int) cty.Value {
		k := getNodeKey(n)
		m := configs[k]
		if m == nil {
			m = map[int]cty.Value{}
			configs[k] = m
		}
		x, exists := m[j]
		if !exists {
			ctx := getContext(n)
			x, _ = evaluateBlock(ctx, v2[j].block)
			m[j] = x
		}
		return x
	}

	partials := map[string]map[int]map[int]int{}
	getPartialDist := func(n *searchNode, i, j int) int {
		k := getNodeKey(n)
		m := partials[k]
		if m == nil {
			m = map[int]map[int]int{}
			partials[k] = m
		}
		m2 := m[i]
		if m2 == nil {
			m2 = map[int]int{}
			m[i] = m2
		}
		d, exists := m2[j]
		if !exists {
			x := getConfig(n, j)
			if i == -1 {
				d = ord(x)
			} else {
				v := baseline[i]
				d = dist(v, x)
			}
			m2[j] = d
		}
		return d
	}

	bestcaseDists := make([][]int, len(v1))
	for i := range v1 {
		bestcaseDists[i] = make([]int, len(v2))
		for j, v := range v2 {
			d := 0
			b := baseline[i]
			block := v.block
			b2 := b.AsValueMap()
			unknowns := map[string]bool{}

			for k, v := range block.attributes {
				refs, _ := lang.ReferencesInExpr(v.Expr)
				if len(refs) > 0 {
					unknowns[k] = true
				}
			}
			seen := map[string]bool{}
			for k, v := range block.attributes {
				seen[k] = true
				if !unknowns[k] {
					val, _ := v.Expr.Value(ctx)
					d += dist(b2[k], val)
				}
			}
			for k, v := range b2 {
				if !seen[k] && k != "module_name" {
					d += ord(v)
				}
			}

			d += levenshteinDistance(v1[i].block.moduleName, v2[j].block.moduleName)
			bestcaseDists[i][j] = d
		}
	}

	// getBestCaseDistCache := map[string]map[int]map[int]map[string]int{}
	// getBestCaseDist := func(n *searchNode, i, j int) int {
	// 	moves := map[string]bool{}
	// 	for k := 0; k < len(n.moves); k += 2 {
	// 		v := v2[n.moves[k+1]]
	// 		moves[v.config.Addr().String()] = true
	// 	}

	// 	b := baseline[i]
	// 	block := v2[j].block

	// 	unknowns := map[string]bool{}

	// 	for k, v := range block.attributes {
	// 		refs, _ := lang.ReferencesInExpr(v.Expr)

	// 		for _, r := range refs {
	// 			var key string
	// 			switch ref := r.Subject.(type) {
	// 			case addrs.Resource:
	// 				key = ref.Instance(addrs.NoKey).String()
	// 			case addrs.ResourceInstance:
	// 				key = ref.Resource.String()
	// 			}

	// 			if !moves[key] {
	// 				unknowns[k] = true
	// 				break
	// 			}
	// 		}
	// 	}

	// 	d := 0
	// 	b2 := b.AsValueMap()
	// 	ctx := getContext(n)
	// 	nk := getNodeKey(n.previous)

	// 	var cache map[string]int
	// 	if m, ok := getBestCaseDistCache[nk]; ok {
	// 		if mi, ok := m[i]; ok {
	// 			if mj, ok := mi[j]; ok {
	// 				cache = mj
	// 			}
	// 		}
	// 	}

	// 	k := getNodeKey(n)
	// 	m := getBestCaseDistCache[k]
	// 	if m == nil {
	// 		m = map[int]map[int]map[string]int{}
	// 		getBestCaseDistCache[k] = m
	// 	}
	// 	mi := m[i]
	// 	if mi == nil {
	// 		mi = map[int]map[string]int{}
	// 		m[i] = mi
	// 	}
	// 	mj := mi[j]
	// 	if mj == nil {
	// 		mj = map[string]int{}
	// 		mi[j] = mj
	// 	}
	// 	myCache := mj

	// 	seen := map[string]bool{}
	// 	for k, v := range block.attributes {
	// 		seen[k] = true
	// 		if !unknowns[k] {
	// 			if cache != nil {
	// 				if cd, ok := cache[k]; ok {
	// 					d += cd
	// 					myCache[k] = cd
	// 					continue
	// 				}
	// 			}
	// 			val, _ := v.Expr.Value(ctx)
	// 			cd := dist(b2[k], val)
	// 			myCache[k] = cd
	// 			d += cd
	// 		}
	// 	}
	// 	// for k, v := range b2 {
	// 	// 	if !seen[k] && k != "module_name" {
	// 	// 		d += ord(v)
	// 	// 	}
	// 	// }

	// 	// d += levenshteinDistance(v1[i].block.moduleName, v2[j].block.moduleName)

	// 	return d
	// }

	bases := make([]int, len(v1))
	id := 0
	for i, x := range baseline {
		bases[i] = ord(x)
		id += bases[i]
	}

	// getBestCaseDistAll := func(n *searchNode) int {
	// 	pq := PriorityQueue[[]int]{}

	// 	for _, j := range n.openNew {
	// 		for _, i := range pairs[j] {
	// 			if n.closedOld[i] {
	// 				continue
	// 			}

	// 			// d := getBestCaseDist(n, i, j)
	// 			d := bestcaseDists[i][j]
	// 			pq.Insert([]int{i, j}, d)
	// 		}
	// 	}

	// 	td := 0
	// 	seen1 := map[int]bool{}
	// 	seen2 := map[int]bool{}
	// 	for len(pq) > 0 {
	// 		x, d := pq.Extract()
	// 		if seen1[x[0]] || seen2[x[1]] {
	// 			continue
	// 		}
	// 		seen1[x[0]] = true
	// 		seen2[x[1]] = true
	// 		td += d
	// 	}
	// 	for _, j := range n.openNew {
	// 		if !seen2[j] {
	// 			td += getPartialDist(n, -1, j)
	// 		}
	// 	}
	// 	return td
	// }

	// names := map[string]bool{}
	// for _, v := range v2 {
	// 	names[v.config.Addr().String()] = true
	// }

	// isDepsSatisfied := func(n *searchNode, j int) bool {
	// 	moves := map[string]bool{}
	// 	for k := 0; k < len(n.moves); k += 2 {
	// 		v := v2[n.moves[k+1]]
	// 		moves[v.config.Addr().String()] = true
	// 	}
	// 	v := v2[j]
	// 	for _, d := range v.block.dependencies {
	// 		if names[d] && !moves[d] {
	// 			return false
	// 		}
	// 	}
	// 	return true
	// }

	enumeratePairs := func(n *searchNode) []int {
		p := make([]int, 0)
		closedOld := map[int]bool{}
		moves := getMoves(n)
		closedNew := map[int]bool{}
		for k := 0; k < len(moves); k += 2 {
			i := moves[k]
			j := moves[k+1]
			closedOld[i] = true
			closedNew[j] = true
		}

		for j := range v2 {
			if closedNew[j] {
				continue
			}
			for _, i := range pairs[j] {
				if closedOld[i] {
					continue
				}

				d := getPartialDist(n, i, j)
				ln, rn := v1[i].config.Name, v2[j].config.Name
				log.Printf("[TRACE] distance [%v]: %s, %s, %v\n", j, ln, rn, d)

				// Perfect match
				if d == 0 && ln == rn {
					return []int{i, j, d}
				}

				p = append(p, i, j, d)
			}

			d := getPartialDist(n, -1, j)
			p = append(p, -1, j, d) // Empty pair
			log.Printf("[TRACE] distance [%v]: [none] %s, %v\n", j, v2[j].config.Name, d)
		}

		return p
	}

	pq := PriorityQueue[searchNode]{}
	node := searchNode{
		pos:         0,
		state:       &myData,
		score:       id,
		currentMove: nil,
	}

	var tt int64
	var tt2 int64
	var lc int
	pq.Insert(node, id)
	for len(pq) > 0 {
		lc += 1
		n, _ := pq.Extract()
		s := n.score
		if n.pos == len(v2) {
			result := MovesResult{Score: s, Moves: make([]moveOp, 0)}
			moves := getMoves(&n)
			for k := 0; k < len(moves); k += 2 {
				u := v1[moves[k]]
				v := v2[moves[k+1]]
				if u.config.Name != v.config.Name {
					result.Moves = append(result.Moves, moveOp{
						From: resourceAddress{Type: u.config.Type, Name: u.config.Name},
						To:   resourceAddress{Type: v.config.Type, Name: v.config.Name},
					})
				}
			}

			return result
		}

		if lc%10 == 0 {
			log.Printf("[INFO] <sample> remaining: %v; pq size: %v; score: %v; loops: %v; enumeration: %vns; copy %vns", len(v2)-n.pos, len(pq), n.score, lc, tt2, tt)
			tt2 = 0
			tt = 0
		}

		st2 := time.Now()
		p := enumeratePairs(&n)
		tt2 += time.Now().UnixNano() - st2.UnixNano()

		for k := 0; k < len(p); k += 3 {
			i, j, c := p[k], p[k+1], p[k+2]

			var y int
			if i != -1 {
				y = bases[i]
			}
			ns := s + c - y
			// fmt.Printf("loop: %v, %v, %v\n", n.pos, i, c)
			st := time.Now()
			nn := searchNode{
				pos:         n.pos + 1,
				state:       n.state,
				score:       ns,
				previous:    &n,
				currentMove: []int{i, j},
			}

			vs := getDist(&nn, i, j)
			tt += time.Now().UnixNano() - st.UnixNano()

			if vs != -1 && nn.score >= vs {
				log.Printf("[DEBUG] skipped higher cost node %s", nn.key)
				continue
			}

			//h := getBestCaseDistAll(&nn)

			setDist(&nn, i, j, nn.score)
			pq.Insert(nn, nn.score)
		}
	}

	return MovesResult{}
}

func (a *Allocator) Resolve(ctx EvalContext, c *configs.Resource) (*ResolveResult, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	providerAddr := a.Config.ResolveAbsProviderAddr(c.ProviderConfigAddr(), addrs.RootModule)
	p, err := a.getProvider(ctx, providerAddr)
	if err != nil {
		return nil, diags.Append(err)
	}

	addr := c.Addr()
	schema, version, schemaDiags := a.GetSchema(ctx, addr, providerAddr)
	diags.Append(schemaDiags)
	if diags.HasErrors() {
		return nil, diags
	}

	log.Printf("[INFO] Starting apply for %s", addr)

	configVal, _, configDiags := ctx.EvaluateBlock(c.Config, schema, nil, instances.RepetitionData{})
	diags = diags.Append(configDiags)
	if configDiags.HasErrors() {
		return nil, diags
	}

	return &ResolveResult{
		Provider:      p,
		Config:        configVal,
		Schema:        schema,
		SchemaVersion: version,
		ProviderAddr:  providerAddr,
	}, nil
}

type ApplyResult struct {
	State         *states.ResourceInstanceObject
	Schema        *configschema.Block
	SchemaVersion uint64
}

func (a *Allocator) Apply(ctx EvalContext, c *configs.Resource) (*ApplyResult, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	addr := c.Addr()
	log.Printf("[INFO] Starting apply for %s", addr)

	resolved, resolveDiags := a.Resolve(ctx, c)
	if resolveDiags.HasErrors() {
		return nil, diags
	}

	p := resolved.Provider
	configVal := resolved.Config
	schema := resolved.Schema
	version := resolved.SchemaVersion
	providerAddr := resolved.ProviderAddr

	if !configVal.IsWhollyKnown() {
		// We don't have a pretty format function for a path, but since this is
		// such a rare error, we can just drop the raw GoString values in here
		// to make sure we have something to debug with.
		var unknownPaths []string
		cty.Transform(configVal, func(p cty.Path, v cty.Value) (cty.Value, error) {
			if !v.IsKnown() {
				unknownPaths = append(unknownPaths, fmt.Sprintf("%#v", p))
			}
			return v, nil
		})

		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Configuration contains unknown value",
			fmt.Sprintf("configuration for %s still contains unknown values during apply (this is a bug in Terraform; please report it!)\n"+
				"The following paths in the resource configuration are unknown:\n%s",
				c,
				strings.Join(unknownPaths, "\n"),
			),
		))
		return nil, diags
	}

	// unmarkedConfigVal, _ := configVal.UnmarkDeep()
	ty := schema.ImpliedType()
	nullVal := cty.NullVal(ty)
	state := &states.ResourceInstanceObject{Value: nullVal}
	ris := ctx.State().Module(addrs.RootModuleInstance).ResourceInstance(addr.Instance(addrs.NoKey))
	if ris != nil && ris.HasCurrent() {
		decoded, err := ris.Current.Decode(ty)
		if err != nil {
			return nil, diags.Append(err)
		}
		state = decoded
	}

	proposedNewVal := objchange.ProposedNew(schema, state.Value, configVal)

	metaConfigVal, metaDiags := a.GetMeta(ctx, addr, providerAddr)
	diags = diags.Append(metaDiags)
	if diags.HasErrors() {
		return nil, diags
	}

	d := a.configureProvider(ctx, providerAddr)
	if d.HasErrors() {
		return nil, d
	}

	planResp := p.PlanResourceChange(providers.PlanResourceChangeRequest{
		TypeName:         addr.Type,
		Config:           configVal,
		PriorState:       state.Value,
		ProposedNewState: proposedNewVal,
		PriorPrivate:     state.Private,
		ProviderMeta:     *metaConfigVal,
	})

	diags = diags.Append(planResp.Diagnostics)
	if diags.HasErrors() {
		return nil, diags
	}

	afterState := planResp.PlannedState

	log.Printf("[INFO] %s: applying configuration", addr)

	// If our config, Before or After value contain any marked values,
	// ensure those are stripped out before sending
	// this to the provider
	// unmarkedConfigVal, _ := configVal.UnmarkDeep()
	// unmarkedBefore, beforePaths := change.Before.UnmarkDeepWithPaths()
	// unmarkedAfter, afterPaths := change.After.UnmarkDeepWithPaths()

	eqV := state.Value.Equals(planResp.PlannedState)
	eq := eqV.IsKnown() && eqV.True()
	if eq {
		// Copy the previous state, changing only the value
		newState := &states.ResourceInstanceObject{
			CreateBeforeDestroy: state.CreateBeforeDestroy,
			Dependencies:        state.Dependencies,
			Private:             state.Private,
			Status:              state.Status,
			Value:               afterState,
		}
		return &ApplyResult{State: newState, Schema: schema}, diags
	}

	resp := p.ApplyResourceChange(providers.ApplyResourceChangeRequest{
		TypeName:       addr.Type,
		PriorState:     state.Value,
		Config:         configVal,
		PlannedState:   planResp.PlannedState,
		PlannedPrivate: planResp.PlannedPrivate,
		ProviderMeta:   *metaConfigVal,
	})
	applyDiags := resp.Diagnostics
	applyDiags = applyDiags.InConfigBody(c.Config, addr.String())
	diags = diags.Append(applyDiags)

	// Even if there are errors in the returned diagnostics, the provider may
	// have returned a _partial_ state for an object that already exists but
	// failed to fully configure, and so the remaining code must always run
	// to completion but must be defensive against the new value being
	// incomplete.
	newVal := resp.NewState

	if newVal == cty.NilVal {
		// Providers are supposed to return a partial new value even when errors
		// occur, but sometimes they don't and so in that case we'll patch that up
		// by just using the prior state, so we'll at least keep track of the
		// object for the user to retry.
		newVal = state.Value

		// As a special case, we'll set the new value to null if it looks like
		// we were trying to execute a delete, because the provider in this case
		// probably left the newVal unset intending it to be interpreted as "null".
		if planResp.PlannedState.IsNull() {
			newVal = cty.NullVal(schema.ImpliedType())
		}

		if !diags.HasErrors() {
			diags = diags.Append(tfdiags.Sourceless(
				tfdiags.Error,
				"Provider produced invalid object",
				fmt.Sprintf(
					"Provider %q produced an invalid nil value after apply for %s.\n\nThis is a bug in the provider, which should be reported in the provider's own issue tracker.",
					providerAddr.String(), addr.String(),
				),
			))
		}
	}

	var conformDiags tfdiags.Diagnostics
	for _, err := range newVal.Type().TestConformance(schema.ImpliedType()) {
		conformDiags = conformDiags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Provider produced invalid object",
			fmt.Sprintf(
				"Provider %q produced an invalid value after apply for %s. The result cannot not be saved in the Terraform state.\n\nThis is a bug in the provider, which should be reported in the provider's own issue tracker.",
				providerAddr.String(), tfdiags.FormatErrorPrefixed(err, addr.String()),
			),
		))
	}
	diags = diags.Append(conformDiags)
	if conformDiags.HasErrors() {
		// Bail early in this particular case, because an object that doesn't
		// conform to the schema can't be saved in the state anyway -- the
		// serializer will reject it.
		return nil, diags
	}

	// After this point we have a type-conforming result object and so we
	// must always run to completion to ensure it can be saved. If n.Error
	// is set then we must not return a non-nil error, in order to allow
	// evaluation to continue to a later point where our state object will
	// be saved.

	// By this point there must not be any unknown values remaining in our
	// object, because we've applied the change and we can't save unknowns
	// in our persistent state. If any are present then we will indicate an
	// error (which is always a bug in the provider) but we will also replace
	// them with nulls so that we can successfully save the portions of the
	// returned value that are known.
	if !newVal.IsWhollyKnown() {
		// To generate better error messages, we'll go for a walk through the
		// value and make a separate diagnostic for each unknown value we
		// find.
		cty.Walk(newVal, func(path cty.Path, val cty.Value) (bool, error) {
			if !val.IsKnown() {
				pathStr := tfdiags.FormatCtyPath(path)
				diags = diags.Append(tfdiags.Sourceless(
					tfdiags.Error,
					"Provider returned invalid result object after apply",
					fmt.Sprintf(
						"After the apply operation, the provider still indicated an unknown value for %s%s. All values must be known after apply, so this is always a bug in the provider and should be reported in the provider's own repository. Terraform will still save the other known object values in the state.",
						addr, pathStr,
					),
				))
			}
			return true, nil
		})

		// NOTE: This operation can potentially be lossy if there are multiple
		// elements in a set that differ only by unknown values: after
		// replacing with null these will be merged together into a single set
		// element. Since we can only get here in the presence of a provider
		// bug, we accept this because storing a result here is always a
		// best-effort sort of thing.
		newVal = cty.UnknownAsNull(newVal)
	}

	if !diags.HasErrors() {
		// Only values that were marked as unknown in the planned value are allowed
		// to change during the apply operation. (We do this after the unknown-ness
		// check above so that we also catch anything that became unknown after
		// being known during plan.)
		//
		// If we are returning other errors anyway then we'll give this
		// a pass since the other errors are usually the explanation for
		// this one and so it's more helpful to let the user focus on the
		// root cause rather than distract with this extra problem.
		if errs := objchange.AssertObjectCompatible(schema, planResp.PlannedState, newVal); len(errs) > 0 {
			if resp.LegacyTypeSystem {
				// The shimming of the old type system in the legacy SDK is not precise
				// enough to pass this consistency check, so we'll give it a pass here,
				// but we will generate a warning about it so that we are more likely
				// to notice in the logs if an inconsistency beyond the type system
				// leads to a downstream provider failure.
				var buf strings.Builder
				fmt.Fprintf(&buf, "[WARN] Provider %q produced an unexpected new value for %s, but we are tolerating it because it is using the legacy plugin SDK.\n    The following problems may be the cause of any confusing errors from downstream operations:", providerAddr.String(), addr)
				for _, err := range errs {
					fmt.Fprintf(&buf, "\n      - %s", tfdiags.FormatError(err))
				}
				log.Print(buf.String())

				// The sort of inconsistency we won't catch here is if a known value
				// in the plan is changed during apply. That can cause downstream
				// problems because a dependent resource would make its own plan based
				// on the planned value, and thus get a different result during the
				// apply phase. This will usually lead to a "Provider produced invalid plan"
				// error that incorrectly blames the downstream resource for the change.

			} else {
				for _, err := range errs {
					diags = diags.Append(tfdiags.Sourceless(
						tfdiags.Error,
						"Provider produced inconsistent result after apply",
						fmt.Sprintf(
							"When applying changes to %s, provider %q produced an unexpected new value: %s.\n\nThis is a bug in the provider, which should be reported in the provider's own issue tracker.",
							addr, providerAddr, tfdiags.FormatError(err),
						),
					))
				}
			}
		}
	}

	// If a provider returns a null or non-null object at the wrong time then
	// we still want to save that but it often causes some confusing behaviors
	// where it seems like Terraform is failing to take any action at all,
	// so we'll generate some errors to draw attention to it.
	if !diags.HasErrors() {
		// if change.Action == plans.Delete && !newVal.IsNull() {
		// 	diags = diags.Append(tfdiags.Sourceless(
		// 		tfdiags.Error,
		// 		"Provider returned invalid result object after apply",
		// 		fmt.Sprintf(
		// 			"After applying a %s plan, the provider returned a non-null object for %s. Destroying should always produce a null value, so this is always a bug in the provider and should be reported in the provider's own repository. Terraform will still save this errant object in the state for debugging and recovery.",
		// 			change.Action, n.Addr,
		// 		),
		// 	))
		// }
		if newVal.IsNull() {
			diags = diags.Append(tfdiags.Sourceless(
				tfdiags.Error,
				"Provider returned invalid result object after apply",
				fmt.Sprintf(
					"After applying the configuration, the provider returned a null object for %s. Only destroying should always produce a null value, so this is always a bug in the provider and should be reported in the provider's own repository.",
					addr,
				),
			))
		}
	}

	switch {
	case diags.HasErrors() && newVal.IsNull():
		// Sometimes providers return a null value when an operation fails for
		// some reason, but we'd rather keep the prior state so that the error
		// can be corrected on a subsequent run. We must only do this for null
		// new value though, or else we may discard partial updates the
		// provider was able to complete. Otherwise, we'll continue using the
		// prior state as the new value, making this effectively a no-op.  If
		// the item really _has_ been deleted then our next refresh will detect
		// that and fix it up.
		return &ApplyResult{State: state, Schema: schema, SchemaVersion: version}, diags

	case diags.HasErrors() && !newVal.IsNull():
		refs, _ := lang.ReferencesInBlock(c.Config, schema)
		// diags.Append(d)
		dependsOn := mapSlice(c.DependsOn, func(t hcl.Traversal) *addrs.Reference {
			ref, d := addrs.ParseRef(t)
			diags = diags.Append(d)

			return ref
		})

		if diags.HasErrors() {
			panic(diags.Err())
		}

		refs = append(refs, dependsOn...)
		deps := make([]addrs.ConfigResource, 0)
		for _, r := range refs {
			switch t := r.Subject.(type) {
			case addrs.Resource:
				deps = append(deps, addrs.RootModule.Resource(t.Mode, t.Type, t.Name))
			}
		}

		// if we have an error, make sure we restore the object status in the new state
		newState := &states.ResourceInstanceObject{
			Status:              state.Status,
			Value:               newVal,
			Private:             resp.Private,
			CreateBeforeDestroy: state.CreateBeforeDestroy,
			Dependencies:        deps,
		}

		return &ApplyResult{State: newState, Schema: schema, SchemaVersion: version}, diags

	case !newVal.IsNull():
		// Non error case with a new state
		newState := &states.ResourceInstanceObject{
			Status:              states.ObjectReady,
			Value:               newVal,
			Private:             resp.Private,
			CreateBeforeDestroy: state.CreateBeforeDestroy,
		}
		return &ApplyResult{State: newState, Schema: schema, SchemaVersion: version}, diags

	default:
		// Non error case, were the object was deleted
		return nil, diags
	}
}

func (a *Allocator) getProvider(ctx EvalContext, addr addrs.AbsProviderConfig) (providers.Interface, error) {
	var p providers.Interface
	if p = ctx.Provider(addr); p == nil {
		initialized, err := ctx.InitProvider(addr)
		if err != nil {
			return nil, err
		}
		p = initialized
	}

	return p, nil
}

func (a *Allocator) configureProvider(ctx EvalContext, addr addrs.AbsProviderConfig) (diags tfdiags.Diagnostics) {
	log.Printf("[INFO] configuring provider %s", addr)

	key := a.Config.Module.LocalNameForProvider(addr.Provider)
	config, exists := a.Config.Module.ProviderConfigs[key]
	if !exists {
		if addr.Provider.Type != "terraform" {
			return diags.Append(tfdiags.Sourceless(
				tfdiags.Error,
				"Missing provider configuration",
				fmt.Sprintf("Provider %s is missing a configuration block", key),
			))
		}

		return diags
	}

	schemas, err := ctx.ProviderSchema(addr)
	if err != nil {
		return diags.Append(err)
	}

	val, _, configDiags := ctx.EvaluateBlock(config.Config, schemas.Provider, nil, EvalDataForNoInstanceKey)
	diags = diags.Append(configDiags)
	if diags.HasErrors() {
		return diags
	}

	p, err := a.getProvider(ctx, addr)
	if err != nil {
		return diags.Append(err)
	}

	validateResp := p.ValidateProviderConfig(providers.ValidateProviderConfigRequest{Config: val})
	diags = diags.Append(validateResp.Diagnostics)
	if diags.HasErrors() {
		return diags
	}

	diags = diags.Append(ctx.ConfigureProvider(addr, validateResp.PreparedConfig))

	return diags
}

type ackRequest struct {
	TransactionId string `json:"transactionId"`
}

func (a *Allocator) ackDestroy(txid string) (diags tfdiags.Diagnostics) {
	url, d := makeUrl(a.endpoint, a.scope, "ack")
	if d.HasErrors() {
		return diags.Append(d)
	}

	req := ackRequest{TransactionId: txid}
	resp := &empty{}

	return sendRequest(url, req, resp)
}

func (a *Allocator) destroyResource(ctx EvalContext, addr addrs.Resource) (diags tfdiags.Diagnostics) {
	log.Printf("[INFO] allocator: destroying resource %s", addr)

	if a.imported == nil {
		a.ImportState()
	}

	key := addr.String()
	r, exists := (*a.imported)[key]
	if !exists {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Resource not imported",
			fmt.Sprintf("Resource %s was not imported and so cannot be destroyed by the allocator", key),
		))
	}

	providerAddr, addrDiags := addrs.ParseAbsProviderConfigStr(r.Provider)
	diags = diags.Append(addrDiags)
	if diags.HasErrors() {
		return diags
	}

	p, err := a.getProvider(ctx, providerAddr)
	if err != nil {
		return diags.Append(err)
	}

	// `r.State.Type()` is technically not the correct type

	metaConfigVal, d := a.GetMeta(ctx, addr, providerAddr)
	diags = diags.Append(d)
	if diags.HasErrors() {
		return diags
	}

	ris := states.ResourceInstanceObjectSrc{
		SchemaVersion: r.SchemaVersion,
		AttrsJSON:     r.State,
	}

	schema, _, _ := a.GetSchema(ctx, addr, providerAddr)
	// Not sure how to avoid unmarshaling here
	decoded, err := ris.Decode(schema.ImpliedType())
	if err != nil {
		return diags.Append(err)
	}

	nullVal := cty.NullVal(decoded.Value.Type())
	planResp := p.PlanResourceChange(providers.PlanResourceChangeRequest{
		TypeName:         addr.Type,
		Config:           nullVal,
		PriorState:       decoded.Value,
		ProposedNewState: nullVal,
		PriorPrivate:     r.PrivateState,
		ProviderMeta:     *metaConfigVal,
	})

	diags = diags.Append(planResp.Diagnostics)
	if diags.HasErrors() {
		return diags
	}

	resp := p.ApplyResourceChange(providers.ApplyResourceChangeRequest{
		TypeName:       addr.Type,
		PriorState:     decoded.Value,
		PlannedState:   planResp.PlannedState,
		Config:         nullVal,
		PlannedPrivate: planResp.PlannedPrivate,
		ProviderMeta:   *metaConfigVal,
	})

	return diags.Append(resp.Diagnostics)
}

func (a *Allocator) GetSchema(ctx EvalContext, resource addrs.Resource, absProviderConfig addrs.AbsProviderConfig) (s *configschema.Block, version uint64, diags tfdiags.Diagnostics) {
	if s, exists := a.schemas[resource]; exists {
		return s.block, s.version, diags
	}

	schemas, err := ctx.ProviderSchema(absProviderConfig)
	if err != nil {
		return s, version, diags.Append(err)
	}

	schema, version := schemas.SchemaForResourceAddr(resource)
	if schema == nil {
		return nil, version, diags.Append(fmt.Errorf("provider does not have schema for resource %s", resource))
	}

	a.schemas[resource] = cachedSchema{block: schema, version: version}

	return schema, version, diags
}

func (a *Allocator) GetMeta(ctx EvalContext, resource addrs.Resource, absProviderConfig addrs.AbsProviderConfig) (v *cty.Value, diags tfdiags.Diagnostics) {
	if m, exists := a.providerMetaValues[absProviderConfig.Provider]; exists {
		return m, diags
	}
	schemas, _ := ctx.ProviderSchema(absProviderConfig)

	metaConfigVal := cty.NullVal(cty.DynamicPseudoType)
	if metaConfig, exists := a.ProvidersMeta[absProviderConfig.Provider]; exists {
		// FIXME: this should be cached
		val, _, configDiags := ctx.EvaluateBlock(metaConfig.Config, schemas.ProviderMeta, nil, EvalDataForNoInstanceKey)
		diags = diags.Append(configDiags)
		if diags.HasErrors() {
			return v, diags
		}
		metaConfigVal = val
	}

	v = &metaConfigVal
	a.providerMetaValues[absProviderConfig.Provider] = v

	return v, diags
}

//

type graphNodeResourceDestroyer struct {
	txid               string
	config             *configs.Resource
	allocator          *Allocator
	target             addrs.Resource
	providerConfigAddr addrs.ProviderConfig
	providerAddr       tfaddr.Provider
	absProviderConfig  addrs.AbsProviderConfig
}

func (n *graphNodeResourceDestroyer) Name() string {
	return fmt.Sprintf("%s (destroy)", n.target)
}

// Execute implements GraphNodeExecutable.
func (n *graphNodeResourceDestroyer) Execute(ctx EvalContext, op walkOperation) tfdiags.Diagnostics {
	diags := n.allocator.destroyResource(ctx, n.target)
	if diags.HasErrors() {
		return diags
	}

	log.Printf("[INFO] %s (destroy): sending ack", n.target)

	return n.allocator.ackDestroy(n.txid)
}

// ModulePath implements GraphNodeProviderConsumer.
func (*graphNodeResourceDestroyer) ModulePath() addrs.Module {
	return addrs.RootModule
}

// ProvidedBy implements GraphNodeProviderConsumer.
func (n *graphNodeResourceDestroyer) ProvidedBy() (addr addrs.ProviderConfig, exact bool) {
	return n.providerConfigAddr, true
}

// Provider implements GraphNodeProviderConsumer.
func (n *graphNodeResourceDestroyer) Provider() (provider tfaddr.Provider) {
	return n.providerAddr
}

// SetProvider implements GraphNodeProviderConsumer.
func (n *graphNodeResourceDestroyer) SetProvider(addr addrs.AbsProviderConfig) {
	n.absProviderConfig = addr
}

var (
	_ GraphNodeModulePath       = (*graphNodeResourceDestroyer)(nil)
	_ GraphNodeProviderConsumer = (*graphNodeResourceDestroyer)(nil)
	_ GraphNodeExecutable       = (*graphNodeResourceDestroyer)(nil)
)

var allocator *Allocator

func GetAllocator(config *configs.Config) *Allocator {
	if allocator != nil {
		return allocator
	}

	a, err := createAllocator(config)
	if err != nil {
		panic(err)
	}

	// allocator = a

	return a
}

type AllocatorTransformer struct {
	Config       *configs.Config
	State        *states.State
	ShouldExport bool
}

func (t *AllocatorTransformer) Transform(g *Graph) error {
	return t.transformModule(g, t.Config)
}

func (t *AllocatorTransformer) transformModule(g *Graph, c *configs.Config) error {
	if c == nil {
		return nil
	}

	allocator := c.Module.Allocator
	if allocator == nil {
		return nil
	}

	imported, diags := GetAllocator(c).ImportState()
	if diags.HasErrors() {
		return diags.Err()
	}

	ir := make([]importedResource, 0)
	rMap := map[string]*configs.Resource{}
	for _, r := range c.Module.ManagedResources {
		addr := r.Addr()
		key := addr.String()
		rMap[key] = r

		if s, exists := (*imported)[key]; exists {
			ir = append(ir, importedResource{
				addr:     addr,
				provider: c.ResolveAbsProviderAddr(r.ProviderConfigAddr(), addrs.RootModule),
				state:    s,
			})
		}
	}

	exported := make([]exportedResource, 0)
	for _, ref := range allocator.Resources {
		key := ref.Subject.String()
		if _, exists := (*imported)[key]; !exists {
			if r, rExists := rMap[key]; rExists {
				exported = append(exported, exportedResource{
					addr:     r.Addr(),
					provider: c.ResolveAbsProviderAddr(r.ProviderConfigAddr(), addrs.RootModule),
				})

				log.Printf("[INFO] allocator: planning to export resource %s", key)
			}
		}
	}

	nodes := g.Vertices()
	for i := 0; i < len(nodes); i++ {
		n := nodes[i]
		switch n := n.(type) {
		case GraphNodeConfigResource:
			key := n.ResourceAddr().String()
			log.Printf("[INFO] allocator: %s ", key)

			if _, exists := (*imported)[key]; !exists {
				continue
			}
		default:
			continue
		}

		log.Printf("[INFO] allocator: %s is no longer needed, removing", dag.VertexName(n))
		g.Remove(n)
	}

	for _, r := range ir {
		node := graphNodeResourceAllocator{resource: r}
		g.Add(&node)
	}

	if len(exported) > 0 && t.ShouldExport {
		g.Add(&graphNodeAllocatorExport{
			Endpoint: allocator.Endpoint,
			Scope:    allocator.Scope,
			Module:   allocator.Module,
			Exported: exported,
		})
	}

	return nil
}

type DeallocatorTransformer struct {
	Config *configs.Config
	Skip   bool
}

func (t *DeallocatorTransformer) Transform(g *Graph) error {
	return t.transformModule(g, t.Config)
}

func (t *DeallocatorTransformer) transformModule(g *Graph, c *configs.Config) error {
	if c == nil || t.Skip {
		return nil
	}

	allocator := c.Module.Allocator
	if allocator == nil {
		return nil
	}

	log.Printf("[INFO] allocator: transforming for destroy")

	shouldDestroy, diags := GetAllocator(c).Unref()
	if diags.HasErrors() {
		return diags.Err()
	}

	log.Printf("[INFO] allocator: planning to destroy %d resources", len(shouldDestroy))

	for _, tx := range shouldDestroy {
		config := c.Root.Module.ResourceByAddr(tx.Resource.Resource)
		g.Add(&graphNodeResourceDestroyer{
			txid:               tx.TransactionId,
			config:             config,
			allocator:          GetAllocator(c),
			target:             tx.Resource.Resource,
			providerAddr:       config.Provider,
			providerConfigAddr: config.ProviderConfigAddr(),
			absProviderConfig:  c.ResolveAbsProviderAddr(config.ProviderConfigAddr(), addrs.RootModule),
		})
	}

	return nil
}

type importedResource struct {
	addr     addrs.Resource
	provider addrs.AbsProviderConfig
	state    ExportedResource
}

type exportedResource struct {
	addr     addrs.Resource
	provider addrs.AbsProviderConfig
}

type graphNodeAllocatorExport struct {
	Endpoint string
	Scope    string
	Module   string
	Exported []exportedResource
}

var (
	_ GraphNodeModulePath = (*graphNodeAllocatorExport)(nil)
	_ GraphNodeExecutable = (*graphNodeAllocatorExport)(nil)
	_ GraphNodeReferencer = (*graphNodeAllocatorExport)(nil)
)

func (n *graphNodeAllocatorExport) Name() string {
	return fmt.Sprintf("allocator (export)")
}

func (n *graphNodeAllocatorExport) ModulePath() addrs.Module {
	return addrs.RootModule
}

func (n *graphNodeAllocatorExport) References() []*addrs.Reference {
	refs := make([]*addrs.Reference, len(n.Exported))
	for i, ref := range n.Exported {
		refs[i] = &addrs.Reference{Subject: ref.addr}
	}

	return refs
}

type ExportedResource struct {
	State               json.RawMessage `json:"attributes"`
	Provider            string          `json:"provider"`
	SchemaVersion       uint64          `json:"schema_version"`
	Dependencies        []string        `json:"dependencies"`
	PrivateState        []byte          `json:"private,omitempty"`
	CreateBeforeDestroy bool            `json:"create_before_destroy,omitempty"`
}

func mapSlice[T any, U any](a []T, fn func(v T) U) []U {
	b := make([]U, len(a))
	for i, v := range a {
		b[i] = fn(v)
	}
	return b
}

func (n *graphNodeAllocatorExport) Execute(ctx EvalContext, op walkOperation) (diags tfdiags.Diagnostics) {
	if op == walkApply {
		log.Printf("[INFO] allocator: exporting resources, total count %d", len(n.Exported))
		resources := map[string]ExportedResource{}
		for _, r := range n.Exported {
			absAddr := r.addr.Absolute(addrs.RootModuleInstance)
			state := ctx.State().Resource(absAddr)
			is := state.Instance(addrs.NoKey)
			if is != nil && is.HasCurrent() {
				schemas, _ := ctx.ProviderSchema(r.provider)
				resources[r.addr.String()] = ExportedResource{
					State:               is.Current.AttrsJSON,
					Provider:            r.provider.String(),
					SchemaVersion:       schemas.ResourceTypeSchemaVersions[r.addr.Type],
					PrivateState:        is.Current.Private,
					CreateBeforeDestroy: is.Current.CreateBeforeDestroy,
					Dependencies: mapSlice(is.Current.Dependencies, func(v addrs.ConfigResource) string {
						return v.String()
					}),
				}

				// Don't persist this resource
				is.Current.Imported = true
				ctx.State().SetResourceInstanceCurrent(absAddr.Instance(addrs.NoKey), is.Current, r.provider)
			}
		}

		return diags.Append(exportState(n.Endpoint, n.Scope, n.Module, resources))
	}

	return diags
}

type allocatorImportRequest struct {
	Module    string   `json:"callerModuleId"`
	Resources []string `json:"resources"`
}

type allocatorImportResponse struct {
	Resources map[string]ExportedResource `json:"resources"`
}

type allocatorErrorResponse struct {
	Message string `json:"message"`
}

func sendRequest[T any, R any](endpoint string, body T, ret R) (diags tfdiags.Diagnostics) {
	client := httpclient.New()
	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(body)
	if err != nil {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Bad encode",
			fmt.Sprintf("Bad encode %s", err),
		))
	}

	httpRequest, _ := http.NewRequest("POST", endpoint, buf)
	httpRequest.Header["Content-Type"] = []string{"application/json"}

	username, _ := os.LookupEnv("TF_HTTP_USERNAME")
	password, _ := os.LookupEnv("TF_HTTP_PASSWORD")
	if username != "" && password != "" {
		httpRequest.SetBasicAuth(username, password)
	}

	resp, err := client.Do(httpRequest)
	if err != nil {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Resource allocator unavailable",
			fmt.Sprintf("The resource allocator failed to respond at %s: %w", endpoint, err),
		))
	}

	if resp.StatusCode >= 300 {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Resource allocator returned non-2xx status code",
			fmt.Sprintf("The resource allocator returned non-2xx status code: %d %s", resp.StatusCode, resp.Status),
		))
	}

	if resp.StatusCode == 204 {
		return diags
	}

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(ret)
	if err != nil {
		return diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Resource allocator bad response",
			fmt.Sprintf("Malformed response: %w", err),
		))
	}

	return diags
}

func makeUrl(endpoint, scope, path string) (ret string, diags tfdiags.Diagnostics) {
	url, err := url.Parse(endpoint)
	if err != nil {
		return ret, diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Invalid allocator endpoint",
			fmt.Sprintf("Endpoint is not a url %s: %s", endpoint, err),
		))
	}

	ret = url.JoinPath(path, scope).String()
	return ret, diags
}

type allocatorExportRequest struct {
	Module    string                      `json:"callerModuleId"`
	Resources map[string]ExportedResource `json:"resources"`
}

type empty struct{}

func exportState(endpoint, scope, module string, resources map[string]ExportedResource) (diags tfdiags.Diagnostics) {
	req := allocatorExportRequest{
		Module:    module,
		Resources: resources,
	}

	url, d := makeUrl(endpoint, scope, "export")
	if d.HasErrors() {
		return diags.Append(d)
	}

	resp := &empty{}
	d = sendRequest(url, req, resp)
	if d.HasErrors() {
		return diags.Append(d)
	}

	return diags
}

type allocatorUnrefRequest struct {
	Module string `json:"callerModuleId"`
}

// FIXME: this is not really a good architecture but it's easy to implement
type allocatorUnrefResponse struct {
	TransactionId string `json:"transactionId"`
	// Maps resource -> txid
	ShouldDestroy map[string]string `json:"shouldDestroy"`
}

func (a *Allocator) Unref() (shouldDestroy map[string]pendingDestroy, diags tfdiags.Diagnostics) {
	if a.pendingDestroy != nil {
		return *a.pendingDestroy, diags
	}

	a.pendingDestroy = &map[string]pendingDestroy{}

	req := allocatorUnrefRequest{
		Module: a.module,
	}

	url, d := makeUrl(a.endpoint, a.scope, "unref")
	if d.HasErrors() {
		return shouldDestroy, diags.Append(d)
	}

	resp := &allocatorUnrefResponse{}
	d = sendRequest(url, req, resp)
	if d.HasErrors() {
		return shouldDestroy, diags.Append(d)
	}

	// TODO: how to make this work in parallel?
	for r, txid := range resp.ShouldDestroy {
		addr, addrDiags := addrs.ParseAbsResourceStr(r)
		diags = diags.Append(addrDiags)
		if diags.HasErrors() {
			return shouldDestroy, diags
		}

		(*a.pendingDestroy)[r] = pendingDestroy{
			TransactionId: txid,
			Resource:      addr,
		}
	}

	return *a.pendingDestroy, diags
}

type graphNodeResourceAllocator struct {
	resource importedResource
}

var (
	_ GraphNodeModulePath       = (*graphNodeResourceAllocator)(nil)
	_ GraphNodeExecutable       = (*graphNodeResourceAllocator)(nil)
	_ GraphNodeReferenceable    = (*graphNodeResourceAllocator)(nil)
	_ GraphNodeProviderConsumer = (*graphNodeResourceAllocator)(nil)
)

func (n *graphNodeResourceAllocator) Name() string {
	return fmt.Sprintf("%s (import)", n.resource.addr)
}

// ReferenceableAddrs implements GraphNodeReferenceable.
func (n *graphNodeResourceAllocator) ReferenceableAddrs() []addrs.Referenceable {
	return []addrs.Referenceable{n.resource.addr}
}

// ModulePath implements GraphNodeProviderConsumer.
func (*graphNodeResourceAllocator) ModulePath() addrs.Module {
	return addrs.RootModule
}

// ProvidedBy implements GraphNodeProviderConsumer.
func (n *graphNodeResourceAllocator) ProvidedBy() (addr addrs.ProviderConfig, exact bool) {
	return n.resource.provider, true
}

// Provider implements GraphNodeProviderConsumer.
func (n *graphNodeResourceAllocator) Provider() (provider tfaddr.Provider) {
	return n.resource.provider.Provider
}

// SetProvider implements GraphNodeProviderConsumer.
func (n *graphNodeResourceAllocator) SetProvider(addr addrs.AbsProviderConfig) {
}

func (n *graphNodeResourceAllocator) Execute(ctx EvalContext, op walkOperation) (diags tfdiags.Diagnostics) {
	if op == walkPlanDestroy || op == walkDestroy {
		return diags
	}

	ris := states.ResourceInstanceObjectSrc{
		SchemaVersion: n.resource.state.SchemaVersion,
		AttrsJSON:     n.resource.state.State,
	}
	schemas, _ := ctx.ProviderSchema(n.resource.provider)
	schema, _ := schemas.SchemaForResourceAddr(n.resource.addr)
	// Not sure how to avoid unmarshaling here
	decoded, err := ris.Decode(schema.ImpliedType())
	if err != nil {
		return diags.Append(err)
	}

	decoded.Status = states.ObjectReady
	decoded.Imported = true

	addr := n.resource.addr.Instance(addrs.NoKey).Absolute(addrs.RootModuleInstance)
	node := &NodeAbstractResourceInstance{
		Addr: addr,
		NodeAbstractResource: NodeAbstractResource{
			ResolvedProvider: n.resource.provider,
		},
	}

	diags = diags.Append(node.writeResourceInstanceState(ctx, decoded, workingState))
	log.Printf("[INFO] graphNodeAllocator: wrote %s", n.resource.addr)

	return diags
}
