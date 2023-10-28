// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package terraform

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/apparentlymart/go-versions/versions"
	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/configs/configschema"
	"github.com/hashicorp/terraform/internal/depsfile"
	"github.com/hashicorp/terraform/internal/getproviders"
	"github.com/hashicorp/terraform/internal/providercache"
	"github.com/hashicorp/terraform/internal/providers"
	"github.com/hashicorp/terraform/internal/provisioners"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

type CachedProvider struct {
	provider   providers.Interface
	configured bool
}

// contextPlugins represents a library of available plugins (providers and
// provisioners) which we assume will all be used with the same
// terraform.Context, and thus it'll be safe to cache certain information
// about the providers for performance reasons.
type contextPlugins struct {
	KeepAlive            bool
	ProviderCache        map[string]*CachedProvider
	providerFactories    map[addrs.Provider]providers.Factory
	provisionerFactories map[string]provisioners.Factory

	// We memoize the schemas we've previously loaded in here, to avoid
	// repeatedly paying the cost of activating the same plugins to access
	// their schemas in various different spots. We use schemas for many
	// purposes in Terraform, so there isn't a single choke point where
	// it makes sense to preload all of them.
	providerSchemas    map[addrs.Provider]*ProviderSchema
	provisionerSchemas map[string]*configschema.Block
	schemasLock        sync.RWMutex
	cacheLock          sync.RWMutex
	providerLocks      map[addrs.Provider]*sync.Mutex
	// schemaLocks        map[addrs.Provider]*sync.Mutex

	installer       *providercache.Installer
	persistLockFile func(*depsfile.Locks) tfdiags.Diagnostics
}

func newContextPlugins(
	providerFactories map[addrs.Provider]providers.Factory,
	provisionerFactories map[string]provisioners.Factory,
	installer *providercache.Installer,
	persistLockFile func(*depsfile.Locks) tfdiags.Diagnostics,
	keepAlive bool,
) *contextPlugins {
	ret := &contextPlugins{
		KeepAlive:            keepAlive,
		providerFactories:    providerFactories,
		provisionerFactories: provisionerFactories,
		installer:            installer,
		persistLockFile:      persistLockFile,
	}
	ret.init()
	return ret
}

func (cp *contextPlugins) init() {
	cp.ProviderCache = map[string]*CachedProvider{}
	cp.providerSchemas = make(map[addrs.Provider]*ProviderSchema, len(cp.providerFactories))
	cp.provisionerSchemas = make(map[string]*configschema.Block, len(cp.provisionerFactories))
	cp.providerLocks = make(map[addrs.Provider]*sync.Mutex, len(cp.providerFactories))
	// cp.schemaLocks = make(map[addrs.Provider]*sync.Mutex, len(cp.providerFactories))

	for addr := range cp.providerFactories {
		cp.providerLocks[addr] = &sync.Mutex{}
		// cp.schemaLocks[addr] = &sync.Mutex{}
	}
}

func (cp *contextPlugins) SetCache(cache map[string]*CachedProvider) {
	cp.ProviderCache = cache
}

func (cp *contextPlugins) GetCache() map[string]*CachedProvider {
	return cp.ProviderCache
}

func (cp *contextPlugins) InstallProvider(ctx context.Context, addr addrs.Provider, version versions.Version, platform getproviders.Platform) error {
	locks, err := cp.installer.InstallProvider(ctx, addr, version, platform)
	if err != nil {
		return err
	}

	diags := cp.persistLockFile(locks)
	if diags.HasErrors() {
		return diags.Err()
	}

	return nil
}

func (cp *contextPlugins) HasProvider(addr addrs.Provider) bool {
	_, ok := cp.providerFactories[addr]
	return ok
}

func (cp *contextPlugins) createProvider(addr addrs.Provider) (providers.Interface, error) {
	l, ok := cp.providerLocks[addr]
	if !ok {
		return nil, fmt.Errorf("unavailable provider %q", addr.String())
	}

	l.Lock()
	defer l.Unlock()

	return cp.providerFactories[addr]()
}

func (cp *contextPlugins) NewProviderInstance(addr addrs.Provider) (providers.Interface, error) {
	cp.cacheLock.RLock()
	if cached, exists := cp.ProviderCache[addr.String()]; exists {
		defer cp.cacheLock.RUnlock()

		return cached.provider, nil
	}

	cp.cacheLock.RUnlock()

	p, err := cp.createProvider(addr)
	if err != nil {
		return nil, err
	}

	cp.cacheLock.Lock()
	cp.ProviderCache[addr.String()] = &CachedProvider{provider: p}
	cp.cacheLock.Unlock()

	return p, nil
}

func (cp *contextPlugins) HasProvisioner(typ string) bool {
	_, ok := cp.provisionerFactories[typ]
	return ok
}

func (cp *contextPlugins) NewProvisionerInstance(typ string) (provisioners.Interface, error) {
	f, ok := cp.provisionerFactories[typ]
	if !ok {
		return nil, fmt.Errorf("unavailable provisioner %q", typ)
	}

	return f()
}

func (cp *contextPlugins) getSchema(addr addrs.Provider) (*ProviderSchema, bool) {
	cp.schemasLock.RLock()
	defer cp.schemasLock.RUnlock()
	schema, ok := cp.providerSchemas[addr]
	return schema, ok
}

// ProviderSchema uses a temporary instance of the provider with the given
// address to obtain the full schema for all aspects of that provider.
//
// ProviderSchema memoizes results by unique provider address, so it's fine
// to repeatedly call this method with the same address if various different
// parts of Terraform all need the same schema information.
func (cp *contextPlugins) ProviderSchema(addr addrs.Provider) (*ProviderSchema, error) {
	if schema, ok := cp.getSchema(addr); ok {
		return schema, nil
	}

	log.Printf("[TRACE] terraform.contextPlugins: Initializing provider %q to read its schema", addr)

	provider, err := cp.NewProviderInstance(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate provider %q to obtain schema: %s", addr, err)
	}

	// if !cp.KeepAlive {
	// 	defer provider.Close()
	// }

	l := cp.providerLocks[addr]
	l.Lock()
	defer l.Unlock()

	// Check again in case we were locked
	if schema, ok := cp.getSchema(addr); ok {
		return schema, nil
	}

	// We don't really need to cache this. It's very fast.
	resp := provider.GetProviderSchema()
	if resp.Diagnostics.HasErrors() {
		return nil, fmt.Errorf("failed to retrieve schema from provider %q: %s", addr, resp.Diagnostics.Err())
	}

	s := &ProviderSchema{
		Provider:      resp.Provider.Block,
		ResourceTypes: make(map[string]*configschema.Block),
		DataSources:   make(map[string]*configschema.Block),

		ResourceTypeSchemaVersions: make(map[string]uint64),
	}

	if resp.Provider.Version < 0 {
		// We're not using the version numbers here yet, but we'll check
		// for validity anyway in case we start using them in future.
		return nil, fmt.Errorf("provider %s has invalid negative schema version for its configuration blocks,which is a bug in the provider ", addr)
	}

	for t, r := range resp.ResourceTypes {
		// TODO: lazy validation
		// if err := r.Block.InternalValidate(); err != nil {
		// 	return nil, fmt.Errorf("provider %s has invalid schema for managed resource type %q, which is a bug in the provider: %q", addr, t, err)
		// }
		s.ResourceTypes[t] = r.Block
		s.ResourceTypeSchemaVersions[t] = uint64(r.Version)
		if r.Version < 0 {
			return nil, fmt.Errorf("provider %s has invalid negative schema version for managed resource type %q, which is a bug in the provider", addr, t)
		}
	}

	for t, d := range resp.DataSources {
		// TODO: lazy validation
		// if err := d.Block.InternalValidate(); err != nil {
		// 	return nil, fmt.Errorf("provider %s has invalid schema for data resource type %q, which is a bug in the provider: %q", addr, t, err)
		// }
		s.DataSources[t] = d.Block
		if d.Version < 0 {
			// We're not using the version numbers here yet, but we'll check
			// for validity anyway in case we start using them in future.
			return nil, fmt.Errorf("provider %s has invalid negative schema version for data resource type %q, which is a bug in the provider", addr, t)
		}
	}

	if resp.ProviderMeta.Block != nil {
		s.ProviderMeta = resp.ProviderMeta.Block
	}

	cp.schemasLock.Lock()
	cp.providerSchemas[addr] = s
	cp.schemasLock.Unlock()

	return s, nil
}

// ProviderConfigSchema is a helper wrapper around ProviderSchema which first
// reads the full schema of the given provider and then extracts just the
// provider's configuration schema, which defines what's expected in a
// "provider" block in the configuration when configuring this provider.
func (cp *contextPlugins) ProviderConfigSchema(providerAddr addrs.Provider) (*configschema.Block, error) {
	providerSchema, err := cp.ProviderSchema(providerAddr)
	if err != nil {
		return nil, err
	}

	return providerSchema.Provider, nil
}

// ResourceTypeSchema is a helper wrapper around ProviderSchema which first
// reads the schema of the given provider and then tries to find the schema
// for the resource type of the given resource mode in that provider.
//
// ResourceTypeSchema will return an error if the provider schema lookup
// fails, but will return nil if the provider schema lookup succeeds but then
// the provider doesn't have a resource of the requested type.
//
// Managed resource types have versioned schemas, so the second return value
// is the current schema version number for the requested resource. The version
// is irrelevant for other resource modes.
func (cp *contextPlugins) ResourceTypeSchema(providerAddr addrs.Provider, resourceMode addrs.ResourceMode, resourceType string) (*configschema.Block, uint64, error) {
	providerSchema, err := cp.ProviderSchema(providerAddr)
	if err != nil {
		return nil, 0, err
	}

	schema, version := providerSchema.SchemaForResourceType(resourceMode, resourceType)
	return schema, version, nil
}

// ProvisionerSchema uses a temporary instance of the provisioner with the
// given type name to obtain the schema for that provisioner's configuration.
//
// ProvisionerSchema memoizes results by provisioner type name, so it's fine
// to repeatedly call this method with the same name if various different
// parts of Terraform all need the same schema information.
func (cp *contextPlugins) ProvisionerSchema(typ string) (*configschema.Block, error) {
	cp.schemasLock.Lock()
	defer cp.schemasLock.Unlock()

	if schema, ok := cp.provisionerSchemas[typ]; ok {
		return schema, nil
	}

	log.Printf("[TRACE] terraform.contextPlugins: Initializing provisioner %q to read its schema", typ)
	provisioner, err := cp.NewProvisionerInstance(typ)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate provisioner %q to obtain schema: %s", typ, err)
	}
	defer provisioner.Close()

	resp := provisioner.GetSchema()
	if resp.Diagnostics.HasErrors() {
		return nil, fmt.Errorf("failed to retrieve schema from provisioner %q: %s", typ, resp.Diagnostics.Err())
	}

	cp.provisionerSchemas[typ] = resp.Provisioner
	return resp.Provisioner, nil
}
