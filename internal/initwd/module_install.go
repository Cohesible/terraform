// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package initwd

import (
	"context"
	"fmt"
	"log"
	"path"
	"path/filepath"
	"strings"

	"github.com/hashicorp/terraform/internal/hcl"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/configs"
	"github.com/hashicorp/terraform/internal/configs/configload"
	"github.com/hashicorp/terraform/internal/modsdir"
	"github.com/hashicorp/terraform/internal/registry"
	"github.com/hashicorp/terraform/internal/registry/response"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

type ModuleInstaller struct {
	modsDir string
	loader  *configload.Loader
	reg     *registry.Client

	// The keys in moduleVersions are resolved and trimmed registry source
	// addresses and the values are the registry response.
	registryPackageVersions map[addrs.ModuleRegistryPackage]*response.ModuleVersions

	// The keys in moduleVersionsUrl are the moduleVersion struct below and
	// addresses and the values are underlying remote source addresses.
	registryPackageSources map[moduleVersion]addrs.ModuleSourceRemote
}

type moduleVersion struct {
	module  addrs.ModuleRegistryPackage
	version string
}

func NewModuleInstaller(modsDir string, loader *configload.Loader, reg *registry.Client) *ModuleInstaller {
	return &ModuleInstaller{
		modsDir:                 modsDir,
		loader:                  loader,
		reg:                     reg,
		registryPackageVersions: make(map[addrs.ModuleRegistryPackage]*response.ModuleVersions),
		registryPackageSources:  make(map[moduleVersion]addrs.ModuleSourceRemote),
	}
}

// InstallModules analyses the root module in the given directory and installs
// all of its direct and transitive dependencies into the given modules
// directory, which must already exist.
//
// Since InstallModules makes possibly-time-consuming calls to remote services,
// a hook interface is supported to allow the caller to be notified when
// each module is installed and, for remote modules, when downloading begins.
// LoadConfig guarantees that two hook calls will not happen concurrently but
// it does not guarantee any particular ordering of hook calls. This mechanism
// is for UI feedback only and does not give the caller any control over the
// process.
//
// If modules are already installed in the target directory, they will be
// skipped unless their source address or version have changed or unless
// the upgrade flag is set.
//
// InstallModules never deletes any directory, except in the case where it
// needs to replace a directory that is already present with a newly-extracted
// package.
//
// If the returned diagnostics contains errors then the module installation
// may have wholly or partially completed. Modules must be loaded in order
// to find their dependencies, so this function does many of the same checks
// as LoadConfig as a side-effect.
//
// If successful (the returned diagnostics contains no errors) then the
// first return value is the early configuration tree that was constructed by
// the installation process.
func (i *ModuleInstaller) InstallModules(ctx context.Context, rootDir string, upgrade bool, hooks ModuleInstallHooks) (*configs.Config, tfdiags.Diagnostics) {
	log.Printf("[TRACE] ModuleInstaller: installing child modules for %s into %s", rootDir, i.modsDir)
	var diags tfdiags.Diagnostics

	rootMod, mDiags := i.loader.Parser().LoadConfigDir(rootDir)
	if rootMod == nil {
		// We drop the diagnostics here because we only want to report module
		// loading errors after checking the core version constraints, which we
		// can only do if the module can be at least partially loaded.
		return nil, diags
	} else if vDiags := rootMod.CheckCoreVersionRequirements(nil, nil); vDiags.HasErrors() {
		// If the core version requirements are not met, we drop any other
		// diagnostics, as they may reflect language changes from future
		// Terraform versions.
		diags = diags.Append(vDiags)
	} else {
		diags = diags.Append(mDiags)
	}

	// manifest, err := modsdir.ReadManifestSnapshotForDir(i.modsDir)
	// if err != nil {
	// 	diags = diags.Append(tfdiags.Sourceless(
	// 		tfdiags.Error,
	// 		"Failed to read modules manifest file",
	// 		fmt.Sprintf("Error reading manifest for %s: %s.", i.modsDir, err),
	// 	))
	// 	return nil, diags
	// }

	return nil, diags

	// fetcher := getmodules.NewPackageFetcher()
	// cfg, instDiags := i.installDescendentModules(ctx, rootMod, rootDir, manifest, upgrade, hooks, fetcher)
	// diags = append(diags, instDiags...)

	// return cfg, diags
}

func (i *ModuleInstaller) installLocalModule(req *configs.ModuleRequest, key string, manifest modsdir.Manifest, hooks ModuleInstallHooks) (*configs.Module, hcl.Diagnostics) {
	var diags hcl.Diagnostics

	parentKey := manifest.ModuleKey(req.Parent.Path)
	parentRecord, recorded := manifest[parentKey]
	if !recorded {
		// This is indicative of a bug rather than a user-actionable error
		panic(fmt.Errorf("missing manifest record for parent module %s", parentKey))
	}

	if len(req.VersionConstraint.Required) != 0 {
		diags = diags.Append(&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "Invalid version constraint",
			Detail:   fmt.Sprintf("Cannot apply a version constraint to module %q (at %s:%d) because it has a relative local path.", req.Name, req.CallRange.Filename, req.CallRange.Start.Line),
			Subject:  req.CallRange.Ptr(),
		})
	}

	// For local sources we don't actually need to modify the
	// filesystem at all because the parent already wrote
	// the files we need, and so we just load up what's already here.
	newDir := filepath.Join(parentRecord.Dir, req.SourceAddr.String())

	log.Printf("[TRACE] ModuleInstaller: %s uses directory from parent: %s", key, newDir)
	// it is possible that the local directory is a symlink
	newDir, err := filepath.EvalSymlinks(newDir)
	if err != nil {
		diags = diags.Append(&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "Unreadable module directory",
			Detail:   fmt.Sprintf("Unable to evaluate directory symlink: %s", err.Error()),
		})
	}

	// Finally we are ready to try actually loading the module.
	mod, mDiags := i.loader.Parser().LoadConfigDir(newDir)
	if mod == nil {
		// nil indicates missing or unreadable directory, so we'll
		// discard the returned diags and return a more specific
		// error message here.
		diags = diags.Append(&hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "Unreadable module directory",
			Detail:   fmt.Sprintf("The directory %s could not be read for module %q at %s:%d.", newDir, req.Name, req.CallRange.Filename, req.CallRange.Start.Line),
		})
	} else if vDiags := mod.CheckCoreVersionRequirements(req.Path, req.SourceAddr); vDiags.HasErrors() {
		// If the core version requirements are not met, we drop any other
		// diagnostics, as they may reflect language changes from future
		// Terraform versions.
		diags = diags.Extend(vDiags)
	} else {
		diags = diags.Extend(mDiags)
	}

	// Note the local location in our manifest.
	manifest[key] = modsdir.Record{
		Key:        key,
		Dir:        newDir,
		SourceAddr: req.SourceAddr.String(),
	}
	log.Printf("[DEBUG] Module installer: %s installed at %s", key, newDir)
	hooks.Install(key, nil, newDir)

	return mod, diags
}

func (i *ModuleInstaller) packageInstallPath(modulePath addrs.Module) string {
	return filepath.Join(i.modsDir, strings.Join(modulePath, "."))
}

// maybeImproveLocalInstallError is a helper function which can recognize
// some specific situations where it can return a more helpful error message
// and thus replace the given errors with those if so.
//
// If this function can't do anything about a particular situation then it
// will just return the given diags verbatim.
//
// This function's behavior is only reasonable for errors returned from the
// ModuleInstaller.installLocalModule function.
func maybeImproveLocalInstallError(req *configs.ModuleRequest, diags hcl.Diagnostics) hcl.Diagnostics {
	if !diags.HasErrors() {
		return diags
	}

	// The main situation we're interested in detecting here is whether the
	// current module or any of its ancestors use relative paths that reach
	// outside of the "package" established by the nearest non-local ancestor.
	// That's never really valid, but unfortunately we historically didn't
	// have any explicit checking for it and so now for compatibility in
	// situations where things just happened to "work" we treat this as an
	// error only in situations where installation would've failed anyway,
	// so we can give a better error about it than just a generic
	// "directory not found" or whatever.
	//
	// Since it's never actually valid to relative out of the containing
	// package, we just assume that any failed local package install which
	// does so was caused by that, because to stop doing it should always
	// improve the situation, even if it leads to another error describing
	// a different problem.

	// To decide this we need to find the subset of our ancestors that
	// belong to the same "package" as our request, along with the closest
	// ancestor that defined that package, and then we can work forwards
	// to see if any of the local paths "escaped" the package.
	type Step struct {
		Path       addrs.Module
		SourceAddr addrs.ModuleSource
	}
	var packageDefiner Step
	var localRefs []Step
	localRefs = append(localRefs, Step{
		Path:       req.Path,
		SourceAddr: req.SourceAddr,
	})
	current := req.Parent // a configs.Config where Children isn't populated yet
	for {
		if current == nil || current.SourceAddr == nil {
			// We've reached the root module, in which case we aren't
			// in an external "package" at all and so our special case
			// can't apply.
			return diags
		}
		if _, ok := current.SourceAddr.(addrs.ModuleSourceLocal); !ok {
			// We've found the package definer, then!
			packageDefiner = Step{
				Path:       current.Path,
				SourceAddr: current.SourceAddr,
			}
			break
		}

		localRefs = append(localRefs, Step{
			Path:       current.Path,
			SourceAddr: current.SourceAddr,
		})
		current = current.Parent
	}
	// Our localRefs list is reversed because we were traversing up the tree,
	// so we'll flip it the other way and thus walk "downwards" through it.
	for i, j := 0, len(localRefs)-1; i < j; i, j = i+1, j-1 {
		localRefs[i], localRefs[j] = localRefs[j], localRefs[i]
	}

	// Our method here is to start with a known base path prefix and
	// then apply each of the local refs to it in sequence until one of
	// them causes us to "lose" the prefix. If that happens, we've found
	// an escape to report. This is not an exact science but good enough
	// heuristic for choosing a better error message.
	const prefix = "*/" // NOTE: this can find a false negative if the user chooses "*" as a directory name, but we consider that unlikely
	packageAddr, startPath := splitAddrSubdir(packageDefiner.SourceAddr)
	currentPath := path.Join(prefix, startPath)
	for _, step := range localRefs {
		rel := step.SourceAddr.String()

		nextPath := path.Join(currentPath, rel)
		if !strings.HasPrefix(nextPath, prefix) { // ESCAPED!
			escapeeAddr := step.Path.String()

			var newDiags hcl.Diagnostics

			// First we'll copy over any non-error diagnostics from the source diags
			for _, diag := range diags {
				if diag.Severity != hcl.DiagError {
					newDiags = newDiags.Append(diag)
				}
			}

			// ...but we'll replace any errors with this more precise error.
			var suggestion string
			if strings.HasPrefix(packageAddr, "/") || filepath.VolumeName(packageAddr) != "" {
				// It might be somewhat surprising that Terraform treats
				// absolute filesystem paths as "external" even though it
				// treats relative paths as local, so if it seems like that's
				// what the user was doing then we'll add an additional note
				// about it.
				suggestion = "\n\nTerraform treats absolute filesystem paths as external modules which establish a new module package. To treat this directory as part of the same package as its caller, use a local path starting with either \"./\" or \"../\"."
			}
			newDiags = newDiags.Append(&hcl.Diagnostic{
				Severity: hcl.DiagError,
				Summary:  "Local module path escapes module package",
				Detail: fmt.Sprintf(
					"The given source directory for %s would be outside of its containing package %q. Local source addresses starting with \"../\" must stay within the same package that the calling module belongs to.%s",
					escapeeAddr, packageAddr, suggestion,
				),
			})

			return newDiags
		}

		currentPath = nextPath
	}

	// If we get down here then we have nothing useful to do, so we'll just
	// echo back what we were given.
	return diags
}

func splitAddrSubdir(addr addrs.ModuleSource) (string, string) {
	switch addr := addr.(type) {
	case addrs.ModuleSourceRegistry:
		subDir := addr.Subdir
		addr.Subdir = ""
		return addr.String(), subDir
	case addrs.ModuleSourceRemote:
		return addr.Package.String(), addr.Subdir
	case nil:
		panic("splitAddrSubdir on nil addrs.ModuleSource")
	default:
		return addr.String(), ""
	}
}
