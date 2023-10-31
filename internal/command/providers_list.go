// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package command

import (
	"encoding/json"
	"fmt"

	tfaddr "github.com/hashicorp/terraform-registry-address"
	"github.com/hashicorp/terraform/internal/command/views"
	"github.com/hashicorp/terraform/internal/getproviders"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

// ProvidersListCommand is a Command implementation that implements the
// "terraform providers mirror" command, which populates a directory with
// local copies of provider plugins needed by the current configuration so
// that the mirror can be used to work offline, or similar.
type ProvidersListCommand struct {
	Meta
}

func (c *ProvidersListCommand) Synopsis() string {
	return "Shows available versions of a provider"
}

func (c *ProvidersListCommand) Run(args []string) int {
	view := views.NewJSONView(c.View)

	args = c.Meta.process(args)
	cmdFlags := c.Meta.defaultFlagSet("providers list")
	var optPlatforms FlagStringSlice
	cmdFlags.Var(&optPlatforms, "platform", "target platform")
	cmdFlags.Usage = func() { c.Ui.Error(c.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		c.Ui.Error(fmt.Sprintf("Error parsing command-line flags: %s\n", err.Error()))
		return 1
	}

	var diags tfdiags.Diagnostics

	args = cmdFlags.Args()
	if len(args) != 1 {
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"No provider specified",
			"The providers list command requires a provider source as a command-line argument.",
		))
		view.Diagnostics(diags)
		return 1
	}

	addr, err := tfaddr.ParseProviderSource(args[0])
	if err != nil {
		diags = diags.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Invalid provider",
			fmt.Sprintf("Error parsing provider source: %s\n", err.Error()),
		))
		view.Diagnostics(diags)
		return 1
	}

	var platforms []getproviders.Platform
	if len(optPlatforms) == 0 {
		platforms = []getproviders.Platform{getproviders.CurrentPlatform}
	} else {
		platforms = make([]getproviders.Platform, 0, len(optPlatforms))
		for _, platformStr := range optPlatforms {
			platform, err := getproviders.ParsePlatform(platformStr)
			if err != nil {
				diags = diags.Append(tfdiags.Sourceless(
					tfdiags.Error,
					"Invalid target platform",
					fmt.Sprintf("The string %q given in the -platform option is not a valid target platform: %s.", platformStr, err),
				))
				continue
			}
			platforms = append(platforms, platform)
		}
	}

	ctx, cancel := c.InterruptibleContext()
	defer cancel()

	// Unlike other commands, this command always consults the origin registry
	// for every provider so that it can be used to update a local mirror
	// directory without needing to first disable that local mirror
	// in the CLI configuration.
	source := getproviders.NewMemoizeSource(
		getproviders.NewRegistrySource(c.Services),
	)

	avail, _, err := source.AvailableVersions(ctx, addr)
	if err != nil {
		diags = diags.Append(err)
		view.Diagnostics(diags)
		return 1
	}

	avail.Sort()
	bytes, err := json.Marshal(avail)
	if err != nil {
		diags = diags.Append(err)
		view.Diagnostics(diags)
		return 1
	}

	view.Result(bytes)

	return 0
}

func (c *ProvidersListCommand) Help() string {
	return `
Usage: terraform [global options] providers list [options] <provider>

  Lists all available versions of a provider

Options:

  -platform=os_arch  Choose which target platform to build a mirror for.
                     By default Terraform will obtain plugin packages
                     suitable for the platform where you run this command.
                     Use this flag multiple times to include packages for
                     multiple target systems.

                     Target names consist of an operating system and a CPU
                     architecture. For example, "linux_amd64" selects the
                     Linux operating system running on an AMD64 or x86_64
                     CPU. Each provider is available only for a limited
                     set of target platforms.
`
}
