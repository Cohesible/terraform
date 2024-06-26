#!/usr/bin/env bash
# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

set -uo pipefail

# Trim the "v" prefix, if any.
VERSION="${RAW_VERSION#v}"

# Split off the build metadata part, if any
# (we won't actually include it in our final version, and handle it only for
# compleness against semver syntax.)
IFS='+' read -ra VERSION BUILD_META <<< "$VERSION"

# Separate out the prerelease part, if any
# (version.go expects it to be in a separate variable)
IFS='-' read -r BASE_VERSION PRERELEASE <<< "$VERSION"

LDFLAGS="-w -s"

LDFLAGS="${LDFLAGS} -X 'github.com/hashicorp/terraform/version.Version=${BASE_VERSION}'"
LDFLAGS="${LDFLAGS} -X 'github.com/hashicorp/terraform/version.Prerelease=${PRERELEASE}'"

echo "Building Terraform CLI ${VERSION}"
echo "product-version=${VERSION}" | tee -a "${GITHUB_OUTPUT}"
echo "product-version-base=${BASE_VERSION}" | tee -a "${GITHUB_OUTPUT}"
echo "product-version-pre=${PRERELEASE}" | tee -a "${GITHUB_OUTPUT}"
echo "go-ldflags=${LDFLAGS}" | tee -a "${GITHUB_OUTPUT}"