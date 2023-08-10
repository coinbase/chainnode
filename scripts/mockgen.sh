#!/usr/bin/env bash

set -eo pipefail

# Read the mocks.yaml file and run mockgen for each item
packges=$(yq e ".gomocks[].package" mocks.yml)
for package in $packges; do
    interfaces=$(yq e ".gomocks[] | select(.package == \"${package}\") | .interfaces | join(\",\")" mocks.yml)
    echo "Running mockgen for $package"
    mockgen -destination "${package}/mocks/mocks.go" -package "${package##*/}mocks" "github.com/coinbase/chainnode/${package}" "${interfaces}"
done
