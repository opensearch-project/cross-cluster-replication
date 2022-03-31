#!/bin/bash

# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

set -e
DIR="$(dirname "$0")"

if [ -z "$1" ]; then (echo "syntax: run.sh --bundle-manifest path_to_manifest --config path_to_config"; exit -1); fi
command -v python3 >/dev/null 2>&1 || (echo "missing python3"; exit -1)

# Setup venv and install dependencies.
cd "$DIR/../perf_workflow"
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run the test suite
python "run_perf_suite/run_perf_suite.py" "${@:1}"
deactivate
