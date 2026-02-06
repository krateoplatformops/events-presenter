#!/bin/bash

set -euo pipefail

# directory dello script
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MOD_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

KO_DOCKER_REPO=kind.local ko build --base-import-paths  "$MOD_DIR"

kubectl apply -f "$SCRIPT_DIR/presenter.yaml"
