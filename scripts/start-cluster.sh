#!/usr/bin/env bash
# Builds the project and starts a local 5-node cluster (one JVM per node). Ctrl-C stops all of them.
#   ./scripts/start-cluster.sh            keep existing data in data/
#   ./scripts/start-cluster.sh --fresh    wipe data first
set -euo pipefail
cd "$(dirname "$0")/.."
./gradlew -q installDist
exec build/install/raft-kv/bin/raft-kv cluster "$@"
