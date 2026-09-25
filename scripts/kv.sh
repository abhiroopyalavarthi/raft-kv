#!/usr/bin/env bash
# Client for the local cluster:  ./scripts/kv.sh put k v | get k | delete k | status   (no args: shell)
set -euo pipefail
cd "$(dirname "$0")/.."
if [ ! -x build/install/raft-kv/bin/raft-kv ]; then ./gradlew -q installDist; fi
exec build/install/raft-kv/bin/raft-kv client "$@"
