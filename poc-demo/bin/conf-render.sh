#!/bin/bash
# Render every config the demo needs into demo/conf/generated, from the
# templates in demo/conf/templates and the ports in demo/.env, and make sure
# the preload shims are in demo/conf/shims.
#
# A thin wrapper: the renderer is the test suite's own module, so a config an
# operator starts a process with and a config an act starts one with cannot
# drift.
#
# Usage: conf-render.sh [--quiet]
set -u
. "$(dirname "$0")/lib.sh"
need_node
need_backbeat

mkdir -p "$SHIM_DIR"
for s in oplog-h-shim.js oplog-v2diff-shim.js kafka-metadata-refresh-shim.js pidfile-shim.js; do
    [ -f "$SHIM_DIR/$s" ] && continue
    if [ -f "$DEMO/../rig/conf/$s" ]; then
        cp "$DEMO/../rig/conf/$s" "$SHIM_DIR/$s"
        say "shim copied from the rig: $SHIM_DIR/$s"
    else
        warn "missing shim $SHIM_DIR/$s"
    fi
done

exec "$NODE" "$SUITE/lib/render-cli.js" "$@"
