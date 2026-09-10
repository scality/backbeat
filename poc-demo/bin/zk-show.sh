#!/usr/bin/env bash
# Print what ZooKeeper holds for the demo, for the terminal pane next to
# the browser: the workgroups document pretty-printed, the generation it
# declares, the consumer group each workgroup therefore joins, and the
# populator's log offsets.
#
#   zk-show.sh              the workgroups document and the populator offsets
#   zk-show.sh <znode>      any other node, pretty-printed if it is JSON
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
load_env

# zkCli wraps its answer in log lines. The data is the last line that is
# not a log line, so keep lines that do not look like one.
zk_get() {
    zk_cli get "$1" | grep -vE '^[0-9]{4}-[0-9]{2}-[0-9]{2} |^WATCHER|^WatchedEvent|^$|^Connecting to |^JLine support|^\[zk:' || true
}

zk_ls_r() {
    zk_cli ls -R "$1" | grep -E "^${1}" || true
}

pretty() {
    python3 -c '
import json, sys
raw = sys.stdin.read().strip()
if not raw:
    print("    (node has no data)")
    sys.exit(0)
try:
    print("\n".join("    " + l for l in json.dumps(json.loads(raw), indent=2).splitlines()))
except ValueError:
    print("    " + raw)
'
}

if [ "$#" -ge 1 ]; then
    say "$1"
    zk_get "$1" | pretty
    exit 0
fi

say "workgroups document at ${ZK_WORKGROUPS_PATH}"
doc="$(zk_get "$ZK_WORKGROUPS_PATH")"
if [ -z "$doc" ]; then
    warn "no document there yet. The workers read it at startup, and the"
    warn "cutover tool writes it. Check the workgroups.zookeeperPath in the"
    warn "backbeat config the workers are started with, and remember the"
    warn "path is relative to any chroot on the zookeeper connection string."
else
    printf '%s' "$doc" | pretty
    printf '%s' "$doc" | python3 -c '
import json, os, sys
try:
    d = json.load(sys.stdin)
except ValueError:
    sys.exit(0)
base = os.environ.get("DELIVERY_GROUP_BASE", "bucket-notification-delivery-group")
gen = d.get("generation")
print()
print("==> generation %s, configVersion %s, topic %s" % (gen, d.get("configVersion"), d.get("topic")))
print("==> workgroups and the consumer group each one joins")
for wg in d.get("workgroups", []):
    rule = wg.get("rule", {})
    if rule.get("type") == "hashmod":
        shape = "hashmod modulo %s remainders %s" % (rule.get("modulo"), rule.get("remainders"))
    else:
        shape = "static %s" % ", ".join(rule.get("destinationIds") or [])
    print("    %-16s %-44s %s-%s-gen%s" % (wg.get("id"), shape, base, wg.get("id"), gen))
prev = d.get("previousGroups") or []
if prev:
    print("==> previous generation groups, written by the cutover tool")
    for g in prev:
        print("    %s" % g)
barriers = d.get("barriers") or {}
if barriers:
    print("==> cutover barriers, partition to offset")
    for k in sorted(barriers, key=lambda x: int(x)):
        print("    partition %-3s offset %s" % (k, barriers[k]))
'
fi

say "populator log offsets under ${ZK_POPULATOR_PATH}"
found=0
for node in $(zk_ls_r "$ZK_POPULATOR_PATH"); do
    case "$node" in
        *logOffset)
            printf '    %s\n' "$node"
            zk_get "$node" | pretty
            found=1
            ;;
    esac
done
[ "$found" -eq 0 ] && info "no logOffset node yet; the populator writes one once it has read a batch"

say "browse the whole tree at http://localhost:${ZOONAV_PORT}"
