#!/usr/bin/env bash
# What is running, on which ports, which topics exist, which consumer
# groups exist and how far behind they are, whether prometheus is actually
# scraping, and the URLs. Read-only: it starts and changes nothing.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
load_env

say "project ${PROJECT}, PORT_OFFSET ${PORT_OFFSET}"

say "containers"
if docker ps -a --filter "label=com.docker.compose.project=${PROJECT}" --format '{{.Names}}' | grep -q .; then
    docker ps -a --filter "label=com.docker.compose.project=${PROJECT}" \
        --format '    {{.Names}}\t{{.Status}}\t{{.Image}}' | expand -t 34,58
else
    warn "nothing running. bin/stack-up.sh"
    exit 0
fi

say "published ports"
docker ps --filter "label=com.docker.compose.project=${PROJECT}" \
    --format '{{.Names}}\t{{.Ports}}' \
    | sed 's/, \[::\][^,]*//g; s/0\.0\.0\.0://g' \
    | awk -F'\t' '{printf "    %-32s %s\n", $1, $2}'

say "kafka topics"
if kafka_cli kafka-topics.sh --list >/dev/null 2>&1; then
    internal="$(kafka_cli kafka-topics.sh --list 2>/dev/null | tr -d '\r' | grep -c '^_' || true)"
    [ "${internal:-0}" -gt 0 ] && info "$internal internal topic(s) hidden (names starting with _)"
    for t in $(kafka_cli kafka-topics.sh --list 2>/dev/null | tr -d '\r' | grep -v '^_' | sort); do
        parts="$(kafka_cli kafka-topics.sh --describe --topic "$t" 2>/dev/null \
            | grep -c 'Partition:' || true)"
        ends="$(kafka_cli kafka-get-offsets.sh --topic "$t" 2>/dev/null \
            | awk -F: '{print $3}' | paste -sd/ - || true)"
        printf '    %-38s P=%-3s end offsets %s\n' "$t" "$parts" "${ends:-?}"
    done
else
    warn "broker not answering on localhost:${KAFKA_PORT}"
fi

say "consumer groups"
groups="$(kafka_cli kafka-consumer-groups.sh --list 2>/dev/null | tr -d '\r' | grep -v '^$' | sort || true)"
if [ -z "$groups" ]; then
    info "none yet"
else
    for g in $groups; do
        # total lag and whether any partition has no committed offset,
        # which is what a wedged consumer looks like
        summary="$(kafka_cli kafka-consumer-groups.sh --describe --group "$g" 2>/dev/null \
            | awk 'NR>1 && NF>=6 {
                     parts++
                     if ($4 == "-") uncommitted++
                     else if ($6 ~ /^[0-9]+$/) lag += $6
                   }
                   END { printf "%d partitions, lag %d%s", parts, lag,
                          (uncommitted ? ", " uncommitted " with NO committed offset" : "") }')"
        printf '    %-52s %s\n' "$g" "$summary"
    done
    info 'a group holding partitions with no committed offset and a lag that stops falling is the wedge, not progress'
fi

say "prometheus targets"
if curl -fsS "http://localhost:${PROMETHEUS_PORT}/api/v1/targets?state=any" >/dev/null 2>&1; then
    curl -fsS "http://localhost:${PROMETHEUS_PORT}/api/v1/targets?state=any" \
      | python3 -c '
import json, sys, collections
d = json.load(sys.stdin)["data"]["activeTargets"]
by = collections.defaultdict(lambda: [0, 0, []])
for t in d:
    job = t["labels"].get("job", "?")
    up = t["health"] == "up"
    by[job][0] += 1
    by[job][1] += 1 if up else 0
    if not up:
        by[job][2].append(t["labels"].get("instance", "?"))
for job in sorted(by):
    total, up, down = by[job]
    line = "    %-28s %d/%d up" % (job, up, total)
    if down:
        line += "   down: " + ", ".join(sorted(down)[:6])
        if len(down) > 6:
            line += " (+%d)" % (len(down) - 6)
    print(line)
'
    info "delivery-workers targets that are down simply have no worker process on that port"
else
    warn "prometheus not answering on localhost:${PROMETHEUS_PORT}"
fi

say "grafana dashboards"
if curl -fsS "http://localhost:${GRAFANA_PORT}/api/search?type=dash-db" >/dev/null 2>&1; then
    curl -fsS "http://localhost:${GRAFANA_PORT}/api/search?type=dash-db" \
      | python3 -c '
import json, sys
for d in json.load(sys.stdin):
    print("    %-30s %s" % (d.get("title", "?"), d.get("url", "")))
' || warn "could not list dashboards"
else
    warn "grafana not answering on localhost:${GRAFANA_PORT}"
fi

say "cloudserver"
if curl -sS -o /dev/null -D - --max-time 5 "http://localhost:${CLOUDSERVER_PORT}/" 2>/dev/null \
        | grep -qi '^x-amz-request-id'; then
    info "answering on http://localhost:${CLOUDSERVER_PORT}  (accessKey1 / verySecretKey1)"
    info "the port is published by cloudserver-net, which owns its network namespace"
    dests="$(python3 -c '
import json, sys
try:
    c = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
print(" ".join(d["resource"] for d in c.get("bucketNotificationDestinations", [])))
' "$DEMO_DIR/cloudserver/config.json" 2>/dev/null || true)"
    [ -n "$dests" ] && info "notification destinations: $dests"
else
    warn "not answering on localhost:${CLOUDSERVER_PORT}"
fi

say "zookeeper contents"
info "browser http://localhost:${ZOONAV_PORT}  (auto-connects to zookeeper:2181)"
wg="$(zk_cli get "$ZK_WORKGROUPS_PATH" 2>/dev/null | grep -c '"generation"' || true)"
if [ "${wg:-0}" -gt 0 ]; then
    gen="$(zk_cli get "$ZK_WORKGROUPS_PATH" 2>/dev/null \
        | python3 -c '
import json, sys
for line in sys.stdin:
    line = line.strip()
    if line.startswith("{"):
        try:
            d = json.loads(line)
        except ValueError:
            continue
        print("generation %s, %d workgroup(s): %s" % (
            d.get("generation"), len(d.get("workgroups", [])),
            ", ".join(w.get("id", "?") for w in d.get("workgroups", []))))
        break
' 2>/dev/null || true)"
    info "workgroups document at ${ZK_WORKGROUPS_PATH}: ${gen:-present}"
else
    info "workgroups document at ${ZK_WORKGROUPS_PATH}: not written yet"
fi
info "bin/zk-show.sh prints the document and the populator offsets"

say "kerberos destination broker"
if docker ps --format '{{.Names}}' | grep -q "^${PROJECT}-krb-kafka-1$"; then
    if docker exec -i "${PROJECT}-krb-kafka-1" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "localhost:${KRB_VERIFY_PORT}" --list 2>/dev/null | sed 's/^/    topic /'; then
        info "SASL_GSSAPI on localhost:${KRB_BROKER_PORT}, plaintext readback on localhost:${KRB_VERIFY_PORT}"
    fi
    ls "$DEMO_DIR/krb/keytabs"/*.keytab >/dev/null 2>&1 \
        && info "keytabs: $(cd "$DEMO_DIR/krb/keytabs" && echo *.keytab)"
else
    info "not running (bin/stack-up.sh --krb)"
fi

say "URLs and endpoints"
urls
