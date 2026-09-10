#!/bin/bash
# Create a bucket and put its notification configuration, in one step.
#
# Usage:
#   bucket.sh create <bucket> <dest>[:<prefix>] [<dest>[:<prefix>] ...]
#   bucket.sh get <bucket>
#   bucket.sh config <bucket> <dest>[:<prefix>] ...     re-put only the config
#
# A destination token is a destination id, optionally with a key prefix
# filter, which is how the overlapping-rules scenario builds a catch-all rule
# plus a prefix rule. A token that starts with "arn:" is used verbatim, which
# is how the name-collision scenario puts an account-scoped ARN.
#
# Rule ids are "<dest>-all" or "<dest>-<prefix>", and the rule id is what
# arrives in the delivered event as s3.configurationId, so the demo can show
# which rule produced an event.
#
# Note measured on the rig: CloudServer's filter rule names are case
# sensitive ("Prefix", not "prefix"), while the matcher compares
# case-insensitively.
set -u
. "$(dirname "$0")/lib.sh"

CMD=${1:-}
BUCKET=${2:-}
shift 2 2>/dev/null || true

aws_s3api() { aws --endpoint-url "$S3" s3api "$@"; }

build_config() {
    python3 - "$@" <<'PY'
import json, sys
rules = []
for tok in sys.argv[1:]:
    if tok.startswith('arn:'):
        arn, prefix, rid = tok, None, tok.split(':')[-1] + '-verbatim'
    else:
        dest, _, prefix = tok.partition(':')
        arn = 'arn:scality:bucketnotif:::%s' % dest
        rid = '%s-%s' % (dest, prefix.strip('/').replace('/', '-') if prefix else 'all')
    r = {
        'Id': rid,
        'QueueArn': arn,
        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*'],
    }
    if prefix:
        r['Filter'] = {'Key': {'FilterRules': [{'Name': 'Prefix', 'Value': prefix}]}}
    rules.append(r)
print(json.dumps({'QueueConfigurations': rules}, indent=2))
PY
}

case "$CMD" in
create|config)
    [ -n "$BUCKET" ] || die "usage: bucket.sh create <bucket> <dest>[:<prefix>] ..."
    [ $# -gt 0 ] || die "at least one destination is needed"
    need_conf
    TMP=$DEMO/run/notif-$BUCKET.json
    build_config "$@" > "$TMP"
    if [ "$CMD" = create ]; then
        step "creating bucket $BUCKET on $S3"
        aws_s3api create-bucket --bucket "$BUCKET" >/dev/null 2>&1 \
            && say "bucket created" || say "bucket already exists (or create failed, see get below)"
    fi
    step "putting the notification configuration on $BUCKET"
    say "rules: $*"
    say "body:  $TMP"
    if ! aws_s3api put-bucket-notification-configuration --bucket "$BUCKET" \
            --notification-configuration "file://$TMP"; then
        warn "PUT refused. CloudServer validates every ARN's last segment against"
        warn "bucketNotificationDestinations in $GENERATED/cloudserver-config.json,"
        warn "read once at startup, so a new destination needs a CloudServer restart."
        exit 1
    fi
    say "accepted"
    step "reading it back"
    aws_s3api get-bucket-notification-configuration --bucket "$BUCKET"
    ;;
get)
    [ -n "$BUCKET" ] || die "usage: bucket.sh get <bucket>"
    aws_s3api get-bucket-notification-configuration --bucket "$BUCKET"
    ;;
*) die "usage: bucket.sh create <bucket> <dest>[:<prefix>] ... | config <bucket> ... | get <bucket>" ;;
esac
