#!/usr/bin/env bash
# Image test: verify a backbeat image is Kerberos-capable.
#
# Checks inside the given image:
#   1. node-rdkafka compiled with sasl_gssapi;
#   2. kinit present (used to obtain tickets);
#   3. Cyrus SASL GSSAPI plugin present (loaded at connect time).
#
# Usage: check_image_kerberos_support.sh <image-ref>
set -euo pipefail

IMAGE="$1"

echo "checking Kerberos support in ${IMAGE}"
docker run --rm --entrypoint /bin/bash "${IMAGE}" -c '
    set -e
    node -e "
        const f = require(\"node-rdkafka\").features;
        console.log(\"node-rdkafka features: \" + f.join(\", \"));
        process.exit(f.includes(\"sasl_gssapi\") ? 0 : 1);
    "
    command -v kinit
    ls /usr/lib/*/sasl2/ | grep -i gssapi
'
echo "OK: ${IMAGE} is Kerberos-capable"
