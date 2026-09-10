#!/usr/bin/env bash
# Build the image act 07 runs the kerberos functional suite inside, and seed
# its node_modules volume once. Setup time, not demo time: it is called from
# stack-up.sh --krb, so a fresh machine has it ready before the recording and
# a plain `yarn demo:up` never pays for it.
#
# Act 07 (tests/functional/demo/acts/07-kerberos.js) expects:
#   - an image  backbeat-krbtest:spike   (KRB_TEST_IMAGE)
#   - a volume  backbeat-krb-nm          (KRB_NODE_MODULES_VOLUME), holding a
#     Linux node_modules built from THIS repository, mounted over the repo's
#     own node_modules so the native GSSAPI modules match the container.
#
# Both are created here. Re-running is cheap: the image layer cache makes the
# build a no-op, and the install is skipped when the volume already has a
# node_modules.
#
#   krb-test-image.sh            build the image and seed the volume if empty
#   krb-test-image.sh --reinstall  force the yarn install into the volume
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

IMAGE="${KRB_TEST_IMAGE:-backbeat-krbtest:spike}"
VOLUME="${KRB_NODE_MODULES_VOLUME:-backbeat-krb-nm}"
DOCKERFILE="$DEMO_DIR/krb/Dockerfile.krbtest"
REINSTALL=0
[ "${1:-}" = "--reinstall" ] && REINSTALL=1

[ -f "$DOCKERFILE" ] || die "missing $DOCKERFILE"
need_backbeat

step "kerberos test image"
if docker image inspect "$IMAGE" >/dev/null 2>&1; then
    say "$IMAGE present (delete it to force a rebuild)"
else
    say "building $IMAGE from krb/Dockerfile.krbtest (one-time, needs network)"
    docker build -t "$IMAGE" -f "$DOCKERFILE" "$DEMO_DIR/krb" \
        || die "could not build $IMAGE"
fi

# Is there already a node_modules in the volume? A fresh named volume is
# empty, and the install is the slow part, so only do it when needed.
have_nm=0
if docker volume inspect "$VOLUME" >/dev/null 2>&1; then
    if docker run --rm -v "$VOLUME:/nm" "$IMAGE" \
        sh -c 'test -d /nm/node-rdkafka && test -d /nm/kerberos' >/dev/null 2>&1; then
        have_nm=1
    fi
fi

if [ "$have_nm" -eq 1 ] && [ "$REINSTALL" -eq 0 ]; then
    say "$VOLUME already carries a built node_modules; skipping install"
    say "  (krb-test-image.sh --reinstall rebuilds it)"
    exit 0
fi

say "installing node_modules into $VOLUME from $BACKBEAT_DIR (a few minutes)"
docker run --rm \
    -v "$BACKBEAT_DIR:/usr/src/app" \
    -v "$VOLUME:/usr/src/app/node_modules" \
    -w /usr/src/app \
    "$IMAGE" \
    bash -lc 'yarn install --frozen-lockfile' \
    || die "yarn install into $VOLUME failed. See the output above."
say "done: $IMAGE built, $VOLUME seeded. Act 07 can run on the krb profile."
