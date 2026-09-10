#!/usr/bin/env bash
# `yarn install` for the demo, with the one workaround a fresh macOS needs.
#
# node-gyp (pulled in transitively by fcntl, an arsenal file-backend module
# the demo never touches) imports distutils, which Python removed in 3.12. On
# a machine whose python3 is 3.12+ without setuptools, the install prints a
# gyp failure on that one module. Everything the demo needs, node-rdkafka
# included, still builds, so the failure is harmless, but it is noisy and
# looks fatal. When setuptools is missing this script makes a throwaway venv
# with setuptools and points node-gyp at it, so the install is clean.
#
# Run from the backbeat repository root, or pass no arguments and let it find
# it from this script's location.
#
#   poc-demo/bin/install.sh
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need_node
need_backbeat

cd "$BACKBEAT_DIR" || die "cannot enter $BACKBEAT_DIR"
export PATH="$NODE_BIN:$PATH"

gyp_python=""
if command -v python3 >/dev/null 2>&1 \
   && ! python3 -c 'import distutils' >/dev/null 2>&1 \
   && ! python3 -c 'import setuptools' >/dev/null 2>&1; then
    venv="$(mktemp -d)/gyp-venv"
    say "python3 has no distutils/setuptools; making a throwaway venv for node-gyp"
    python3 -m venv "$venv"
    # shellcheck disable=SC1091
    "$venv/bin/pip" install --quiet --upgrade pip setuptools >/dev/null 2>&1 \
        || warn "could not install setuptools into the venv; the install may be noisy"
    gyp_python="$venv/bin/python"
    info "node-gyp will use $gyp_python"
fi

say "yarn install --frozen-lockfile in $BACKBEAT_DIR"
if [ -n "$gyp_python" ]; then
    npm_config_python="$gyp_python" PYTHON="$gyp_python" \
        yarn install --frozen-lockfile
else
    yarn install --frozen-lockfile
fi
say "install done. Next: yarn demo:preflight, then yarn demo:up."
