#!/usr/bin/env bash
# Install Vault's dependencies inside a Linux container (Vault does not
# install on a Mac). Run it from the Mac and feed it on stdin, since the
# container mounts only the Vault clone, with the token passed by name
# only, so that it stays out of the Mac's shell history and `ps`:
#
#   GH_TOKEN="$(gh auth token)" docker exec -i -e GH_TOKEN -e WORKTREE=<worktree> \
#       <container> bash -s < <this script>
#
# bash reads the rest of the script from that same stdin, so the
# installers below get </dev/null and cannot swallow it.
# The container mounts the Vault clone at its host path, with the
# worktree's node_modules in a named volume. The token needs the
# read:packages scope. It is written only to the container's own home
# (~/.npmrc and a global git insteadOf for the private git dependencies),
# never to the mounted clone, and both are removed when the script exits.
# Inside the container, git sees it in its arguments while yarn fetches.
# YARN_FLAGS replaces --frozen-lockfile: add -e YARN_FLAGS=" " to the
# docker exec line to let yarn update yarn.lock after a dependency change.
set -euo pipefail
: "${GH_TOKEN:?GH_TOKEN missing}"
: "${WORKTREE:?WORKTREE missing}"
export DEBIAN_FRONTEND=noninteractive
if ! dpkg -s libsnappy-dev >/dev/null 2>&1; then
    apt-get update -qq </dev/null
    apt-get install -y -qq libsnappy-dev </dev/null >/dev/null
fi
command -v tsc >/dev/null || npm install -g --silent typescript@4.9.5 </dev/null
cleanup() {
    rm -f ~/.npmrc
    # Every token's rewrite, not only this one: git takes the first of two
    # equal insteadOf matches, so an old token would win over a new one.
    git config --global --get-regexp '^url\.https://x-access-token:' 2>/dev/null \
        | sed 's/\.insteadof .*//' | sort -u \
        | while read -r s; do git config --global --remove-section "$s"; done || true
}
trap cleanup EXIT
# A run killed before its trap leaves the token behind: start clean.
cleanup
umask 077
printf '@scality:registry=https://npm.pkg.github.com\n//npm.pkg.github.com/:_authToken=%s\n' "$GH_TOKEN" > ~/.npmrc
git config --global url."https://x-access-token:${GH_TOKEN}@github.com/".insteadOf "git@github.com:"
git config --global --add url."https://x-access-token:${GH_TOKEN}@github.com/".insteadOf "ssh://git@github.com/"
git config --global --add safe.directory '*'
cd "$WORKTREE"
yarn install ${YARN_FLAGS:---frozen-lockfile} --network-concurrency 1 --non-interactive </dev/null 2>&1 \
    | grep -vE '^(warning|info) ' | perl -pe 's/\Q$ENV{GH_TOKEN}\E/***/g' | tail -15
exit "${PIPESTATUS[0]}"
