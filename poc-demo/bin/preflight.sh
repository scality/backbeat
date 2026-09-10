#!/usr/bin/env bash
# One check before a demo on a fresh machine: is Docker running, is there a
# node 22 on PATH, are the ports this stack wants free at the chosen
# PORT_OFFSET, and can node-gyp find a usable Python. Prints what is wrong and
# how to fix it, exits non-zero if anything blocks a run. Changes nothing.
#
#   preflight.sh
set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
load_env >/dev/null

problems=0
note_ok()   { printf '    ok    %s\n' "$*"; }
note_bad()  { printf '!!  FAIL  %s\n' "$*" >&2; problems=$((problems + 1)); }
note_warn() { printf '    warn  %s\n' "$*"; }

say "BNaaS demo preflight (project ${PROJECT}, PORT_OFFSET ${PORT_OFFSET})"

# ---- docker ----
if docker info >/dev/null 2>&1; then
    note_ok "docker daemon is running"
else
    note_bad "docker is not running. Start Docker Desktop and re-run."
fi

# ---- node 22 ----
if command -v node >/dev/null 2>&1; then
    v="$(node --version 2>/dev/null)"
    case "$v" in
        v22.*) note_ok "node $v on PATH" ;;
        v2[3-9].*|v[3-9][0-9].*) note_bad "node $v on PATH; the native modules need node 22. Use nvm: nvm install 22 && nvm use 22" ;;
        *) note_warn "node $v on PATH; this POC was built on node 22. If native modules fail, switch to 22." ;;
    esac
else
    note_bad "no node on PATH. Install node 22 (nvm install 22)."
fi

# ---- ports ----
busy=""
for name in "${PORT_NAMES[@]}"; do
    port="${!name:-}"
    [ -n "$port" ] || continue
    # krb ports only matter with the krb profile; check them anyway but do
    # not fail on them, since a plain demo does not use them.
    if port_busy "$port"; then
        owner="$(port_owner "$port" || true)"
        if [ -n "$owner" ] && [[ "$owner" == "${PROJECT}"* ]]; then
            note_ok "$name $port held by our own $owner (a stack is already up)"
        else
            busy="$busy $name=$port(${owner:-host process})"
        fi
    fi
done
if [ -n "$busy" ]; then
    note_bad "ports in use by something that is not this stack:$busy"
    note_warn "raise PORT_OFFSET in poc-demo/.env (e.g. to 1000) and re-run, or stop the owner"
else
    note_ok "every port this stack wants is free at PORT_OFFSET=${PORT_OFFSET}"
fi

# ---- python for node-gyp ----
# node-gyp's older versions import distutils, which Python removed in 3.12.
# The demo does not need the module that fails to build (arsenal's fcntl file
# backend), so this is a warning, not a blocker, but the fix is worth naming.
if command -v python3 >/dev/null 2>&1; then
    pyv="$(python3 -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null)"
    if python3 -c 'import distutils' >/dev/null 2>&1; then
        note_ok "python3 $pyv has distutils; node-gyp is happy"
    elif python3 -c 'import setuptools' >/dev/null 2>&1; then
        note_ok "python3 $pyv has setuptools (distutils shim); node-gyp is happy"
    else
        note_warn "python3 $pyv has no distutils/setuptools: one native module (fcntl) will fail to build."
        note_warn "  the demo does not use it, so this is harmless. For a clean install run poc-demo/bin/install.sh,"
        note_warn "  which points node-gyp at a venv with setuptools."
    fi
else
    note_warn "no python3 found; node-gyp may fail on one unused native module (harmless for the demo)"
fi

echo
if [ "$problems" -eq 0 ]; then
    say "preflight passed. Next: yarn demo:up (or yarn demo:up:krb), then yarn demo:wait."
    exit 0
fi
die "preflight found ${problems} blocking issue(s). Fix them and re-run."
