#!/usr/bin/env python3
"""Keep the Federation epic pinned to the CI images of the service epics.

Usage:
  pins.py [--federation <path>] [--epic <branch>] [--kms <version>]
          [--dry-run | --apply] [--only cloudserver|backbeat|vault|kms] [--json]
  pins.py --self-test

For cloudserver, backbeat and vault, the script reads the head of the
epic branch on GitHub through gh (scality/Vault is private), checks that the CI image of that head exists on
ghcr.io, and compares it with the pin in group_vars/all of the Federation
epic, as `git show origin/<epic>:group_vars/all` gives it in the
Federation checkout, so fetch the epic first. Dry run is the default and
changes nothing. With --apply it rewrites, in the working tree, only the
pins whose image exists, keeps every other byte of the file as it is, and
prints the unified diff; --only limits the rewrite to one service, so
that each service gets its own commit with a plain `git add`. The action
table still compares with the epic,
and the write sets every pin whose image exists to the epic head, even
one the epic already has. It never commits. --apply refuses unless the
checkout is on a pin branch, improvement/<ticket>-epic-image-pins or the
CI branch improvement/<ticket>-epic-image-pins-bot. No scality-kms image
exists per sha, so --kms pins that one to a released tag instead. The
epic defaults to the remote branch epic/*-bnaas-d1 of the Federation
checkout. The image lookup uses `docker buildx imagetools inspect`, so
`docker login ghcr.io` must have been done. The exit code is 1 when any
row is an error: a failed lookup other than a missing epic image, a
missing --kms tag, or a pin absent from group_vars/all. A stop before
the survey (no epic ref, no docker or gh) exits 2, and in --json mode
still prints {"epic": null, "rows": [], "error": "<message>"}.
"""
import argparse
import difflib
import json
import os
import re
import shutil
import subprocess
import sys

# Each service: repository, image, tag suffix, and the pin's place in
# group_vars/all: a top-level key (block None) or a key under `  <block>:`.
SERVICES = [
    ("cloudserver", "scality/cloudserver", "ghcr.io/scality/cloudserver",
     "-federation", None, "cloudserver_image_version"),
    ("backbeat", "scality/backbeat", "ghcr.io/scality/backbeat",
     "-federation", "backbeat", "og_docker_image_version"),
    ("vault", "scality/Vault", "ghcr.io/scality/vault",
     "", "vault", "og_docker_image_version"),
]
GROUP_VARS = os.path.join("group_vars", "all")
KMS = ("kms", "og_docker_image_version")
# The only branches that --apply writes on.
PIN_BRANCH = re.compile(r"improvement/[A-Z][A-Z0-9]*-[0-9]+-epic-image-pins(-bot)?")
# A YAML value after its key: single-quoted, double-quoted (a Jinja
# reference often is) or bare. Group 1 is the colon and the blanks.
VALUE = r""":(\s*)(?:'([^']*)'|"([^"]*)"|([^\s#'"]+))"""


# In --json mode an early stop still prints JSON, so that what's next can
# read pins.json and leave the pins row out.
AS_JSON = False


def fail(message, code=2):
    """Stop with message: on stderr, and in --json mode as JSON on stdout."""
    if AS_JSON:
        print(json.dumps({"epic": None, "rows": [], "error": message}))
    print(message, file=sys.stderr)
    sys.exit(code)


def run(cmd):
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0")
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    return proc.returncode, proc.stdout, proc.stderr


def default_epic(federation):
    code, out, _ = run(["git", "-C", federation, "for-each-ref",
                        "--format=%(refname:short)",
                        "refs/remotes/origin/epic/*-bnaas-d1"])
    names = [n.split("/", 1)[1] for n in out.split() if "/" in n]
    if code != 0 or len(names) != 1:
        fail("error: cannot guess the epic branch, pass --epic")
    return names[0]


def epic_lines(federation, epic):
    """The lines of group_vars/all on the Federation epic, or exit."""
    ref = "origin/%s:group_vars/all" % epic
    code, out, err = run(["git", "-C", federation, "show", ref])
    if code != 0:
        fail("error: cannot read %s in %s: %s; fetch the epic first"
             % (ref, federation, err.strip()))
    return out.splitlines(keepends=True)


def check_pin_branch(federation):
    """Exit unless the checkout is on a pin branch."""
    code, out, _ = run(["git", "-C", federation, "symbolic-ref", "--short", "HEAD"])
    branch = out.strip() if code == 0 else "a detached HEAD"
    if not PIN_BRANCH.fullmatch(branch):
        fail("error: --apply writes only on a pin branch "
             "(improvement/<ticket>-epic-image-pins), and %s is on %s"
             % (federation, branch))


def epic_head(repo, epic):
    """The full sha of the epic head on GitHub, or None, and the stderr.
    Through gh, not anonymous git: scality/Vault is private, and gh uses
    the developer's login, or GH_TOKEN in CI."""
    code, out, err = run(["gh", "api", "repos/%s/commits/%s" % (repo, epic), "--jq", ".sha"])
    if code == 0 and re.fullmatch(r"[0-9a-f]{40}", out.strip()):
        return out.strip(), ""
    return None, err.strip() or "no branch %s in %s" % (epic, repo)


def image_state(ref):
    """exists, missing, or error, from the registry lookup of one image,
    and the stderr of that lookup."""
    code, _, err = run(["docker", "buildx", "imagetools", "inspect", ref])
    if code == 0:
        return "exists", ""
    # Only the registry's own answer means a missing image: a missing
    # credential helper also says "not found", about an executable.
    missing = err.strip().endswith(ref + ": not found")
    return ("missing" if missing else "error"), err.strip()


def find_pin(lines, block, key):
    """Index and value of the pin line, or (None, None) when absent. A
    block is a `  <block>:` under the top-level `defaults:` only, so a
    vault or kms block under another key is never taken for it."""
    top = current = None
    for i, line in enumerate(lines):
        if re.match(r"[^\s#]", line):
            top, current = line.split(":", 1)[0].strip(), None
        # A block header may carry a trailing comment.
        head = re.match(r"  (\S+):\s*(?:#.*)?$", line)
        if head:
            current = head.group(1) if top == "defaults" else None
        if current == block and line.startswith(
                ("" if block is None else "    ") + key + ":"):
            value = re.search(VALUE, line)
            if value:
                return i, next(g for g in value.groups()[1:] if g is not None)
    return None, None


def kms_warning(lines, rows):
    """Warn when kms follows cloudserver and has no image for that head."""
    _, value = find_pin(lines, *KMS)
    if value is None or "cloudserver_image_version" not in value:
        return None
    head = rows[0]["head"]
    if head is None:
        return None
    kms = "ghcr.io/scality/scality-kms:%s-federation" % head
    if image_state(kms)[0] == "exists":
        return None
    return ("scality-kms still follows cloudserver_image_version and %s "
            "does not exist, so the Federation build cannot pull it; "
            "pin kms to a released version in group_vars/all" % kms)


def survey(lines, epic, kms=None):
    """One row per service, plus a scality-kms row when a tag is given."""
    rows = []
    for name, repo, image, suffix, block, key in SERVICES:
        head, detail = epic_head(repo, epic)
        index, current = find_pin(lines, block, key)
        if index is None:
            detail = "no %s in %s" % (key, GROUP_VARS)
        row = {"repo": name, "current": current, "head": head, "image": None,
               "state": "error", "action": "error", "line": index, "detail": detail}
        if head and index is not None:
            row["image"] = "%s:%s%s" % (image, head, suffix)
            row["state"], row["detail"] = image_state(row["image"])
            wanted = head + suffix
            if row["state"] == "exists":
                row["action"] = "same" if current == wanted else "pin"
            elif row["state"] == "missing":
                row["action"] = "wait"
        rows.append(row)
    if kms:
        index, current = find_pin(lines, *KMS)
        image = "ghcr.io/scality/scality-kms:" + kms
        state, detail = (image_state(image) if index is not None
                         else ("error", "no kms og_docker_image_version in " + GROUP_VARS))
        # --kms names a released tag, which CI never publishes later: a
        # missing one is a typo or a wrong version, so an error, not a wait.
        action = {"exists": "same" if current == kms else "pin",
                  "missing": "error"}.get(state, "error")
        # The fixed tag stands where the other rows show their epic head.
        rows.append({"repo": "scality-kms", "current": current, "head": kms,
                     "image": image, "state": state, "action": action,
                     "line": index, "detail": detail})
    return rows


def apply_pins(lines, rows):
    """New file lines with each pin whose image exists set to that image,
    on its own line. The action comes from the epic, but the lines can be
    the pin branch's own file: a pin there that differs from the wanted
    tag is rewritten even when the epic is already current ("same"), or
    the PR would revert the epic's pin."""
    places = {name: (block, key) for name, _, _, _, block, key in SERVICES}
    places["scality-kms"] = KMS
    new = list(lines)
    for row in rows:
        if row["state"] != "exists":
            continue
        index, current = find_pin(new, *places[row["repo"]])
        tag = row["image"].split(":", 1)[1]
        if index is None or current == tag:
            continue
        new[index] = re.sub(VALUE, lambda m: ":%s'%s'" % (m.group(1), tag), new[index],
                            count=1)
    return new


def only_rows(rows, only):
    """The rows that --only keeps: all of them without it."""
    name = {"kms": "scality-kms"}.get(only, only)
    return [r for r in rows if not name or r["repo"] == name]


def self_test():
    """The line-based edit of group_vars/all, on a sample with decoy blocks
    before defaults:, trailing comments and a bare value."""
    sample = ("other:\n  vault:\n    og_docker_image_version: 'decoy'\n"
              "  kms:  # not ours\n    og_docker_image_version: 'decoykms'\n"
              "cloudserver_image_version: old-federation  # bare value\n"
              "defaults:\n  backbeat:\n    og_docker_image_version: 'oldbb-federation'\n"
              "  vault:  # identity\n    og_docker_image_version: \"oldv\"\n"
              "  kms:\n    og_docker_image_version: '9.4.4-federation'\n")
    lines = sample.splitlines(keepends=True)
    assert find_pin(lines, None, "cloudserver_image_version") == (5, "old-federation")
    assert find_pin(lines, "backbeat", "og_docker_image_version") == (8, "oldbb-federation")
    assert find_pin(lines, "vault", "og_docker_image_version") == (10, "oldv")
    assert find_pin(lines, *KMS) == (12, "9.4.4-federation")
    assert find_pin(lines, "nothere", "og_docker_image_version") == (None, None)
    head = "a" * 40
    rows = [{"repo": "cloudserver", "state": "exists",
             "image": "ghcr.io/scality/cloudserver:%s-federation" % head},
            {"repo": "backbeat", "state": "exists",
             "image": "ghcr.io/scality/backbeat:%s-federation" % head},
            {"repo": "vault", "state": "missing", "image": "ghcr.io/scality/vault:" + head},
            {"repo": "scality-kms", "state": "exists",
             "image": "ghcr.io/scality/scality-kms:9.4.5-federation"}]
    want = {"cloudserver": (5, head + "-federation"), "backbeat": (8, head + "-federation"),
            "vault": None, "kms": (12, "9.4.5-federation")}
    for only, change in want.items():
        new = apply_pins(lines, only_rows(rows, only))
        changed = [i for i, (a, b) in enumerate(zip(lines, new)) if a != b]
        if change is None:
            # A missing image is never written, so --only vault changes nothing.
            assert changed == [], (only, changed)
            continue
        index, value = change
        assert changed == [index], (only, changed)
        assert new[index].endswith("'%s'%s" % (value, "  # bare value\n" if index == 5 else "\n")), \
            new[index]
    # Every other byte stays: the decoys, the comments, the vault pin.
    new = apply_pins(lines, rows)
    assert [i for i, (a, b) in enumerate(zip(lines, new)) if a != b] == [5, 8, 12], new
    assert new[1:5] == lines[1:5] and new[9:11] == lines[9:11], new
    # A missing image is the registry's exact answer, nothing else.
    real_run = globals()["run"]
    try:
        for err, want in (("ERROR: ghcr.io/scality/vault:x: not found", "missing"),
                          ('error getting credentials - err: exec: "docker-credential-x": '
                           "executable file not found in $PATH", "error")):
            globals()["run"] = lambda cmd, err=err: (1, "", err)
            assert image_state("ghcr.io/scality/vault:x")[0] == want, (err, want)
    finally:
        globals()["run"] = real_run
    print("self-test ok")


def print_table(rows):
    cells = [("repo", "current pin", "epic head", "image", "action")]
    # Shorten the shas only: the scality-kms row shows its whole tag.
    cells += [(r["repo"], r["current"] or "?",
               r["head"] if r["repo"] == "scality-kms" else (r["head"] or "?")[:12],
               r["state"], r["action"]) for r in rows]
    widths = [max(len(c[i]) for c in cells) for i in range(5)]
    for cell in cells:
        print("  ".join(v.ljust(w) for v, w in zip(cell, widths)).rstrip())
    # The reason of each unknown head or failed lookup, as git or docker gave it.
    for r in rows:
        if r.get("detail") and (r["action"] == "error" or not r["head"]):
            print("%s: %s" % (r["repo"], r["detail"].splitlines()[-1]))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--federation", default=".",
                        help="a Federation checkout (default: current dir)")
    parser.add_argument("--epic", help="epic branch name in every repo")
    parser.add_argument("--kms", help="released scality-kms tag to pin")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", default=True,
                      help="report only, the default")
    mode.add_argument("--apply", action="store_true",
                      help="rewrite the pins in group_vars/all on a pin branch, no commit")
    parser.add_argument("--only", choices=("cloudserver", "backbeat", "vault", "kms"),
                        help="rewrite this one service's pin only")
    parser.add_argument("--json", action="store_true", dest="as_json")
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return
    args = parser.parse_args()
    global AS_JSON
    AS_JSON = args.as_json
    if args.only == "kms" and not args.kms:
        # No scality-kms row without a tag: the run would write nothing.
        parser.error("--only kms needs --kms <released tag>")

    for tool in ("docker", "gh"):
        if not shutil.which(tool):
            fail("error: %s is not installed or not on PATH; the lookups need it" % tool)
    epic = args.epic or default_epic(args.federation)
    if args.apply:
        check_pin_branch(args.federation)
    lines = epic_lines(args.federation, epic)
    rows = survey(lines, epic, args.kms)
    warning = None if args.kms else kms_warning(lines, rows)
    path = os.path.join(args.federation, GROUP_VARS)
    if args.apply:
        # The pin branch may hold pins the epic does not have yet: rewrite
        # its own file, not a copy of the epic's.
        try:
            with open(path, "rb") as handle:
                lines = handle.read().decode("utf-8").splitlines(keepends=True)
        except OSError as e:
            fail("error: cannot read %s: %s" % (path, e.strerror))
        # A cloudserver pin that scality-kms follows to a missing image
        # cannot deploy: pin kms first, with --kms. Only when the cloudserver
        # pin will move: a cloudserver image not published yet is no reason
        # to hold back the other pins.
        if not args.kms and args.only in (None, "cloudserver") and rows[0]["state"] == "exists":
            blocker = kms_warning(lines, rows)
            if blocker:
                fail("error: " + blocker, 1)
    new = apply_pins(lines, only_rows(rows, args.only))
    diff = list(difflib.unified_diff(lines, new, "a/" + GROUP_VARS,
                                     "b/" + GROUP_VARS))
    if args.apply and diff:
        with open(path, "wb") as handle:
            handle.write("".join(new).encode("utf-8"))

    if args.as_json:
        print(json.dumps({"epic": epic, "rows": rows, "diff": "".join(diff),
                          "warning": warning,
                          "applied": bool(args.apply and diff)},
                         indent=2))
    else:
        print("epic: " + epic)
        print_table(rows)
        if diff:
            print("\n" + ("applied:" if args.apply else "would change:"))
            sys.stdout.write("".join(diff))
        if warning:
            print("\nwarning: " + warning)
    # With --only, only the selected row decides: another service's failed
    # lookup must not fail a one-service run.
    sys.exit(1 if any(r["action"] == "error" for r in only_rows(rows, args.only)) else 0)


if __name__ == "__main__":
    main()
