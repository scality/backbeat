#!/usr/bin/env python3
"""Keep the Federation epic pinned to the CI images of the service epics.

Usage:
  pins.py [--federation <path>] [--epic <branch>] [--kms <version>]
          [--dry-run | --apply] [--json]

For cloudserver, backbeat and vault, the script reads the head of the
epic branch on GitHub, checks that the CI image of that head exists on
ghcr.io, and compares it with the pin in group_vars/all of the Federation
checkout. Dry run is the default and changes nothing. With --apply it
rewrites only the pins whose image exists, keeps every other byte of the
file as it is, and prints the unified diff. It never commits. No
scality-kms image exists per sha, so --kms pins that one to a released
tag instead. The epic defaults to the remote branch epic/*-bnaas-d1 of
the Federation checkout. The image lookup uses `docker buildx imagetools
inspect`, so `docker login ghcr.io` must have been done. The exit code
is non-zero when a lookup fails for a reason other than a missing image.
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
        sys.exit("cannot guess the epic branch, pass --epic")
    return names[0]


def epic_head(repo, epic):
    """The full sha of the epic head on GitHub, or None, and the stderr."""
    code, out, err = run(["git", "ls-remote", "https://github.com/" + repo,
                          "refs/heads/" + epic])
    if code == 0 and out.strip():
        return out.split()[0], ""
    return None, err.strip() or "no branch %s in %s" % (epic, repo)


def image_state(ref):
    """exists, missing, or error, from the registry lookup of one image,
    and the stderr of that lookup."""
    code, _, err = run(["docker", "buildx", "imagetools", "inspect", ref])
    if code == 0:
        return "exists", ""
    return ("missing" if "not found" in err else "error"), err.strip()


def find_pin(lines, block, key):
    """Index and value of the pin line, or (None, None) when absent."""
    current = None
    for i, line in enumerate(lines):
        if re.match(r"\S", line):
            current = None
        head = re.match(r"  (\S+):\s*$", line)
        current = head.group(1) if head else current
        if current == block and line.startswith(
                ("" if block is None else "    ") + key + ":"):
            value = re.search(r":\s*'([^']*)'", line)
            if value:
                return i, value.group(1)
    return None, None


def kms_warning(lines, rows):
    """Warn when kms follows cloudserver and has no image for that head."""
    _, value = find_pin(lines, "kms", "og_docker_image_version")
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
        index, current = find_pin(lines, "kms", "og_docker_image_version")
        image = "ghcr.io/scality/scality-kms:" + kms
        state, detail = (image_state(image) if index is not None
                         else ("error", "no kms og_docker_image_version in " + GROUP_VARS))
        action = {"exists": "same" if current == kms else "pin",
                  "missing": "wait"}.get(state, "error")
        rows.append({"repo": "scality-kms", "current": current, "head": "-",
                     "image": image, "state": state, "action": action,
                     "line": index, "detail": detail})
    return rows


def apply_pins(lines, rows):
    """New file lines with every 'pin' row rewritten on its own line."""
    new = list(lines)
    for row in rows:
        if row["action"] != "pin":
            continue
        tag = row["image"].split(":", 1)[1]
        line = new[row["line"]]
        new[row["line"]] = re.sub(r"'[^']*'", "'" + tag + "'", line, count=1)
    return new


def print_table(rows):
    cells = [("repo", "current pin", "epic head", "image", "action")]
    cells += [(r["repo"], r["current"] or "?", (r["head"] or "?")[:12],
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
                      help="rewrite the pins in group_vars/all, no commit")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()

    if not shutil.which("docker"):
        print("error: docker is not installed or not on PATH; the image lookup needs it",
              file=sys.stderr)
        sys.exit(2)
    path = os.path.join(args.federation, GROUP_VARS)
    try:
        with open(path, "rb") as handle:
            text = handle.read().decode("utf-8")
    except OSError as e:
        print("error: cannot read %s: %s; pass --federation <Federation checkout>"
              % (path, e.strerror), file=sys.stderr)
        sys.exit(2)
    lines = text.splitlines(keepends=True)
    epic = args.epic or default_epic(args.federation)
    rows = survey(lines, epic, args.kms)
    new = apply_pins(lines, rows)
    diff = list(difflib.unified_diff(lines, new, "a/" + GROUP_VARS,
                                     "b/" + GROUP_VARS))
    warning = None if args.kms else kms_warning(lines, rows)
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
    sys.exit(1 if any(r["action"] == "error" for r in rows) else 0)


if __name__ == "__main__":
    main()
