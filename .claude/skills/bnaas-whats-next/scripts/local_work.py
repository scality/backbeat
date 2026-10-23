#!/usr/bin/env python3
"""List the local D1 work that no remote branch holds yet.

Usage:
  local_work.py --workspace <folder> [--json]
  local_work.py --self-test

For every D1 clone in the workspace folder, and every worktree of it
(the clone itself, <clone>/.claude/worktrees/* and any other), the script
lists the local branches with commits that are on no remote branch: the
repository, the worktree that has the branch checked out (or none), the
branch, the commits ahead of every remote branch, and the last subject.
It reads git only, changes nothing and fetches nothing, so a remote
branch that moved since the last fetch shows as it was then. Clones whose
origin is not a scality GitHub repository are skipped.
"""
import json
import os
import re
import subprocess
import sys
import tempfile

REPOS = ("arsenal", "vault", "cloudserver", "cloudserverclient", "backbeat",
         "federation", "integration", "ring", "s3utils")
SCALITY = re.compile(r"^(?:git@github\.com:|(?:https|ssh)://(?:git@)?github\.com/)scality/",
                     re.I)


def git(path, *args):
    out = subprocess.run(["git", "-C", path] + list(args), capture_output=True, text=True)
    return out.stdout if out.returncode == 0 else None


def worktrees(clone):
    """{branch: worktree path} for the branches checked out somewhere."""
    found, path = {}, None
    for line in (git(clone, "worktree", "list", "--porcelain") or "").splitlines():
        if line.startswith("worktree "):
            path = line[len("worktree "):]
        elif line.startswith("branch refs/heads/") and path:
            found[line[len("branch refs/heads/"):]] = path
    return found


def scan_clone(clone, name):
    rows = []
    where = worktrees(clone)
    heads = git(clone, "for-each-ref", "--format=%(refname:short)", "refs/heads") or ""
    for branch in heads.split():
        ahead = git(clone, "rev-list", "--count", "refs/heads/" + branch, "--not", "--remotes")
        if not ahead or int(ahead) == 0:
            continue
        subject = (git(clone, "log", "-1", "--format=%s", "refs/heads/" + branch) or "").strip()
        rows.append({"repo": name, "worktree": where.get(branch), "branch": branch,
                     "ahead": int(ahead), "subject": subject})
    return rows


def scan(workspace):
    rows = []
    # A default clone of scality/Arsenal is named "Arsenal": match without case.
    entries = {e.lower(): e for e in sorted(os.listdir(workspace))} if os.path.isdir(workspace) else {}
    for name in REPOS:
        clone = os.path.join(workspace, entries.get(name, name))
        if not os.path.exists(os.path.join(clone, ".git")):
            continue
        if not SCALITY.match((git(clone, "remote", "get-url", "origin") or "").strip()):
            continue
        rows += scan_clone(clone, name)
    return rows


def markdown(rows, workspace):
    if not rows:
        return "No local branch holds commits that are on no remote branch.\n"
    lines = ["| Repository | Worktree | Branch | Ahead | Last commit |", "|---|---|---|---|---|"]
    for r in rows:
        # Both paths resolved: git gives the real path, the workspace may be
        # given through a symlink (the macOS temp folder is one).
        wt = (os.path.relpath(os.path.realpath(r["worktree"]), os.path.realpath(workspace))
              if r["worktree"] else "not checked out")
        lines.append("| %s | %s | %s | %d | %s |" % (r["repo"], wt, r["branch"], r["ahead"],
                                                    r["subject"].replace("|", "\\|")))
    return "\n".join(lines) + "\n"


def self_test():
    with tempfile.TemporaryDirectory() as ws:
        def run(path, *args):
            subprocess.run(["git", "-C", path] + list(args), check=True, capture_output=True)
        remote = os.path.join(ws, "remote.git")
        clone = os.path.join(ws, "backbeat")
        run(ws, "init", "-q", "--bare", remote)
        run(ws, "clone", "-q", remote, clone)
        for k, v in (("user.email", "t@example.com"), ("user.name", "t")):
            run(clone, "config", k, v)
        run(clone, "commit", "-q", "--allow-empty", "-m", "base")
        run(clone, "push", "-q", "origin", "HEAD:main")
        run(clone, "fetch", "-q")
        # Not a scality origin yet: skipped.
        assert scan(ws) == [], scan(ws)
        run(clone, "remote", "set-url", "origin", "git@github.com:scality/backbeat.git")
        run(clone, "worktree", "add", "-q", "-b", "improvement/topic",
            os.path.join(clone, ".claude", "worktrees", "topic"))
        wt = os.path.join(clone, ".claude", "worktrees", "topic")
        run(wt, "commit", "-q", "--allow-empty", "-m", "one")
        run(wt, "commit", "-q", "--allow-empty", "-m", "two")
        run(clone, "branch", "parked")
        rows = scan(ws)
        assert len(rows) == 1, rows
        r = rows[0]
        assert (r["repo"], r["branch"], r["ahead"], r["subject"]) == \
            ("backbeat", "improvement/topic", 2, "two"), r
        assert os.path.realpath(r["worktree"]) == os.path.realpath(wt), r
        assert "| backbeat | backbeat/.claude/worktrees/topic |" in markdown(rows, ws), \
            markdown(rows, ws)
    print("self-test ok")


def main(argv):
    if argv[:1] == ["--self-test"]:
        self_test()
        return 0
    if "--workspace" not in argv or argv.index("--workspace") + 1 >= len(argv):
        print("usage: local_work.py --workspace <folder> [--json] | --self-test",
              file=sys.stderr)
        return 2
    workspace = os.path.abspath(argv[argv.index("--workspace") + 1])
    rows = scan(workspace)
    if "--json" in argv:
        print(json.dumps(rows, indent=1))
    else:
        sys.stdout.write(markdown(rows, workspace))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
