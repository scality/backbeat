#!/usr/bin/env python3
"""Rank the next actions of one D1 developer, by critical path.

Usage:
  next_actions.py --render <render.json> --local <local.json> --prs <prs.json>
                  --backlog <backlog.md> --login <login> [--pins <pins.json>]
  next_actions.py --self-test

The inputs are the outputs of render_tickets.py --json, local_work.py
--json, prs.py --json, the Backlog tab export and, when there is one,
pins.py --dry-run --json. The order, for this developer:

1. Unblock the other developer: the PRs that wait for their review.
2. Finish what they started: local commits to push (and open as a PR),
   their PRs with changes requested or approved, their claimed tasks with
   no PR yet.
3. Keep the pipeline flowing: Federation pins behind, Jira mismatches.
4. Start the next task: the ready task with no owner, in their
   repositories, that unblocks the most work downstream (the count of its
   transitive dependents, then their estimate days). Admin and
   nice-to-have tasks come last. A task whose only open dependencies have
   an open PR is offered as a piggyback: a branch cut from that PR.

It prints {"actions": [...]}, best first. Each action has `kind`, `rank`
(the group above), `title` (the plain name: "Arsenal: Add the bucketnotif
IAM actions"), `ref` ("task 25", "Federation#7261" or a branch), `does`
(what the action does and what it leads to, in plain words), `why`,
`unblocks` (plain task names), `links` and `recommended` (the first).
"""
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "..", "bnaas", "scripts"))
from backlog import load_backlog  # noqa: E402

JIRA_NAMES = {"anurag4dsb": "Anurag Mittal", "tmacro": "Taylor McKinnon"}
REPO_NAMES = {"arsenal": "Arsenal", "vault": "Vault", "cloudserver": "CloudServer",
              "cloudserverclient": "cloudserverclient", "backbeat": "backbeat",
              "federation": "Federation", "integration": "Integration", "ring": "RING",
              "s3utils": "s3utils"}
LOW = re.compile(r"\badmin\b|nice[- ]to[- ]have|\boptional\b", re.I)
KEY = re.compile(r"\b([A-Z][A-Z0-9]+-(?:\d+|X+))\b")
ENDED = ("Merged", "Closed")


def repo_label(repos):
    if not repos:
        return "D1"
    if len(repos) > 3:
        return "All repositories"
    return ", ".join(REPO_NAMES.get(r, r) for r in repos)


def task_name(task):
    return "%s: %s" % (repo_label(task["repos"]), task["title"])


def pr_name(pr):
    repo = pr["pr"].split("#")[0]
    return "%s: %s" % (REPO_NAMES.get(repo.lower(), repo), KEY.sub("", pr["title"])
                       .lstrip(": ").strip() or pr["title"])


def graph(tasks, done):
    """{task id: (dependent ids, their estimate days)} over open tasks."""
    after = {}
    for t in tasks:
        for d in t["depends_on"]:
            after.setdefault(d, []).append(t["id"])
    by_id = {t["id"]: t for t in tasks}
    out = {}
    for t in tasks:
        seen, todo = [], list(after.get(t["id"], []))
        while todo:
            x = todo.pop(0)
            if x in seen or x in done:
                continue
            seen.append(x)
            todo += after.get(x, [])
        out[t["id"]] = (seen, sum(by_id[x]["estimate_days"] for x in seen))
    return out


def build(render, local, prs_doc, backlog, login, pins=None):
    tasks = backlog["tasks"]
    by_id = {t["id"]: t for t in tasks}
    done = set(render.get("done", []))
    reach = graph(tasks, done)
    me = JIRA_NAMES.get(login.lower(), login)
    rows = {r["id"]: r for a in render.get("areas", []) for r in a["rows"]}
    task_of_key = {k: tid for tid, r in rows.items() for k in r.get("tickets", [])}
    prs = prs_doc.get("prs", []) if isinstance(prs_doc, dict) else prs_doc
    # A PR into a development branch with no D1 ticket is not D1 work.
    skipped = {s["pr"] for s in render.get("skipped", [])}
    prs = [p for p in prs if p["pr"] not in skipped]
    open_prs = [p for p in prs if not p["status"].startswith(ENDED)]
    actions = []

    def unblocks(tid, limit=3):
        ids = reach.get(tid, ([], 0))[0]
        names = ["%s (task %s)" % (task_name(by_id[x]), x) for x in ids[:limit]]
        if len(ids) > limit:
            names.append("%d more" % (len(ids) - limit))
        return names

    def add(rank, kind, title, ref, does, why, links, tid=None):
        actions.append({"rank": rank, "kind": kind, "title": title, "ref": ref, "does": does,
                        "why": why, "unblocks": unblocks(tid) if tid else [],
                        "links": links, "recommended": False,
                        "_weight": -len(reach.get(tid, ([], 0))[0]) if tid else 0})

    # 1. Unblock the other developer.
    for p in prs:
        if p.get("kind") == "review":
            add(1, "review", pr_name(p), p["pr"],
                "Opens the PR in your browser and gives a five-line review brief. Your "
                "approval lets %s merge it." % p.get("author", "its author"),
                "%s waits for your review%s." % (p.get("author", "Its author"),
                                                 ", and it is a draft" if p.get("draft") else ""),
                [p["url"]], task_of_key.get(p.get("ticket")))

    # 2. Finish what you started.
    heads = {p.get("head"): p for p in open_prs if p.get("head")}
    for b in local:
        if b["branch"].startswith("backup/"):
            continue
        key = KEY.search(b["branch"])
        tid = task_of_key.get(key.group(1)) if key else None
        subject = KEY.sub("", b["subject"]).lstrip(": ").strip()
        title = "%s: %s" % (REPO_NAMES.get(b["repo"], b["repo"]), subject)
        pr = heads.get(b["branch"])
        if pr:
            add(2, "push", title, b["branch"],
                "Pushes %d new commit(s) to %s, so that its reviewers see them."
                % (b["ahead"], pr["pr"]),
                "The commits exist only on this machine.", [pr["url"]], tid)
        else:
            add(2, "push-pr", title, b["branch"],
                "Checks the commit story, pushes the branch and opens a draft PR with the "
                "five sections, then opens it in your browser.",
                "%d commit(s) exist only on this machine; nobody can review them yet."
                % b["ahead"], [], tid)
    for p in prs:
        if p.get("kind") != "mine":
            continue
        tid = task_of_key.get(p.get("ticket"))
        if p["status"].startswith("Changes requested"):
            add(2, "fix-pr", pr_name(p), p["pr"],
                "Opens the PR and its review comments, so that you fix them and push.",
                p["status"] + ".", [p["url"]], tid)
        elif p["status"].startswith("Approved"):
            add(2, "merge", pr_name(p), p["pr"],
                "Merges the approved PR, which moves the epic forward.",
                "It has its two approvals.", [p["url"]], tid)
    for tid, r in rows.items():
        if me in r.get("owner", "") and tid not in done and not r.get("prs") \
                and not any(k in b["branch"] for b in local for k in r.get("tickets", [])):
            add(2, "continue", task_name(by_id[tid]) if tid in by_id else r["task"],
                "task %s" % tid,
                "Makes the worktree of its ticket and starts plan mode.",
                "You claimed it (%s) and nothing of it is pushed yet." % r.get("status", ""),
                [], tid)

    # 3. Keep the pipeline flowing.
    behind = [r for r in (pins or {}).get("rows", []) if r.get("action") == "pin"]
    if behind:
        add(3, "pins", "Federation: pin the new epic images", "bnaas-pins",
            "Runs /bnaas-pins, which opens or updates the Federation pin PR.",
            "The pins are behind for %s, so Federation deploys old code."
            % ", ".join(r["repo"] for r in behind), [])
    fixes = [x for x in render.get("out_of_sync", []) if not x.startswith("prs.py error")]
    if fixes:
        add(3, "jira-fix", "Jira: fix %d mismatch(es)" % len(fixes), "out of sync",
            "Proposes the Jira fixes one by one; nothing changes without your yes.",
            "; ".join(fixes[:2]) + ("; and more" if len(fixes) > 2 else "") + ".", [])

    # 4. Start the next task.
    mine = {repo for r in rows.values() if me in r.get("owner", "") for repo in r["repos"]}
    mine |= {b["repo"] for b in local}
    claimed = {tid for tid, r in rows.items() if r.get("tickets")}
    start = []
    for t in tasks:
        tid = t["id"]
        if tid in done or tid in claimed or t["depends_on_unresolved"]:
            continue
        if mine and not mine & set(t["repos"]):
            continue
        missing = [d for d in t["depends_on"] if d not in done]
        count, days = len(reach[tid][0]), reach[tid][1]
        low = bool(LOW.search(" ".join((t["title"], t.get("notes", "")))))
        if not missing:
            start.append((low, 0, -count, -days, t, None))
            continue
        # Piggyback: every open dependency is claimed and has an open PR.
        bases = []
        for d in missing:
            dep_prs = [p for p in open_prs if task_of_key.get(p.get("ticket")) == d]
            if d not in claimed or not dep_prs:
                break
            bases.append((d, dep_prs[0]))
        else:
            start.append((low, 1, -count, -days, t, bases))
    start.sort(key=lambda x: x[:4])
    for low, _, count, days, t, bases in start[:3]:
        why = "Ready, nobody owns it, and it unblocks %d task(s), %s days of work." \
            % (-count, -days) if not bases else \
            "It waits only on open PRs, so a branch cut from them starts it today."
        if low:
            why += " Admin or nice to have, so it comes last."
        if not bases:
            add(4, "start", task_name(t), "task %s" % t["id"],
                "Creates its Jira ticket after one question, makes its worktree and starts "
                "plan mode.", why, [], t["id"])
            continue
        cross = [p for d, p in bases if set(by_id[d]["repos"]) != set(t["repos"])]
        add(4, "piggyback", task_name(t), "task %s" % t["id"],
            "Claims it, and cuts its branch from %s instead of the epic.%s"
            % (", ".join(p["pr"] for d, p in bases),
               " Across repositories, package.json pins that PR's commit by git until "
               "its release, and the pin goes back to the released version before the "
               "PR leaves draft." if cross else " If that PR changes, rebase."),
            why, [p["url"] for d, p in bases], t["id"])

    # Within a group, the action that unblocks the most comes first. The
    # start group is already in its own order.
    actions.sort(key=lambda a: (a["rank"], a.pop("_weight") * (a["rank"] < 4)))
    if actions:
        actions[0]["recommended"] = True
    return {"login": login, "actions": actions}


def self_test():
    backlog = {"areas": [{"id": "g1", "title": "A"}], "tasks": [
        {"id": "1", "title": "Base", "repos": ["arsenal"], "estimate_days": 1,
         "depends_on": [], "depends_on_unresolved": []},
        {"id": "2", "title": "Add the IAM actions", "repos": ["arsenal"], "estimate_days": 2,
         "depends_on": ["1"], "depends_on_unresolved": []},
        {"id": "3", "title": "Use the actions", "repos": ["cloudserver"],
         "estimate_days": 3, "depends_on": ["2"], "depends_on_unresolved": []},
        {"id": "4", "title": "Admin tool", "repos": ["arsenal"], "estimate_days": 1,
         "depends_on": ["1"], "depends_on_unresolved": [], "notes": "admin"},
        {"id": "5", "title": "Lonely", "repos": ["arsenal"], "estimate_days": 1,
         "depends_on": [], "depends_on_unresolved": []}]}
    render = {"done": ["1"], "out_of_sync": ["ZZ-9 has no assignee"], "areas": [{"rows": [
        {"id": "1", "task": "Base", "repos": ["arsenal"], "owner": "Anurag Mittal",
         "tickets": ["ZZ-1"], "prs": [], "status": "Done"},
        {"id": "2", "task": "Add the IAM actions", "repos": ["arsenal"],
         "owner": "Taylor McKinnon", "tickets": ["ZZ-2"], "prs": [{"pr": "arsenal#5"}],
         "status": "In Review"}]}]}
    render["skipped"] = [{"pr": "arsenal#6"}]
    prs = {"prs": [{"pr": "arsenal#6", "url": "u", "title": "Not D1", "kind": "review",
                    "status": "Waiting for review from Anurag"},
                   {"pr": "arsenal#5", "url": "https://github.com/scality/arsenal/pull/5",
                    "title": "ZZ-2: Add the IAM actions", "author": "Taylor", "draft": False,
                    "ticket": "ZZ-2", "kind": "review", "head": "improvement/ZZ-2-iam",
                    "status": "Waiting for review from Anurag"}]}
    local = [{"repo": "arsenal", "branch": "improvement/ZZ-7-x", "ahead": 2,
              "subject": "ZZ-7: Add the lonely bit", "worktree": None},
             {"repo": "backbeat", "branch": "backup/old", "ahead": 9, "subject": "x"}]
    pins = {"rows": [{"repo": "vault", "action": "pin"}]}
    got = build(render, local, prs, backlog, "anurag4DSB", pins)["actions"]
    assert all("_weight" not in a for a in got)
    kinds = [a["kind"] for a in got]
    assert kinds == ["review", "push-pr", "pins", "jira-fix", "start", "start"], kinds
    review = got[0]
    assert review["recommended"] and review["title"] == "Arsenal: Add the IAM actions", review
    assert review["ref"] == "arsenal#5"
    assert review["unblocks"] == ["CloudServer: Use the actions (task 3)"], review
    assert got[1]["title"] == "Arsenal: Add the lonely bit", got[1]
    starts = [a for a in got if a["rank"] == 4]
    # 5 unblocks nothing but is not admin; 4 is admin, so last.
    assert [a["ref"] for a in starts] == ["task 5", "task 4"], starts
    # Task 3 is in cloudserver, which is not among Anurag's repositories here.
    assert all(a["ref"] != "task 3" for a in got)
    # As Taylor, who owns 2: task 3 piggybacks on the open PR across repositories.
    local_t = [{"repo": "cloudserver", "branch": "improvement/ZZ-8-y", "ahead": 1,
                "subject": "ZZ-8: y"}]
    got = build(render, local_t, prs, backlog, "tmacro")["actions"]
    pig = [a for a in got if a["kind"] == "piggyback"]
    assert pig and pig[0]["ref"] == "task 3" and "package.json" in pig[0]["does"], got
    assert all(not re.fullmatch(r"\d+", a["title"]) for a in got)
    print("self-test ok")


def main(argv):
    if argv[:1] == ["--self-test"]:
        self_test()
        return 0
    args = dict(zip(argv[::2], argv[1::2]))
    need = ("--render", "--local", "--prs", "--backlog", "--login")
    if any(k not in args for k in need):
        print("usage: next_actions.py --render <f> --local <f> --prs <f> --backlog <f> "
              "--login <login> [--pins <f>] | --self-test", file=sys.stderr)
        return 2
    try:
        loaded = {}
        for k in ("--render", "--local", "--prs", "--pins"):
            if k in args:
                with open(args[k], encoding="utf-8") as f:
                    loaded[k] = json.load(f)
        backlog = load_backlog(args["--backlog"])
    except (OSError, ValueError) as e:
        print("cannot read the inputs: %s" % e, file=sys.stderr)
        return 2
    print(json.dumps(build(loaded["--render"], loaded["--local"], loaded["--prs"], backlog,
                           args["--login"], loaded.get("--pins")), indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
