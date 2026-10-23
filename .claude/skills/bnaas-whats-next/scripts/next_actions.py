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
   no PR yet, except a standing or dropped task. A local branch counts only when its name carries a ticket
   key of the Jira export (render.json `jira_keys`) or it is the head of
   an open D1 PR; the others go to `other_local`, never ranked.
3. Keep the pipeline flowing: Federation pins behind, Jira mismatches.
4. Start the next task: the ready task with no owner, in their
   repositories, that unblocks the most work downstream (the count of its
   transitive dependents, then their estimate days). Admin and
   nice-to-have tasks come last. A task whose only open dependencies have
   an open PR is offered as a piggyback: a branch cut from that PR. A
   dropped task is never offered, never counts as unblocked work, and
   never blocks a task that depends on it.

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
from backlog import is_dropped, is_standing, load_backlog  # noqa: E402

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
        if is_dropped(t):
            continue
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
    # A row is a task, or one step ticket of a task (render_tickets.py).
    rows = [r for a in render.get("areas", []) for r in a["rows"]]
    task_of_key = {k: r.get("task_id", r["id"]) for r in rows for k in r.get("tickets", [])}
    prs = prs_doc.get("prs", []) if isinstance(prs_doc, dict) else prs_doc
    # A PR into a development branch with no D1 ticket is not D1 work.
    skipped = {s["pr"] for s in render.get("skipped", [])}
    prs = [p for p in prs if p["pr"] not in skipped]
    open_prs = [p for p in prs if not p["status"].startswith(ENDED)]
    # Without the Jira export, D1 work and claims are unknown: only the
    # reviews of epic PRs, the local work and the pins can be ranked.
    jira_ok = bool(render.get("jira_keys"))
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
        if p.get("kind") == "review" and (
                jira_ok or not (p.get("base") or "").startswith("development/")):
            add(1, "review", pr_name(p), p["pr"],
                "Opens the PR in your browser and gives a five-line review brief. Your "
                "approval lets %s merge it." % p.get("author", "its author"),
                "%s waits for your review%s." % (p.get("author", "Its author"),
                                                 ", and it is a draft" if p.get("draft") else ""),
                [p["url"]], task_of_key.get(p.get("ticket")))

    # 2. Finish what you started. Only D1 branches: the clones also hold
    # other work, which is listed but never ranked.
    # By repository and branch: one ticket can have a branch of the same
    # name in two clones.
    heads = {(p["pr"].split("#")[0].lower(), p["head"]): p for p in open_prs if p.get("head")}
    d1_keys = set(render.get("jira_keys", [])) | set(task_of_key)
    # A backup branch is never work: it must not hide a continue (by its
    # key) or widen the developer's repositories.
    local = [b for b in local if not b["branch"].startswith("backup/")]
    other = [b for b in local
             if (b["repo"].lower(), b["branch"]) not in heads
             and not any(k in d1_keys for k in KEY.findall(b["branch"]))] if jira_ok else []
    local = [b for b in local if b not in other]
    for b in local:
        key = KEY.search(b["branch"])
        tid = task_of_key.get(key.group(1)) if key else None
        subject = KEY.sub("", b["subject"]).lstrip(": ").strip()
        title = "%s: %s" % (REPO_NAMES.get(b["repo"], b["repo"]), subject)
        pr = heads.get((b["repo"].lower(), b["branch"]))
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
        elif p.get("approved"):
            # The flag of prs.py, not the status words. Two distinct humans
            # are enough; without the other developer, say so (Rule 4).
            # bert-e merges into development/* (and runs the waterfall):
            # there the author approves with /approve, never gh pr merge.
            dev = (p.get("base") or "").startswith("development/")
            add(2, "merge", pr_name(p), p["pr"],
                "Comments /approve on the approved PR; bert-e merges it into %s." % p["base"]
                if dev else "Merges the approved PR, which moves the epic forward.",
                "It has its two approvals." if p.get("other_approved", True) else
                "It has two approvals, without the other developer's: the journal "
                "records the merge without their review.", [p["url"]], tid)
    # Whole keys, so that a branch of ZZ-917 is not local work of ZZ-91.
    local_keys = {k for b in local for k in KEY.findall(b["branch"])}
    for r in rows if jira_ok else []:
        # Per ticket row: the steps of one task can belong to different people.
        tid = r.get("task_id", r["id"])
        if tid in done or (tid in by_id and (is_standing(by_id[tid]) or is_dropped(by_id[tid]))):
            continue
        if r.get("owner") == me and r.get("tickets") and not r.get("done") \
                and not r.get("prs") and not any(k in local_keys for k in r["tickets"]):
            add(2, "continue", task_name(by_id[tid]) if tid in by_id else r["task"],
                "task %s" % r["id"],
                "Makes the worktree of its ticket and starts plan mode.",
                "You claimed it (%s) and nothing of it is pushed yet." % r.get("status", ""),
                [], tid)
    # A task whose step tickets are all Done is still open (the Backlog tab
    # does not list its steps): its owner either opens the next step ticket
    # or marks the task done, or its dependents wait for good.
    steps = {}
    for r in rows:
        if r.get("tickets") and r.get("task_id", r["id"]) != r["id"]:
            steps.setdefault(r["task_id"], []).append(r)
    for tid, group in steps.items() if jira_ok else []:
        if tid in done or (tid in by_id and (is_standing(by_id[tid]) or is_dropped(by_id[tid]))):
            continue
        # An open task-level ticket still carries the task: no next step.
        whole_open = any(r.get("tickets") and r["id"] == tid and not r.get("done") for r in rows)
        if not whole_open and all(r.get("done") for r in group) \
                and any(r.get("owner") == me for r in group):
            add(2, "next-step", task_name(by_id[tid]) if tid in by_id else group[0]["task"],
                "task %s" % tid,
                "Asks whether to open the next step ticket of the task, or to mark the task "
                "done in the Backlog tab; nothing changes without your yes.",
                "Its step tickets (%s) are all Done, but the task is not, so its dependents "
                "wait." % ", ".join(r["id"] for r in group), [], tid)

    # 3. Keep the pipeline flowing.
    behind = [r for r in (pins or {}).get("rows", []) if r.get("action") == "pin"]
    if behind:
        add(3, "pins", "Federation: pin the new epic images", "bnaas-pin-federation",
            "Runs /bnaas-pin-federation, which opens or updates the Federation pin PR.",
            "The pins are behind for %s, so Federation deploys old code."
            % ", ".join(r["repo"] for r in behind), [])
    fixes = [x for x in render.get("out_of_sync", []) if not x.startswith("prs.py error")]
    if fixes and jira_ok:
        add(3, "jira-fix", "Jira: fix %d mismatch(es)" % len(fixes), "out of sync",
            "Proposes the Jira fixes one by one; nothing changes without your yes.",
            "; ".join(fixes[:2]) + ("; and more" if len(fixes) > 2 else "") + ".", [])

    # 4. Start the next task.
    mine = {repo for r in rows if me in r.get("owner", "") for repo in r["repos"]}
    mine |= {b["repo"] for b in local}
    claimed = {r.get("task_id", r["id"]) for r in rows if r.get("tickets")}
    start = []
    for t in tasks if jira_ok else []:
        tid = t["id"]
        if tid in done or tid in claimed or t["depends_on_unresolved"] or is_dropped(t):
            continue
        # Prefer the developer's repositories, but never hide a ready task.
        elsewhere = bool(mine) and not mine & set(t["repos"])
        # A dropped dependency is no longer required.
        missing = [d for d in t["depends_on"]
                   if d not in done and not (d in by_id and is_dropped(by_id[d]))]
        count, days = len(reach[tid][0]), reach[tid][1]
        low = bool(LOW.search(" ".join((t["title"], t.get("notes", "")))))
        if not missing:
            start.append((low, elsewhere, 0, -count, -days, t, None))
            continue
        # Piggyback: every open dependency is claimed and has an open PR.
        bases = []
        for d in missing:
            dep_prs = [p for p in open_prs if task_of_key.get(p.get("ticket")) == d]
            if d not in claimed or not dep_prs:
                break
            bases.append((d, dep_prs[0]))
        else:
            start.append((low, elsewhere, 1, -count, -days, t, bases))
    start.sort(key=lambda x: x[:5])
    for low, elsewhere, _, count, days, t, bases in start[:3]:
        why = "Ready, nobody owns it, and it unblocks %d task(s), %s days of work." \
            % (-count, -days) if not bases else \
            "It waits only on open PRs, so a branch cut from them starts it today."
        if elsewhere:
            why += " Outside your repositories, so it comes after them."
        if low:
            why += " Admin or nice to have, so it comes last."
        if not bases:
            add(4, "start", task_name(t), "task %s" % t["id"],
                "Creates its Jira ticket after one question, makes its worktree and starts "
                "plan mode.", why, [], t["id"])
            continue
        # Across repositories when the task is not in the PR's repository.
        cross = [p for d, p in bases if set(t["repos"]) != {p["pr"].split("#")[0].lower()}]
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
    return {"login": login, "actions": actions,
            "other_local": ["%s %s" % (b["repo"], b["branch"]) for b in other]}


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
         "depends_on": [], "depends_on_unresolved": []},
        {"id": "6", "title": "Old idea", "repos": ["arsenal"], "estimate_days": 1,
         "depends_on": [], "depends_on_unresolved": [],
         "notes": "Dropped: no epic releases (2026-10-22)"},
        {"id": "7", "title": "Older idea", "repos": ["arsenal"], "estimate_days": 1,
         "depends_on": ["5"], "depends_on_unresolved": [], "status": "dropped"},
        {"id": "8", "title": "After the old idea", "repos": ["arsenal"], "estimate_days": 1,
         "depends_on": ["6"], "depends_on_unresolved": [], "notes": "admin"},
        {"id": "9", "title": "Skill fixes", "repos": ["backbeat"], "estimate_days": 0,
         "depends_on": [], "depends_on_unresolved": [], "status": "standing"},
        {"id": "10", "title": "Shared", "repos": ["arsenal"], "estimate_days": 1,
         "depends_on": [], "depends_on_unresolved": []},
        {"id": "11", "title": "Stepped", "repos": ["arsenal"], "estimate_days": 1,
         "depends_on": [], "depends_on_unresolved": []},
        {"id": "12", "title": "Carried", "repos": ["arsenal"], "estimate_days": 1,
         "depends_on": [], "depends_on_unresolved": []}]}
    render = {"done": ["1"], "out_of_sync": ["ZZ-9 has no assignee"], "areas": [{"rows": [
        {"id": "1", "task": "Base", "repos": ["arsenal"], "owner": "Anurag Mittal",
         "tickets": ["ZZ-1"], "prs": [], "status": "Done"},
        {"id": "2", "task": "Add the IAM actions", "repos": ["arsenal"],
         "owner": "Taylor McKinnon", "tickets": ["ZZ-2"], "prs": [{"pr": "arsenal#5"}],
         "status": "In Review"},
        # A standing task: its open ticket is never work to continue.
        {"id": "9", "task": "Skill fixes", "repos": ["backbeat"], "owner": "Anurag Mittal",
         "tickets": ["ZZ-9"], "prs": [], "status": "Ongoing"},
        # A dropped task with a ticket is never offered either.
        {"id": "6", "task": "Old idea", "repos": ["arsenal"], "owner": "Anurag Mittal",
         "tickets": ["ZZ-6"], "prs": [], "status": "dropped"},
        # A task with a step ticket each: Taylor's has a PR, Anurag's none.
        {"id": "10", "task": "Shared", "repos": ["arsenal"], "owner": "", "tickets": [],
         "prs": [], "status": "", "task_id": "10"},
        {"id": "10.1", "task": "Shared", "repos": ["arsenal"], "owner": "Anurag Mittal",
         "tickets": ["ZZ-10"], "prs": [], "status": "Ongoing", "done": False, "task_id": "10"},
        {"id": "10.2", "task": "Shared", "repos": ["arsenal"], "owner": "Taylor McKinnon",
         "tickets": ["ZZ-11"], "prs": [{"pr": "arsenal#9"}], "status": "In Review",
         "done": False, "task_id": "10"},
        # Task 12 has an open task-level ticket and a Done step 12.1: the
        # task-level ticket carries it, so no next step.
        {"id": "12", "task": "Carried", "repos": ["arsenal"], "owner": "Anurag Mittal",
         "tickets": ["ZZ-13"], "prs": [], "status": "Ongoing", "done": False, "task_id": "12"},
        {"id": "12.1", "task": "Carried", "repos": ["arsenal"], "owner": "Anurag Mittal",
         "tickets": ["ZZ-14"], "prs": [], "status": "Done", "done": True, "task_id": "12"},
        # Task 11 has only step 11.1, Done: the task is still open.
        {"id": "11", "task": "Stepped", "repos": ["arsenal"], "owner": "", "tickets": [],
         "prs": [], "status": "", "task_id": "11"},
        {"id": "11.1", "task": "Stepped", "repos": ["arsenal"], "owner": "Anurag Mittal",
         "tickets": ["ZZ-12"], "prs": [], "status": "Done", "done": True, "task_id": "11"}]}]}
    render["skipped"] = [{"pr": "arsenal#6"}]
    render["jira_keys"] = ["ZZ-1", "ZZ-2", "ZZ-7", "ZZ-8"]
    prs = {"prs": [{"pr": "arsenal#6", "url": "u", "title": "Not D1", "kind": "review",
                    "status": "Waiting for review from Anurag"},
                   {"pr": "arsenal#5", "url": "https://github.com/scality/arsenal/pull/5",
                    "title": "ZZ-2: Add the IAM actions", "author": "Taylor", "draft": False,
                    "ticket": "ZZ-2", "kind": "review", "head": "improvement/ZZ-2-iam",
                    "status": "Waiting for review from Anurag"}]}
    local = [{"repo": "arsenal", "branch": "improvement/ZZ-7-x", "ahead": 2,
              "subject": "ZZ-7: Add the lonely bit", "worktree": None},
             {"repo": "backbeat", "branch": "backup/old", "ahead": 9, "subject": "x"},
             {"repo": "cloudserver", "branch": "improvement/ZZ-850-other", "ahead": 1,
              "subject": "ZZ-850: Not D1"}]
    pins = {"rows": [{"repo": "vault", "action": "pin"}]}
    result = build(render, local, prs, backlog, "anurag4DSB", pins)
    # A branch with no D1 ticket is listed, never ranked, and does not make
    # its repository one of the developer's.
    assert result["other_local"] == ["cloudserver improvement/ZZ-850-other"], result
    got = result["actions"]
    assert all("_weight" not in a for a in got)
    kinds = [a["kind"] for a in got]
    assert kinds == ["review", "push-pr", "continue", "continue", "next-step", "pins",
                     "jira-fix", "start", "piggyback", "start"], kinds
    assert [a["ref"] for a in got if a["kind"] == "next-step"] == ["task 11"], got
    # Merge reads the approved flag of prs.py, and says when the other
    # developer is not among the approvers.
    mine = {"prs": [{"pr": "arsenal#20", "url": "u20", "title": "ZZ-1: x", "kind": "mine",
                     "status": "Approved, not merged yet without the other developer",
                     "approved": True, "other_approved": False, "ticket": "ZZ-1"},
                    {"pr": "arsenal#21", "url": "u21", "title": "ZZ-1: y", "kind": "mine",
                     "status": "Approved by a bot only", "ticket": "ZZ-1"}]}
    merges = [a for a in build(render, [], mine, backlog, "anurag4DSB")["actions"]
              if a["kind"] == "merge"]
    assert [a["ref"] for a in merges] == ["arsenal#20"], merges
    assert "without the other developer" in merges[0]["why"], merges
    # Into a development branch, bert-e merges after the author's /approve.
    dev = {"prs": [dict(mine["prs"][0], base="development/8.3")]}
    merges = [a for a in build(render, [], dev, backlog, "anurag4DSB")["actions"]
              if a["kind"] == "merge"]
    assert "/approve" in merges[0]["does"] and "bert-e" in merges[0]["does"], merges
    # Across repositories means the task against the PR's repository, not
    # against the repositories of the task the PR is for.
    wide = {"areas": [{"id": "g1", "title": "A"}], "tasks": [
        {"id": "20", "title": "Both sides", "repos": ["arsenal", "cloudserver"],
         "estimate_days": 1, "depends_on": [], "depends_on_unresolved": []},
        {"id": "21", "title": "Use both", "repos": ["arsenal", "cloudserver"],
         "estimate_days": 1, "depends_on": ["20"], "depends_on_unresolved": []}]}
    claim = {"done": [], "jira_keys": ["ZZ-20"], "areas": [{"rows": [
        {"id": "20", "task": "Both sides", "repos": ["arsenal", "cloudserver"],
         "owner": "Taylor McKinnon", "tickets": ["ZZ-20"], "prs": [{"pr": "arsenal#41"}],
         "status": "In Review", "task_id": "20"}]}]}
    open41 = {"prs": [{"pr": "arsenal#41", "url": "u41", "title": "ZZ-20: x", "ticket": "ZZ-20",
                       "kind": "other", "status": "Waiting for review from Nicolas"}]}
    pig = [a for a in build(claim, [], open41, wide, "anurag4DSB")["actions"]
           if a["kind"] == "piggyback"]
    assert pig and "package.json" in pig[0]["does"], pig
    # Without the Jira export: no non-D1 review, no claim, no start, no
    # Jira fix; the reviews of epic PRs and the local work stay.
    blind = dict(render, jira_keys=[])
    noisy = {"prs": prs["prs"] + [{"pr": "arsenal#30", "url": "u30", "title": "Not D1",
                                   "kind": "review", "base": "development/8.3",
                                   "status": "Waiting for review from Anurag"}]}
    kinds = [a["kind"] for a in build(blind, local, noisy, backlog, "anurag4DSB", pins)["actions"]]
    # The two local branches stay: without Jira, none can be called non-D1.
    assert kinds == ["review", "push-pr", "push-pr", "pins"], kinds
    # The same branch name in another clone is not the head of that PR.
    twin = [{"repo": "cloudserver", "branch": "improvement/ZZ-2-iam", "ahead": 1,
             "subject": "ZZ-2: the CloudServer side"}]
    acts = build(render, twin, prs, backlog, "anurag4DSB")["actions"]
    assert [a["kind"] for a in acts if a["ref"] == "improvement/ZZ-2-iam"] == ["push-pr"], acts
    # A backup branch with the key of a claimed ticket hides nothing.
    backup = local + [{"repo": "arsenal", "branch": "backup/ZZ-10-before-rebase", "ahead": 3,
                       "subject": "ZZ-10: old"}]
    acts = build(render, backup, prs, backlog, "anurag4DSB", pins)["actions"]
    assert "task 10.1" in [a["ref"] for a in acts if a["kind"] == "continue"], acts
    # Anurag's own step ticket of task 10 has no PR, whatever Taylor's has.
    assert [a["ref"] for a in got if a["kind"] == "continue"] == ["task 10.1", "task 12"], got
    review = got[0]
    assert review["recommended"] and review["title"] == "Arsenal: Add the IAM actions", review
    assert review["ref"] == "arsenal#5"
    assert review["unblocks"] == ["CloudServer: Use the actions (task 3)"], review
    assert got[1]["title"] == "Arsenal: Add the lonely bit", got[1]
    starts = [a for a in got if a["rank"] == 4]
    # 5 unblocks nothing but is not admin. Task 3 is in cloudserver, outside
    # Anurag's repositories: offered after them, never hidden. 4 and 8 are
    # admin, so last. 6 and 7 are dropped: never offered, and 7 does not
    # count as work that 5 unblocks.
    assert [a["ref"] for a in starts] == ["task 5", "task 3", "task 4"], starts
    assert starts[0]["unblocks"] == [], starts
    assert "Outside your repositories" in starts[1]["why"], starts
    # Without task 3, 8 shows: it depends only on the dropped 6, so it is ready.
    other = dict(backlog, tasks=[t for t in backlog["tasks"] if t["id"] != "3"])
    refs = [a["ref"] for a in build(render, local, prs, other, "anurag4DSB", pins)["actions"]
            if a["rank"] == 4]
    assert refs == ["task 5", "task 4", "task 8"], refs
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
        for k in ("--render", "--local", "--prs"):
            with open(args[k], encoding="utf-8") as f:
                loaded[k] = json.load(f)
        backlog = load_backlog(args["--backlog"])
    except (OSError, ValueError) as e:
        print("cannot read the inputs: %s" % e, file=sys.stderr)
        return 2
    # The pins are optional: read 4 can fail (no Federation clone), and an
    # empty or broken file then means no pins row, not no actions.
    try:
        if "--pins" in args:
            with open(args["--pins"], encoding="utf-8") as f:
                loaded["--pins"] = json.load(f)
    except (OSError, ValueError) as e:
        print("no pins (%s): the pins row is left out" % e, file=sys.stderr)
    print(json.dumps(build(loaded["--render"], loaded["--local"], loaded["--prs"], backlog,
                           args["--login"], loaded.get("--pins")), indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
