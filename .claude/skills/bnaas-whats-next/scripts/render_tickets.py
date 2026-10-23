#!/usr/bin/env python3
"""Render the Tickets tab of the BNaaS D1 tech spec from the shared state.

Standard library only. The backlog gives the tasks and their dependencies,
the Jira export gives who claimed which task, and prs.py gives the PRs.
The output replaces the whole tab, so nobody edits its cells by hand.

    python3 -I render_tickets.py --backlog <backlog.md|backlog.json>
        --jira <issues.json> --prs <prs.json> [--sprint <name>] [--json]
    python3 -I render_tickets.py --self-test

The backlog is the Backlog tab of the doc exported to Markdown, or the JSON
of import_breakdown.py.

The Jira export is a list of issues (or a search answer that holds them under
"issues") with key, fields.summary,
fields.status.name, fields.assignee.displayName, fields.customfield_10004
(the sprints) and fields.description. The PR file is the output of
`prs.py --json`.

Each row says whether its dependencies are ready, blocked or unknown. A
task with a dependency text that the backlog could not resolve is never
ready: it is "dependencies unknown". The JSON also lists the done task
ids, the input of `backlog.py ready --done`, the ticket keys of the Jira
export (`jira_keys`), and the rows of the two
charts of the Progress tab: `progress` (tasks and estimate days per area
and status) and `scope` (breakdown tasks against new-scope tasks).

In the JSON, each area has `id`, `title` and `rows`; each row has `id`,
`task` (the title), `repos`, `estimate_days`, `depends_on`,
`dependencies`, `owner`, `tickets`, `prs`, `status` and `task_id`. The
steps of one task can belong to different developers, so a task with
more than one ticket renders as a task row that carries only the
dependency state, then one row per ticket, with its step id (such as
3.5), its own owner, status, PRs and `done`.

A task is done when all its tickets are Done, or when its Status cell in
the Backlog tab is "done": work done before the Jira epic has no ticket.
The Backlog tab does not list the steps of a task, so step tickets alone
never complete it: a task with step tickets is done only when its
task-level ticket (one that names the task, not a step) is Done, or its
Status cell says "done"; until then it is in progress.
A dropped task (backlog.is_dropped) is never ready, never done and not in
the progress chart, and its row shows only when a ticket names it. It is
no longer required: a dependency on it does not block, and shows as
"(dropped)".

A PR into a development branch (prs.py --dev, a base that starts with
"development/") counts only when its ticket is in the Jira export: the
developers also open PRs there that are not D1 work. The others, and the
PRs stacked on them, go to `skipped`, not to `out_of_sync`. A PR stacked on a ticket branch is D1
work like a PR into the epic.
"""
import argparse
import datetime
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "..", "bnaas", "scripts"))
from backlog import is_dropped, load_backlog  # noqa: E402

BROWSE = "https://scality.atlassian.net/browse/"
DONE_STATUSES = {"done", "closed", "resolved"}
SUMMARY_PREFIX = "BNaaS D1: "
COLUMNS = ("ID", "Task", "Repository", "Owner", "Ticket", "PR", "Status", "Depends on")


ROW_ID = r"[0-9]+(?:\.[0-9]+)?[a-z]?"


def clean_id(token):
    return token.strip().rstrip(".,;:)").lstrip("(")


def row_ids(issue):
    """The row ids that a ticket names: the "Tickets tab ... row <id>" line of
    the description, else any "row <id>" in it, then the first word of the
    summary after the D1 prefix. Only id-shaped tokens count, so
    the quotes and brackets of an ADF description never stick to an id."""
    fields = issue.get("fields") or {}
    desc = fields.get("description") or ""
    if not isinstance(desc, str):
        desc = json.dumps(desc)
    tracked = [m.group(1) for m in re.finditer(r"Tickets tab[^\n\"]*?\brow\s+(%s)\b" % ROW_ID,
                                                desc, re.I)]
    # The tracked line alone when there is one: another "row" in the text,
    # such as "builds on row 22b", must not claim that task.
    found = tracked or [m.group(1) for m in re.finditer(r"\brow\s+(%s)\b" % ROW_ID, desc, re.I)]
    summary = fields.get("summary") or ""
    if summary.startswith(SUMMARY_PREFIX):
        words = summary[len(SUMMARY_PREFIX):].split()
        if words:
            found.append(clean_id(words[0]))
    return found


def task_of(issue, by_id):
    """The task id of a ticket. A step id like 3.1 maps to task 3."""
    for rid in row_ids(issue):
        if rid in by_id:
            return rid
        if "." in rid and rid.split(".")[0] in by_id:
            return rid.split(".")[0]
    return None


def new_scope_ids(issues, by_id):
    """Row ids with a letter that tickets name and the backlog lacks, such
    as 22b or 3.3a: new scope that nobody added to the Backlog tab yet."""
    found = []
    for issue in issues:
        for rid in row_ids(issue):
            if (re.fullmatch(r"[0-9]+(\.[0-9]+)?[a-z]", rid) and rid not in by_id
                    and rid not in found):
                found.append(rid)
    return found


def step_id(issue, tid):
    """The step of task tid that a ticket names, such as 3.5, else tid."""
    for rid in row_ids(issue):
        if rid == tid or rid.startswith(tid + "."):
            return rid
    return tid


def is_done(issue):
    status = (issue.get("fields") or {}).get("status") or {}
    category = (status.get("statusCategory") or {}).get("key")
    if category:
        return category == "done"
    return (status.get("name") or "").lower() in DONE_STATUSES


def build(backlog, issues, prs_doc, sprint):
    areas, tasks = backlog["areas"], backlog["tasks"]
    by_id = {t["id"]: t for t in tasks}
    prs = prs_doc.get("prs", []) if isinstance(prs_doc, dict) else prs_doc
    out_of_sync = ["prs.py error: %s" % e for e in (prs_doc.get("errors") or [])] \
        if isinstance(prs_doc, dict) else []

    # Tickets by task. A task can hold more than one ticket.
    tickets = {}
    for issue in issues:
        tid = task_of(issue, by_id)
        if tid is None:
            out_of_sync.append("%s matches no task of the backlog" % issue["key"])
        else:
            tickets.setdefault(tid, []).append(issue)
    keys = {i["key"] for group in tickets.values() for i in group}
    exported = {i["key"] for i in issues}
    if not exported:
        out_of_sync.append("the Jira export is empty, so the PRs into development branches "
                           "were not filtered")
    # Not D1 work: a PR into development/* with no ticket of the export, and
    # any PR stacked on one, transitively.
    off = set()
    while exported:
        more = {p["pr"] for p in prs if p["pr"] not in off and p.get("ticket") not in exported
                and ((p.get("base") or "").startswith("development/")
                     or p.get("base") in {q.get("head") for q in prs if q["pr"] in off})}
        if not more:
            break
        off |= more
    prs_by_key, skipped = {}, []
    for pr in prs:
        if pr["pr"] in off:
            skipped.append({"pr": pr["pr"], "url": pr.get("url"), "ticket": pr.get("ticket"),
                            "why": "PR into %s with no D1 ticket" % pr.get("base")})
        elif pr.get("ticket") in keys:
            prs_by_key.setdefault(pr["ticket"], []).append(pr)
        else:
            out_of_sync.append("%s carries ticket %s, which no task has"
                               % (pr["pr"], pr.get("ticket") or "(none)"))

    marked = {t["id"] for t in tasks if t.get("status") == "done"}

    def dep_state(tid):
        if tid in marked:
            return "done"
        if tid in by_id and is_dropped(by_id[tid]):
            return "dropped"
        if tid not in tickets:
            return "unclaimed"
        whole = [i for i in tickets[tid] if step_id(i, tid) == tid]
        if len(whole) < len(tickets[tid]) and not whole:
            # Only step tickets: a step with no ticket yet may still be open.
            return "open"
        return "done" if all(is_done(i) for i in whole or tickets[tid]) else "open"

    done = {tid for tid in set(tickets) | marked if dep_state(tid) == "done"}
    for group in tickets.values():
        for issue in group:
            f = issue.get("fields") or {}
            key = issue["key"]
            if not f.get("assignee"):
                out_of_sync.append("%s has no assignee" % key)
            own = prs_by_key.get(key, [])
            merged = [p for p in own if p["status"].startswith("Merged")]
            closed = [p for p in own if p["status"].startswith("Closed")]
            # A ticket with an open PR is still in flight. One with only a
            # closed PR may be dropped, or waits for a new PR.
            if not is_done(issue) and len(merged) + len(closed) == len(own):
                if merged:
                    out_of_sync.append("%s is %s but its PR %s is merged"
                                       % (key, f["status"]["name"], merged[0]["pr"]))
                elif closed:
                    out_of_sync.append("%s is %s but its only PR %s is closed, not merged"
                                       % (key, f["status"]["name"], closed[0]["pr"]))
            sprints = [s.get("name") for s in (f.get("customfield_10004") or [])]
            if sprint and sprint not in sprints and not is_done(issue):
                out_of_sync.append("%s is not in sprint %s (has: %s)"
                                   % (key, sprint, ", ".join(sprints) or "none"))

    result = []
    for area in areas:
        own = [t for t in tasks if t["area"] == area["id"]]
        if not any(t["id"] in tickets or t["id"] in marked for t in own):
            continue
        rows = []
        for t in own:
            deps = ["%s (%s)" % (d, dep_state(d)) for d in t["depends_on"]]
            if is_dropped(t):
                readiness = "dropped"
            elif t.get("depends_on_unresolved"):
                deps.append("(dependencies unknown)")
                readiness = "unknown"
            elif all(d in done or dep_state(d) == "dropped" for d in t["depends_on"]):
                readiness = "ready"
            else:
                readiness = "blocked"
            row = {"id": t["id"], "task": t["title"], "repos": t["repos"],
                   "estimate_days": t["estimate_days"], "depends_on": deps,
                   "dependencies": readiness, "task_id": t["id"]}

            def ticket_fields(issue):
                """The fields of one ticket: its owner, PRs and status."""
                status = issue["fields"]["status"]["name"]
                return {"owner": (issue["fields"].get("assignee") or {}).get("displayName")
                        or "nobody",
                        "tickets": [issue["key"]], "done": is_done(issue),
                        "prs": [{"pr": p["pr"], "url": p["url"], "status": p["status"],
                                 "draft": p.get("draft", False), "ticket": p["ticket"]}
                                for p in prs_by_key.get(issue["key"], [])],
                        # Dropped wins over the ticket's own status.
                        "status": "dropped" if readiness == "dropped" else status}
            if t["id"] in tickets and len(tickets[t["id"]]) == 1 \
                    and step_id(tickets[t["id"]][0], t["id"]) == t["id"]:
                row.update(ticket_fields(tickets[t["id"]][0]))
            elif t["id"] in tickets:
                # The task row carries the dependency state; each step ticket,
                # even a single one, gets its own row.
                row.update(owner="", tickets=[], prs=[], status="")
                rows.append(row)
                for issue in tickets[t["id"]]:
                    step = {"id": step_id(issue, t["id"]), "task": t["title"],
                            "repos": t["repos"], "estimate_days": "", "depends_on": [],
                            "dependencies": "", "task_id": t["id"]}
                    step.update(ticket_fields(issue))
                    rows.append(step)
                continue
            elif t["id"] in marked:
                row.update(owner="nobody, no ticket", tickets=[], prs=[],
                           status="Done in the Backlog tab")
            elif readiness == "ready":
                row.update(owner="ready, nobody yet", tickets=[], prs=[], status="")
            elif readiness == "unknown":
                row.update(owner="dependencies unknown, nobody yet", tickets=[], prs=[],
                           status="")
            else:
                continue
            rows.append(row)
        result.append({"id": area["id"], "title": area["title"], "rows": rows})
    return {"date": datetime.date.today().isoformat(), "areas": result,
            "done": sorted(done, key=task_order(tasks)), "out_of_sync": out_of_sync,
            "skipped": skipped, "jira_keys": sorted(exported),
            "progress": progress(areas, tasks, done, set(tickets)),
            "scope": scope(tasks, issues, by_id)}


STATUSES = ("done", "in progress", "not started")


def progress(areas, tasks, done, claimed):
    """The rows of the "Progress by area" chart: one per area and status,
    with the task count and the estimate days. A task is in progress when
    it has a ticket that is not done. Dropped tasks are left out."""
    rows = []
    for area in areas:
        own = [t for t in tasks if t["area"] == area["id"] and not is_dropped(t)]
        for status in STATUSES:
            match = [t for t in own if status == ("done" if t["id"] in done else
                                                  "in progress" if t["id"] in claimed
                                                  else "not started")]
            days = round(sum(t["estimate_days"] for t in match), 2)
            rows.append({"area": area["title"], "status": status, "tasks": len(match),
                         "days": int(days) if days == int(days) else days})
    return rows


def scope(tasks, issues, by_id):
    """The counts of the "Scope and pivots over time" chart for this week:
    the breakdown tasks, and the new-scope tasks (Origin "new scope" in the
    Backlog tab, or a letter id that a ticket names and the backlog lacks).
    A dropped task counts in neither, so a drop lowers the chart."""
    tasks = [t for t in tasks if not is_dropped(t)]
    new = [t for t in tasks if (t.get("origin") or "").strip().lower() == "new scope"]
    added = [t["id"] for t in new]
    added += [rid for rid in new_scope_ids(issues, by_id) if rid not in added]
    return {"breakdown": len(tasks) - len(new),
            "new_scope": len(added), "new_scope_ids": added}


def task_order(tasks):
    index = {t["id"]: i for i, t in enumerate(tasks)}
    return lambda tid: index[tid]


def cell(text):
    return str(text).replace("|", "\\|").replace("\n", " ")


def markdown(data):
    lines = ["Rendered by what's next from the backlog, Jira and GitHub on %s. "
             "Do not edit cells here: claim a task in Jira, and the next run "
             "rewrites this tab." % data["date"], ""]
    for area in data["areas"]:
        lines += ["## %s" % area["title"], "",
                  "| " + " | ".join(COLUMNS) + " |",
                  "|" + "---|" * len(COLUMNS)]
        for r in area["rows"]:
            ticket = ", ".join("[%s](%s%s)" % (k, BROWSE, k) for k in r["tickets"])
            prs = ", ".join("[%s](%s)" % (p["pr"], p["url"]) for p in r["prs"])
            status = "; ".join([r["status"]] * bool(r["status"]) +
                               [p["status"] for p in r["prs"]])
            values = (r["id"], r["task"], ", ".join(r["repos"]), r["owner"],
                      ticket, prs, status, ", ".join(r["depends_on"]))
            lines.append("| " + " | ".join(cell(v) for v in values) + " |")
        lines.append("")
    lines += ["## Out of sync", ""]
    lines += ["- " + line for line in data["out_of_sync"]] or ["Nothing."]
    if data["skipped"]:
        lines += ["", "Not D1 work (no ticket in the Jira export): " +
                  ", ".join("[%s](%s)" % (s["pr"], s["url"]) if s.get("url") else s["pr"]
                            for s in data["skipped"]) + "."]
    return "\n".join(lines) + "\n"


def self_test():
    def task(tid, deps=()):
        return {"id": tid, "title": "T" + tid, "area": "g1", "repos": ["x"],
                "estimate_days": 1, "depends_on": list(deps), "depends_on_unresolved": [],
                "status": "", "notes": ""}

    def issue(key, summary, status):
        return {"key": key, "fields": {"summary": SUMMARY_PREFIX + summary,
                                       "status": {"name": status},
                                       "assignee": {"displayName": "A"}}}
    backlog = {"areas": [{"id": "g1", "title": "A"}], "tasks": [task("3"), task("4", ["3"])]}
    # 3.1 is Done and 3.2 has no ticket yet: task 3 is not done.
    got = build(backlog, [issue("ZZ-1", "3.1 first step", "Done")], {"prs": []}, None)
    assert "3" not in got["done"], got["done"]
    rows = [(r["id"], r.get("done")) for a in got["areas"] for r in a["rows"]]
    assert ("3.1", True) in rows and ("3", None) in rows, rows
    progress = {r["status"]: r["tasks"] for r in got["progress"]}
    assert progress == {"done": 0, "in progress": 1, "not started": 1}, progress
    # The task-level ticket of 3 is Done: the task is done, whatever its steps.
    got = build(backlog, [issue("ZZ-1", "3.1 first step", "Done"),
                          issue("ZZ-2", "3 the task", "Done")], {"prs": []}, None)
    assert "3" in got["done"], got["done"]
    # A PR into development/* with no D1 ticket, and the PR stacked on it,
    # are not D1 work; a PR stacked on a D1 ticket branch is.
    prs = {"prs": [
        {"pr": "a#1", "base": "development/8.3", "head": "improvement/YY-1-x",
         "ticket": "YY-1", "status": "Open", "url": "u1"},
        {"pr": "a#2", "base": "improvement/YY-1-x", "head": "improvement/YY-2-y",
         "ticket": "YY-2", "status": "Open", "url": "u2"},
        {"pr": "b#3", "base": "improvement/ZZ-1-z", "head": "improvement/ZZ-1-w",
         "ticket": "ZZ-1", "status": "Open", "url": "u3"}]}
    got = build(backlog, [issue("ZZ-1", "3 the task", "Ongoing")], prs, None)
    assert [s["pr"] for s in got["skipped"]] == ["a#1", "a#2"], got["skipped"]
    assert not got["out_of_sync"], got["out_of_sync"]
    # A ticket tracked as a row the backlog lacks does not fall back to
    # another row its text names: it matches no task.
    tracked = issue("ZZ-9", "New scope work", "Ongoing")
    tracked["fields"]["description"] = "Tickets tab, row 5. Builds on row 4."
    got = build(backlog, [tracked], {"prs": []}, None)
    assert "ZZ-9 matches no task of the backlog" in got["out_of_sync"], got["out_of_sync"]
    # A task with only its own ticket: done when that ticket is Done.
    got = build(backlog, [issue("ZZ-3", "3 the task", "Done")], {"prs": []}, None)
    assert "3" in got["done"], got["done"]
    print("self-test ok")


def main(argv):
    if argv[:1] == ["--self-test"]:
        self_test()
        return 0
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--backlog", required=True)
    parser.add_argument("--jira", required=True)
    parser.add_argument("--prs", required=True)
    parser.add_argument("--sprint")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        with open(args.jira, encoding="utf-8") as f:
            issues = json.load(f)
        with open(args.prs, encoding="utf-8") as f:
            prs_doc = json.load(f)
        backlog = load_backlog(args.backlog)
    except OSError as e:
        print("cannot read %s: %s" % (e.filename, e.strerror), file=sys.stderr)
        return 2
    except ValueError as e:
        print("cannot parse: %s" % e, file=sys.stderr)
        return 2
    if isinstance(issues, dict):
        # A raw Jira search answer wraps the list in "issues".
        issues = issues.get("issues", [])
    data = build(backlog, issues, prs_doc, args.sprint)
    if args.json:
        print(json.dumps(data, indent=2))
    else:
        sys.stdout.write(markdown(data))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
