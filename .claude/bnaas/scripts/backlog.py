#!/usr/bin/env python3
"""Query the BNaaS D1 backlog, the Backlog tab of the tech spec doc.

Standard library only. The backlog is a file: the Backlog tab exported to
Markdown, or the JSON that import_breakdown.py writes. Run it as:

    python3 -I .claude/bnaas/scripts/backlog.py --backlog <file> list
    python3 -I .claude/bnaas/scripts/backlog.py --backlog <file> deps 22
    python3 -I .claude/bnaas/scripts/backlog.py --backlog <file> ready --done 1,2,3
    python3 -I .claude/bnaas/scripts/backlog.py --backlog <file> blocked --done 1,2,3
    python3 -I .claude/bnaas/scripts/backlog.py --backlog <file> unknown --done 1,2,3
    python3 -I .claude/bnaas/scripts/backlog.py --backlog <file> check

ready, blocked and unknown split the open tasks into three lists. The done
set is --done plus the tasks whose Status cell is "done". A task
with a dependency text that names no task (the Notes cell starts with
"Dependencies unknown:") is never ready: it goes to unknown until someone
fixes its Depends on cell in the Backlog tab.
"""
import json
import re
import sys

USAGE = ("usage: backlog.py --backlog <file.md|file.json> "
         "list | deps <id> | ready|blocked|unknown --done <ids> | check")
# The columns of the Backlog tab, in order. Status may follow them.
COLUMNS = ("ID", "Task", "Area", "Repository", "Estimate (days)", "Depends on",
           "Origin", "Notes")
STATUS = "Status"
# The Notes prefix for dependency texts that name no task.
UNKNOWN_PREFIX = "Dependencies unknown:"


def split_row(line):
    """The cells of one Markdown table row. A backslash escapes the next
    character, so an escaped pipe stays in its cell."""
    cells, buf, i = [], [], 0
    line = line.strip()
    if line.startswith("|"):
        line = line[1:]
    while i < len(line):
        c = line[i]
        if c == "\\" and i + 1 < len(line):
            buf.append(line[i + 1])
            i += 2
            continue
        if c == "|":
            cells.append("".join(buf).strip())
            buf = []
        else:
            buf.append(c)
        i += 1
    if "".join(buf).strip():
        cells.append("".join(buf).strip())
    return cells


def number(text, tid):
    """An estimate in days: "1.5", "1,5" or "2d". An empty cell is 0."""
    text = text.strip().replace(",", ".")
    if text.endswith("d"):
        text = text[:-1].strip()
    if not text:
        return 0
    try:
        value = float(text)
    except ValueError:
        raise ValueError("row %s: Estimate (days) is not a number of days" % tid)
    return int(value) if value == int(value) else value


def ids(text):
    """Task ids from a Depends on cell: separated by commas, "and" or
    spaces. A step id like 3.1 stands for its task, 3."""
    out = []
    for x in re.split(r"[\s,]+|\band\b", text):
        x = x.strip().split(".")[0]
        if x and x not in out:
            out.append(x)
    return out


def from_markdown(text):
    """Read the first table whose header is COLUMNS, with or without a
    last Status column. A Status cell "done" marks a task done with no
    ticket, such as the work that came before the Jira epic."""
    lines = text.splitlines()
    want = [c.lower() for c in COLUMNS]
    for start, line in enumerate(lines):
        header = [c.lower() for c in split_row(line)]
        if line.lstrip().startswith("|") and header in (want, want + [STATUS.lower()]):
            break
    else:
        raise ValueError("no table with the columns %s" % " | ".join(COLUMNS))
    areas, tasks = [], []
    for line in lines[start + 2:]:
        if not line.lstrip().startswith("|"):
            break
        cells = split_row(line) + [""] * (len(COLUMNS) + 1)
        tid, title, area, repos, est, deps, origin, notes, status = cells[:len(COLUMNS) + 1]
        if not tid:
            continue
        if area not in [a["title"] for a in areas]:
            areas.append({"id": "g%d" % (len(areas) + 1), "title": area})
        task = {"id": tid, "title": title,
                "area": next(a["id"] for a in areas if a["title"] == area),
                "repos": [r.strip() for r in repos.split(",") if r.strip()],
                "estimate_days": number(est, tid), "depends_on": ids(deps),
                "origin": origin or "breakdown", "notes": notes,
                "status": status.strip().lower()}
        if notes.startswith(UNKNOWN_PREFIX):
            task["depends_on_unresolved"] = [
                u.strip() for u in notes[len(UNKNOWN_PREFIX):].split(";") if u.strip()]
        tasks.append(task)
    return {"areas": areas, "tasks": tasks}


def load_backlog(path):
    """The backlog as {"areas": [...], "tasks": [...]}, from a Markdown
    export of the Backlog tab or from the JSON of import_breakdown.py."""
    with open(path, encoding="utf-8") as f:
        text = f.read()
    if path.endswith(".json"):
        data = json.loads(text)
    else:
        data = from_markdown(text)
    for t in data["tasks"]:
        t.setdefault("depends_on_unresolved", [])
        t.setdefault("origin", "breakdown")
        t.setdefault("status", "")
    return data


def load(path):
    data = load_backlog(path)
    tasks = data["tasks"]
    return data["areas"], tasks, {t["id"]: t for t in tasks}


def transitive_deps(by_id, tid):
    seen, order, todo = set(), [], list(by_id[tid]["depends_on"])
    while todo:
        d = todo.pop(0)
        if d in seen:
            continue
        seen.add(d)
        order.append(d)
        todo.extend(by_id[d]["depends_on"])
    return order


def fail(message):
    print(message, file=sys.stderr)
    sys.exit(2)


def done_set(args, by_id):
    """Read --done ids. Pass task ids, not steps: a task counts as done only
    when all its steps are done. A step id like "3.1" still maps to task 3
    as a convenience."""
    done = set()
    if "--done" not in args:
        fail("%s needs --done <ids>, for example --done 1,2,3 (or --done '' for none)"
             % args[0])
    if args.index("--done") + 1 >= len(args):
        fail("--done needs a comma-separated list of task ids")
    raw = args[args.index("--done") + 1]
    for x in raw.split(","):
        x = x.strip()
        if not x:
            continue
        if x in by_id:
            done.add(x)
        elif x.split(".")[0] in by_id:
            done.add(x.split(".")[0])
        else:
            fail("unknown task id in --done: %s" % x)
    return done


def fmt(t):
    return "%-4s %-4s %-28s %5sd  deps: %s" % (
        t["id"], t["area"], ",".join(t["repos"]), t["estimate_days"],
        ",".join(t["depends_on"]) or "none")


def cmd_check(areas, tasks, by_id):
    bad = 0
    ids = [t["id"] for t in tasks]
    if len(ids) != len(set(ids)):
        print("duplicate task ids")
        bad += 1
    edges = unresolved = 0
    for t in tasks:
        for d in t["depends_on"]:
            edges += 1
            if d not in by_id:
                print("task %s depends on missing id %s" % (t["id"], d))
                bad += 1
        unresolved += len(t["depends_on_unresolved"])
    # Cycle detection by depth-first search with colors.
    color = {}

    def visit(tid, path):
        color[tid] = "grey"
        for d in by_id[tid]["depends_on"]:
            if d not in by_id:
                continue
            if color.get(d) == "grey":
                print("cycle: %s" % " -> ".join(path + [d]))
                return True
            if d not in color and visit(d, path + [d]):
                return True
        color[tid] = "black"
        return False

    for t in tasks:
        if t["id"] not in color and visit(t["id"], [t["id"]]):
            bad += 1
    print("%d tasks, %d areas, %d dependencies resolved, %d unresolved texts"
          % (len(tasks), len(areas), edges, unresolved))
    print("ok" if not bad else "%d problem(s)" % bad)
    return 0 if not bad else 1


def main(argv):
    if "--backlog" not in argv or argv.index("--backlog") + 1 >= len(argv):
        fail(USAGE)
    i = argv.index("--backlog")
    path = argv[i + 1]
    argv = argv[:i] + argv[i + 2:]
    if not argv or argv[0] not in ("list", "deps", "ready", "blocked", "unknown", "check"):
        fail(USAGE)
    try:
        areas, tasks, by_id = load(path)
    except OSError as e:
        fail("cannot read %s: %s" % (path, e.strerror))
    except ValueError as e:
        fail("%s: %s" % (path, e))
    cmd = argv[0]
    if cmd == "check":
        sys.exit(cmd_check(areas, tasks, by_id))
    if cmd == "list":
        for t in tasks:
            print(fmt(t))
    elif cmd == "deps":
        if len(argv) < 2:
            fail("deps needs a task id, for example: backlog.py deps 22")
        tid = argv[1]
        if tid not in by_id:
            fail("unknown task %s" % tid)
        for d in transitive_deps(by_id, tid):
            print("%-4s %s" % (d, by_id[d]["title"]))
    else:
        # A Status cell "done" in the tab counts as done too.
        done = done_set(argv, by_id) | {t["id"] for t in tasks if t["status"] == "done"}
        for t in tasks:
            if t["id"] in done:
                continue
            missing = [d for d in t["depends_on"] if d not in done]
            unknown = t["depends_on_unresolved"]
            if unknown:
                if cmd == "unknown":
                    print("%-4s %s  dependencies unknown: %s"
                          % (t["id"], t["title"], "; ".join(unknown)))
            elif cmd == "ready" and not missing:
                print("%-4s %s" % (t["id"], t["title"]))
            elif cmd == "blocked" and missing:
                print("%-4s %s  missing: %s" % (t["id"], t["title"], ",".join(missing)))


if __name__ == "__main__":
    main(sys.argv[1:])
