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
    python3 -I .claude/bnaas/scripts/backlog.py --self-test

ready, blocked and unknown split the open tasks into three lists. The done
set is --done plus the tasks whose Status cell is "done". A task
with a dependency text that names no task (the Notes cell starts with
"Dependencies unknown:") is never ready: it goes to unknown until someone
fixes its Depends on cell in the Backlog tab. A dropped task (Status
"dropped", or a Notes cell whose first word is "Dropped:") is in none of the three
lists, and it is no longer required: a dependency on it does not block.
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
# The Status cell of a task the team dropped, and of a standing task, whose
# ticket stays open between its fixes. Notes is free text, so only these
# first words mark a task there, case-insensitive: "Dropped: ..." and
# "Standing: ...". Any other text is a plain note.
DROPPED = "dropped"
STANDING = "standing"
DROPPED_NOTE = "dropped:"
STANDING_NOTE = "standing:"


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
    """An estimate in days: "1.5", "1,5" or "2d". An empty cell is 0, and
    so is text such as "TBD", with a warning: a typed estimate must not
    stop the commands that never use it."""
    raw, text = text, text.strip().replace(",", ".")
    if text.endswith("d"):
        text = text[:-1].strip()
    if not text:
        return 0
    try:
        value = float(text)
    except ValueError:
        print("row %s: Estimate (days) %r is not a number of days, read as 0"
              % (tid, raw.strip()), file=sys.stderr)
        return 0
    return int(value) if value == int(value) else value


def ids(text):
    """Ids from a Depends on cell: separated by commas, "and" or spaces.
    "None" means no dependency, as in the breakdown. Step ids stay as
    typed: from_markdown resolves them once it knows every task id."""
    out = []
    for x in re.split(r"[\s,]+|\band\b", text):
        x = x.strip()
        if x and x.lower() != "none" and x not in out:
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
        cells = split_row(line)
        if len(cells) > len(header):
            # An unescaped | shifts every later cell: refuse the row.
            raise ValueError("row %s: %d cells, expected %d (unescaped |?)"
                             % (cells[0], len(cells), len(header)))
        cells += [""] * (len(COLUMNS) + 1)
        tid, title, area, repos, est, deps, origin, notes, status = cells[:len(COLUMNS) + 1]
        if not tid:
            if title:
                # A task added and not numbered would vanish from every list.
                print("row with no ID skipped: %s" % title, file=sys.stderr)
            continue
        if area not in [a["title"] for a in areas]:
            areas.append({"id": "g%d" % (len(areas) + 1), "title": area})
        task = {"id": tid, "title": title,
                "area": next(a["id"] for a in areas if a["title"] == area),
                "repos": [r.strip() for r in repos.split(",") if r.strip()],
                "estimate_days": number(est, tid), "depends_on": ids(deps),
                "origin": origin or "breakdown", "notes": notes,
                "status": status.strip().lower()}
        # People type the prefix by hand: ignore its case and leading blanks.
        note = notes.lstrip()
        if note.lower().startswith(UNKNOWN_PREFIX.lower()):
            task["depends_on_unresolved"] = [
                u.strip() for u in note[len(UNKNOWN_PREFIX):].split(";") if u.strip()]
        tasks.append(task)
    # A step id like 3.1 stands for its task, resolved only now, when every
    # task id is known: only when the part after the last dot is a number
    # and the task exists. A task id (3.3a) stays itself, and a typo (3.3b)
    # stays too, so that check reports it as a missing id.
    known = {t["id"] for t in tasks}

    def resolve(d):
        task, _, step = d.rpartition(".")
        return task if d not in known and step.isdigit() and task in known else d
    for t in tasks:
        t["depends_on"] = list(dict.fromkeys(resolve(d) for d in t["depends_on"]))
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


def is_dropped(task):
    """True for a task the team dropped: its Status cell is "dropped", or
    the first word of its Notes cell is "Dropped:". It is never ready, never
    started and never counted in the progress."""
    return (task.get("status") or "").strip().lower() == DROPPED or \
        (task.get("notes") or "").strip().lower().startswith(DROPPED_NOTE)


def is_standing(task):
    """True for a standing task: its Status cell is "standing", or its Notes
    cell's first word is "Standing:". Its ticket stays open with no PR between
    two fixes, so it is never offered as work to continue."""
    return (task.get("status") or "").strip().lower() == STANDING or \
        (task.get("notes") or "").strip().lower().startswith(STANDING_NOTE)


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
        todo.extend(by_id[d]["depends_on"] if d in by_id else [])
    return order


def fail(message):
    print(message, file=sys.stderr)
    sys.exit(2)


def done_set(args, by_id):
    """Read --done ids. Pass task ids, not steps: a task counts as done only
    when all its steps are done, so a step id like "3.1" is refused."""
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
            fail("pass task ids to --done, not steps: %s" % x)
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

    # White (no color), grey (on the current path), black (done). A task
    # turns black when its visit ends, cycle or not, so a later task that
    # reaches it is not reported as a cycle.
    def visit(tid, path):
        color[tid] = "grey"
        found = False
        for d in by_id[tid]["depends_on"]:
            if d not in by_id:
                continue
            if color.get(d) == "grey":
                # Only the cycle, from the task seen twice.
                print("cycle: %s" % " -> ".join(path[path.index(d):] + [d]))
                found = True
            elif d not in color and visit(d, path + [d]):
                found = True
        color[tid] = "black"
        return found

    for t in tasks:
        if t["id"] not in color and visit(t["id"], [t["id"]]):
            bad += 1
    print("%d tasks (%d dropped), %d areas, %d dependencies resolved, %d unresolved texts"
          % (len(tasks), sum(map(is_dropped, tasks)), len(areas), edges, unresolved))
    print("ok" if not bad else "%d problem(s)" % bad)
    return 0 if not bad else 1


def self_test():
    import contextlib
    import io

    def task(tid, deps):
        return {"id": tid, "title": tid, "area": "g1", "repos": [], "estimate_days": 1,
                "depends_on": deps, "depends_on_unresolved": []}
    tasks = [task("1", ["2"]), task("2", ["1"]), task("3", ["1"])]
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cmd_check([{"id": "g1"}], tasks, {t["id"]: t for t in tasks})
    cycles = [line for line in out.getvalue().splitlines() if line.startswith("cycle:")]
    assert code == 1 and cycles == ["cycle: 1 -> 2 -> 1"], out.getvalue()
    assert "1 problem(s)" in out.getvalue(), out.getvalue()
    # A typed estimate reads as 0, and a row with no ID is skipped, both
    # with a warning.
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        assert number("TBD", "2") == 0
        assert number("1,5d", "2") == 1.5
    assert "row 2: Estimate (days) 'TBD' is not a number of days" in err.getvalue(), err
    # The "Dependencies unknown:" prefix matches whatever its case.
    table = ("| " + " | ".join(COLUMNS) + " |\n|" + " --- |" * len(COLUMNS) + "\n"
             "| 1 | One | A | x | 1 |  | breakdown | dependencies unknown: X; Y |\n")
    got = from_markdown(table)["tasks"][0]["depends_on_unresolved"]
    assert got == ["X", "Y"], got
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        got = from_markdown(table + "|  | Forgot its number | A | x | 1 |  | new scope |  |\n")
    assert len(got["tasks"]) == 1 and "row with no ID skipped: Forgot its number" in \
        err.getvalue(), err.getvalue()
    # A row with an unescaped | has one cell too many: it is refused.
    try:
        from_markdown(table + "| 2 | Two | A | x | 1 |  | breakdown | a | b |\n")
        raise AssertionError("a row with too many cells was read")
    except ValueError as e:
        assert "row 2: 9 cells, expected 8" in str(e), e
    # Only the cycle is printed, not the path that led to it.
    tasks = [task("1", ["5"]), task("5", ["6"]), task("6", ["5"])]
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        cmd_check([{"id": "g1"}], tasks, {t["id"]: t for t in tasks})
    assert "cycle: 5 -> 6 -> 5" in out.getvalue(), out.getvalue()
    # A task id with a dot (3.3a) stays a dependency; a step of a task (3.1)
    # stands for the task; "None" is no dependency.
    rows = ("| 3 | Three | A | x | 1 |  | breakdown |  |\n"
            "| 3.3a | Kms image | A | x | 1 | 3 | new scope |  |\n"
            "| 9 | Nine | A | x | 1 | 3.3a, 3.1 | breakdown |  |\n"
            "| 10 | Ten | A | x | 1 | None | breakdown |  |\n"
            "| 11 | Eleven | A | x | 1 | 3.3b, 3.3a.1 | breakdown |  |\n")
    header = "| " + " | ".join(COLUMNS) + " |\n|" + " --- |" * len(COLUMNS) + "\n"
    deps = {t["id"]: t["depends_on"] for t in from_markdown(header + rows)["tasks"]}
    assert deps["9"] == ["3.3a", "3"] and deps["10"] == [], deps
    # A typo of a dotted id stays as typed, so check reports it; a step of a
    # lettered task counts for that task.
    assert deps["11"] == ["3.3b", "3.3a"], deps
    # Only the exact markers drop a task or make it standing: Notes is free text.
    assert is_dropped({"notes": "Dropped: no epic releases (2026-10-22)"})
    assert is_dropped({"notes": "dropped: case does not matter"})
    assert not is_dropped({"notes": "Dropped on 2026-10-22: the old marker"})
    assert is_dropped({"status": "Dropped"})
    assert not is_dropped({"notes": "Dropped the Kafka step, see 3.1"})
    assert is_standing({"notes": "Standing: its ticket stays open between fixes"})
    assert not is_standing({"notes": "Standing up a test cluster first"})
    print("self-test ok")


def main(argv):
    if argv[:1] == ["--self-test"]:
        self_test()
        return
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
            print("%-4s %s" % (d, by_id[d]["title"] if d in by_id else "(missing from the backlog)"))
    else:
        # A Status cell "done" in the tab counts as done too.
        done = done_set(argv, by_id) | {t["id"] for t in tasks if t["status"] == "done"}
        # A dropped task is no longer required, so it never blocks.
        done |= {t["id"] for t in tasks if is_dropped(t)}
        for t in tasks:
            if t["id"] in done or is_dropped(t):
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
