#!/usr/bin/env python3
"""Find the team sprint of a date, or pick the active sprint from Jira.

Usage:
  sprint.py [YYYY-MM-DD]             the sprint of that date (default: today)
  sprint.py --jira '<json>' [date]   the active team sprint in the value of
                                     the sprint field (customfield_10004)
                                     of Jira tickets: a JSON array of
                                     sprint objects, or null
  sprint.py --jira - [date]          the same, with the JSON on stdin, for
                                     a value that holds a quote
  sprint.py --self-test

A sprint lasts two weeks. It starts on a Monday, ends on the Friday of the
second week, and is named after its Monday ("Hopocalypse 2026-10-19").
Jira closes a sprint on the Monday morning after that Friday, so the
weekend still belongs to the sprint that ends, and so does it here.

Jira is the source of truth. This date math checks the name that Jira
gives, and finds the sprint name to search for when no D1 ticket is in an
open sprint. --jira looks only at the sprints of the team board. It exits
1 when the value holds no active sprint of that board, for example only a
future one, and 2 when the value is not an array of objects or null.
"""
import contextlib
import datetime
import io
import json
import sys

PREFIX = "Hopocalypse "
# The Monday of a known sprint. Every other sprint is a whole number of
# two-week periods away from it.
ANCHOR = datetime.date(2026, 10, 19)
# The Jira board of the team sprints.
BOARD = 339


def sprint_of(day):
    start = ANCHOR + datetime.timedelta(days=(day - ANCHOR).days // 14 * 14)
    return {"name": PREFIX + start.isoformat(), "start": start.isoformat(),
            "end": (start + datetime.timedelta(days=11)).isoformat()}


def active_sprint(field, day):
    """The active team sprint of a Jira sprint field, or None.

    openSprints() also matches future sprints, so the state decides, and
    a ticket can sit in the sprint of another board too. If several team
    sprints are active, prefer the one that the dates expect."""
    expected = sprint_of(day)["name"]
    active = [s for s in field or []
              if s.get("state") == "active" and s.get("boardId") == BOARD]
    active.sort(key=lambda s: s.get("name") != expected)
    if not active:
        return None
    s = active[0]
    return {"id": s.get("id"), "name": s.get("name"), "board": s.get("boardId"),
            "expected": expected, "matches_dates": s.get("name") == expected}


def self_test():
    cases = [
        ("2026-10-19", "Hopocalypse 2026-10-19", "2026-10-30"),
        ("2026-10-22", "Hopocalypse 2026-10-19", "2026-10-30"),
        ("2026-10-30", "Hopocalypse 2026-10-19", "2026-10-30"),
        ("2026-11-01", "Hopocalypse 2026-10-19", "2026-10-30"),
        ("2026-11-02", "Hopocalypse 2026-11-02", "2026-11-13"),
        # Before the anchor: must match the closed sprint that Jira shows.
        ("2026-10-10", "Hopocalypse 2026-10-05", "2026-10-16"),
        ("2027-01-01", "Hopocalypse 2026-12-28", "2027-01-08"),
    ]
    for day, name, end in cases:
        got = sprint_of(datetime.date.fromisoformat(day))
        assert (got["name"], got["end"]) == (name, end), (day, got)

    closed = {"id": 12750, "name": "Hopocalypse 2026-10-05", "state": "closed", "boardId": 339}
    active = {"id": 12767, "name": "Hopocalypse 2026-10-19", "state": "active", "boardId": 339}
    future = {"id": 12780, "name": "Hopocalypse 2026-11-02", "state": "future", "boardId": 339}
    other = {"id": 12001, "name": "Other team 2026-10-12", "state": "active", "boardId": 12}
    day = datetime.date(2026, 10, 22)
    got = active_sprint([closed, active, future], day)
    assert got["id"] == 12767 and got["matches_dates"], got
    assert active_sprint([other, active], day)["id"] == 12767
    assert active_sprint([other], day) is None
    moved = dict(active, name="Hopocalypse 2026-10-20")
    assert not active_sprint([moved], day)["matches_dates"]
    assert active_sprint([closed, future], day) is None
    assert active_sprint([future], day) is None
    assert active_sprint(None, day) is None
    with contextlib.redirect_stderr(io.StringIO()):
        assert main(["--jira", "null", "2026-10-22"]) == 1
        assert main(["--jira", json.dumps([future]), "2026-10-22"]) == 1
        assert main(["--jira", json.dumps(active), "2026-10-22"]) == 2
        assert main(["--jira", '["Hopocalypse 2026-10-19"]', "2026-10-22"]) == 2
        assert main(["--jira"]) == 2
        assert main(["2026-13-01"]) == 2
    # On stdin, a sprint name or goal with an apostrophe needs no shell quoting.
    quoted = dict(active, goal="Ship the team's first epic tags")
    stdin, out = sys.stdin, io.StringIO()
    try:
        sys.stdin = io.StringIO(json.dumps([quoted]))
        with contextlib.redirect_stdout(out):
            assert main(["--jira", "-", "2026-10-22"]) == 0
    finally:
        sys.stdin = stdin
    assert json.loads(out.getvalue())["id"] == 12767, out.getvalue()
    print("ok: %d dates, 13 Jira fields and arguments" % len(cases))


def main(argv):
    if argv[:1] == ["--self-test"]:
        self_test()
        return 0
    jira = argv[:1] == ["--jira"]
    try:
        if jira:
            text = sys.stdin.read() if argv[1] == "-" else argv[1]
            field, argv = json.loads(text), argv[2:]
            if not (field is None or isinstance(field, list)
                    and all(isinstance(s, dict) for s in field)):
                raise ValueError("pass the bare sprint array, a JSON array of objects")
        day = datetime.date.fromisoformat(argv[0]) if argv else datetime.date.today()
    except (IndexError, ValueError) as e:
        print("%s\n%s" % (e, __doc__), file=sys.stderr)
        return 2
    if not jira:
        print(json.dumps(sprint_of(day)))
        return 0
    got = active_sprint(field, day)
    if got is None:
        print("No active sprint in the field. Search Jira for the sprint %r by name."
              % sprint_of(day)["name"], file=sys.stderr)
        return 1
    print(json.dumps(got))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
