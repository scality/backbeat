#!/usr/bin/env python3
"""Build the update of one chart of the Progress tab from render_tickets.py.

Standard library only. Each chart is a widget whose rows sit inline in its
code, as `data:[...]`. This script swaps those rows and prints the whole
`update` payload: a `draft` of the new code on top of `basePub`, which the
docs connector validates and publishes in one call, and refuses when
someone changed the widget since the read.

    python3 -I charts.py progress --render <render.json> --code <code.txt> --pub <n>
    python3 -I charts.py scope --render <render.json> --code <code.txt> --pub <n>
        --pivots <journal.md> [--today YYYY-MM-DD]
    python3 -I charts.py --self-test

<render.json> is the output of `render_tickets.py --json`. <code.txt> holds
`value.code` of a widget read, and <n> its `value.pub`.

progress replaces every row. scope keeps the history: the widget rows are
the only record of past weeks, so it rewrites only the breakdown and
new-scope counts of the current week, and recounts the pivots of every
week from the Pivots table (<journal.md>, the Journal tab of the tech
spec exported to Markdown). Weeks start on 2026-10-06, a Tuesday, last
seven days and follow UTC dates: the widget rows already in the doc are keyed to that anchor, so
a chart week runs Tuesday to Monday on purpose. When the rows
do not change, it prints "unchanged" and no payload.
"""
import argparse
import datetime
import json
import sys

FIRST_WEEK = datetime.date(2026, 10, 6)
PIVOT_COLUMNS = ["date", "what changed", "why", "tasks affected"]


def split_rows(code):
    """The rows in the code, and the text before and after them."""
    start = code.index("data:[") + len("data:")
    rows, length = json.JSONDecoder().raw_decode(code[start:])
    return code[:start], rows, code[start + length:]


def week_of(day):
    return FIRST_WEEK + datetime.timedelta(days=(day - FIRST_WEEK).days // 7 * 7)


def pivot_dates(markdown):
    """The Date cells of the Pivots table."""
    dates, inside = [], False
    for line in markdown.splitlines():
        cells = [c.strip().lower() for c in line.strip().strip("|").split("|")]
        if line.lstrip().startswith("|") and cells == PIVOT_COLUMNS:
            inside = True
        elif inside and line.lstrip().startswith("|"):
            try:
                dates.append(datetime.date.fromisoformat(cells[0]))
            except ValueError:
                pass  # the separator row, or a date someone typed another way
        elif inside:
            break
    return dates


def scope_rows(old, scope, dates, today):
    """Old rows with this week's counts set and every week's pivots
    recounted. A week with no run carries the counts of the week before."""
    counts = {(r["week"], r["series"]): r["count"] for r in old}
    # Keep any later week that a run in another time zone already wrote.
    end = max([week_of(today)] + [datetime.date.fromisoformat(r["week"]) for r in old])
    weeks, day = [], FIRST_WEEK
    while day <= end:
        weeks.append(day.isoformat())
        day += datetime.timedelta(days=7)
    current = week_of(today).isoformat()
    rows, last = [], {"breakdown": 0, "new scope": 0}
    for week in weeks:
        for series in ("breakdown", "new scope"):
            if week == current:
                value = scope["breakdown"] if series == "breakdown" else scope["new_scope"]
            else:
                value = counts.get((week, series), last[series])
            last[series] = value
            rows.append({"week": week, "series": series, "count": value})
        pivots = sum(1 for d in dates if week_of(d).isoformat() == week)
        rows.append({"week": week, "series": "pivots", "count": pivots})
    return rows


def payload(before, rows, after, pub):
    code = before + json.dumps(rows, separators=(",", ":"), ensure_ascii=False) + after
    return {"kind": "draft", "basePub": pub, "code": code}


def self_test():
    code = ("x data:[{\"week\":\"2026-10-06\",\"series\":\"breakdown\",\"count\":108},"
            "{\"week\":\"2026-10-06\",\"series\":\"new scope\",\"count\":0},"
            "{\"week\":\"2026-10-06\",\"series\":\"pivots\",\"count\":0}]}}}> y")
    before, old, after = split_rows(code)
    assert before.endswith("data:") and after.startswith("}}}>") and len(old) == 3
    md = ("| Date | What changed | Why | Tasks affected |\n|---|---|---|---|\n"
          "| 2026-10-22 | a | b | 3 |\n| 2026-10-22 | c | d | 22a |\n\nafter\n")
    dates = pivot_dates(md)
    assert dates == [datetime.date(2026, 10, 22)] * 2
    rows = scope_rows(old, {"breakdown": 110, "new_scope": 5}, dates,
                      datetime.date(2026, 10, 22))
    got = {(r["week"], r["series"]): r["count"] for r in rows}
    assert got[("2026-10-06", "breakdown")] == 108
    assert got[("2026-10-13", "breakdown")] == 108  # carried over a week with no run
    assert got[("2026-10-20", "breakdown")] == 110
    assert got[("2026-10-20", "new scope")] == 5
    assert got[("2026-10-20", "pivots")] == 2 and got[("2026-10-06", "pivots")] == 0
    assert week_of(datetime.date(2026, 10, 12)).isoformat() == "2026-10-06"
    later = rows + [{"week": "2026-10-27", "series": "breakdown", "count": 111}]
    kept = scope_rows(later, {"breakdown": 110, "new_scope": 5}, dates,
                      datetime.date(2026, 10, 22))
    assert {"week": "2026-10-27", "series": "breakdown", "count": 111} in kept
    p = payload(before, rows, after, 4)
    assert p["basePub"] == 4 and split_rows(p["code"])[1] == rows
    print("self-test ok")


def main(argv):
    if argv[:1] == ["--self-test"]:
        self_test()
        return 0
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("chart", choices=("progress", "scope"))
    parser.add_argument("--render", required=True)
    parser.add_argument("--code", required=True)
    parser.add_argument("--pub", required=True, type=int)
    parser.add_argument("--pivots")
    parser.add_argument("--today", type=datetime.date.fromisoformat,
                        default=datetime.datetime.now(datetime.timezone.utc).date())
    args = parser.parse_args(argv)
    if args.chart == "scope" and not args.pivots:
        parser.error("scope needs --pivots <the Journal tab exported to Markdown>")
    try:
        with open(args.render, encoding="utf-8") as f:
            render = json.load(f)
        with open(args.code, encoding="utf-8") as f:
            before, old, after = split_rows(f.read())
        markdown = ""
        if args.pivots:
            with open(args.pivots, encoding="utf-8") as f:
                markdown = f.read()
    except OSError as e:
        print("cannot read %s: %s" % (e.filename, e.strerror), file=sys.stderr)
        return 2
    except ValueError as e:
        print("cannot parse: %s (is <code.txt> the widget code, with data:[...]?)" % e,
              file=sys.stderr)
        return 2
    if args.chart == "progress":
        rows = render["progress"]
    else:
        rows = scope_rows(old, render["scope"], pivot_dates(markdown), args.today)
    if rows == old:
        print("unchanged")
        return 0
    print(json.dumps(payload(before, rows, after, args.pub), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
