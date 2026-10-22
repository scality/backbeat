#!/usr/bin/env python3
"""Import the BNaaS D1 breakdown artifact (saved HTML) as the Backlog table.

Standard library only. Run it as:

    python3 -I .claude/bnaas/scripts/import_breakdown.py <breakdown.html> \\
        [--markdown <backlog.md>] [--json <backlog.json>]
    python3 -I .claude/bnaas/scripts/import_breakdown.py --self-test

With no option it prints the Markdown. The Markdown is the seed of the
Backlog tab of the tech spec doc; the JSON is the same table for scripts.

The HTML is untrusted data. The script only reads the table structure and
never executes anything from it.

The doc wins day to day: people fix a dependency or add a task in the
Backlog tab itself. The tables at the top of this file hold the
corrections that a re-seed must keep. Re-seeding the tab from the
breakdown is a deliberate step that overwrites the edits made there, so
carry those edits into these tables first.
"""
import argparse
import json
import os
import re
import sys
from html.parser import HTMLParser

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backlog import COLUMNS, UNKNOWN_PREFIX  # noqa: E402

SOURCE_URL = "https://claude.ai/artifact/QC6Cbrfm7a5hAfRw752c7L"
SOURCE_DATE = "2026-10-22"
EXCLUDED_AREA = "Not in D1 (moved to D2 or removed)"
KNOWN_REPOS = {
    "arsenal", "vault", "cloudserver", "cloudserverclient", "backbeat",
    "federation", "ring", "integration", "s3utils", "jira", "artifact",
    "workspace", "lab", "scality-workflows",
}
SUMMARY_MAX = 600
INTRO = ("The D1 work as a graph. Seeded from the breakdown on %s; edit here, "
         "and what's next reads it. Depends on lists task ids. Origin is "
         "breakdown or new scope.")

# Dependency texts that name a group of tasks instead of one title.
# Each entry maps the text, as written in the breakdown, to task ids.
DEP_ALIASES = {
    # "The four review skills above": the four skill tasks of the review area.
    "The four review skills above": ["18", "19", "20", "21"],
    # "The four Arsenal tasks above": the Arsenal tasks of the IAM area.
    "The four Arsenal tasks above": ["24", "25", "26", "27"],
}

# Titles that the team changed after the breakdown. Dependency texts still
# name the breakdown titles, so these apply after the dependencies resolve.
TITLE_OVERRIDES = {
    "3": "Set up the epic branches and Integration CI against the Federation epic",
    "28": "Release Arsenal from its development branch for CloudServer, backbeat and Vault",
    "30": "Pin the Vault epic image in Federation (automatic)",
}

# One sentence appended to a task's summary, after the length cut.
SUMMARY_NOTES = {
    "28": "No epic prerelease: D1 Arsenal changes go through the development "
          "branch and ship in a normal release.",
    "44": "Arsenal comes from its released npm version, not an epic tag.",
}


class BreakdownParser(HTMLParser):
    """Collect areas and task rows from the breakdown table."""

    def __init__(self):
        super().__init__()
        self.areas = []          # list of {id, title, count}
        self.tasks = []          # list of raw task dicts, in document order
        self.group = None        # current tbody id
        self.row = None          # current task row being filled
        self.cell = None         # current td class
        self.buf = []            # text pieces of the current cell
        self.in_area_name = False
        self.in_area_size = False
        self.in_repo = False

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        cls = a.get("class", "")
        if tag == "tbody" and a.get("id", "").startswith("g"):
            self.group = a["id"]
            self.areas.append({"id": self.group, "title": "", "count": None})
        elif tag == "tr" and re.fullmatch(r"t[0-9]+[a-z]?", a.get("id", "")):
            self.row = {"id": a["id"][1:], "area": self.group, "repos": [],
                        "num": "", "task": "", "est": "", "com": "", "dep": ""}
        elif tag == "span" and cls == "an":
            self.in_area_name = True
        elif tag == "span" and cls == "as":
            self.in_area_size = True
        elif self.row is not None and tag == "td":
            self.cell = cls
            self.buf = []
        elif self.row is not None and tag == "span" and cls == "repo":
            self.in_repo = True
            self.buf = []
        elif self.row is not None and self.cell == "dep" and tag == "br":
            self.buf.append("\n")
        elif self.row is not None and self.cell == "com" and tag == "li":
            self.buf.append("\x00")  # item separator

    def handle_endtag(self, tag):
        if tag == "span" and self.in_area_name:
            self.in_area_name = False
        elif tag == "span" and self.in_area_size:
            self.in_area_size = False
        elif tag == "span" and self.in_repo:
            self.row["repos"].append("".join(self.buf).strip())
            self.in_repo = False
            self.buf = []
        elif self.row is not None and tag == "td" and self.cell:
            text = "".join(self.buf)
            if self.cell == "com":
                self.row["com"] = text
            elif self.cell in ("num", "task", "est", "dep"):
                self.row[self.cell] = text
            self.cell = None
            self.buf = []
        elif tag == "tr" and self.row is not None:
            self.tasks.append(self.row)
            self.row = None
        elif tag == "tbody":
            self.group = None

    def handle_data(self, data):
        if self.in_area_name:
            self.areas[-1]["title"] += data
        elif self.in_area_size:
            m = re.search(r"(\d+) tasks?", data)
            if m:
                self.areas[-1]["count"] = int(m.group(1))
        elif self.row is not None and (self.cell or self.in_repo):
            self.buf.append(data)


def normalize_repo(raw):
    """Map a repo span to the short name used in the backlog."""
    text = raw.strip().lower()
    if text.startswith("for "):
        text = text[4:]
    base = re.sub(r"\s*\(.*\)$", "", text).strip()
    if base == "scality/workflows":
        base = "scality-workflows"
    return base if base in KNOWN_REPOS else text


def parse_estimate(text):
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)d\s*", text)
    if not m:
        return None
    value = float(m.group(1))
    return int(value) if value == int(value) else value


def flatten_summary(text):
    items = [re.sub(r"\s+", " ", t).strip() for t in text.split("\x00")]
    items = [t for t in items if t]
    out = " | ".join(items)
    return out[:SUMMARY_MAX]


def cell(text):
    return str(text).replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def to_markdown(areas, tasks):
    """The Backlog tab: the intro line and one table, a row per task."""
    title = {a["id"]: a["title"] for a in areas}
    lines = [INTRO % SOURCE_DATE, "",
             "| " + " | ".join(COLUMNS) + " |",
             "|" + "---|" * len(COLUMNS)]
    for t in tasks:
        values = (t["id"], t["title"], title[t["area"]], ", ".join(t["repos"]),
                  t["estimate_days"], ", ".join(t["depends_on"]),
                  t["origin"], t["notes"])
        lines.append("| " + " | ".join(cell(v) for v in values) + " |")
    return "\n".join(lines) + "\n"


def resolve_deps(text, titles):
    """The task ids and the unresolved texts of a Depends on cell, one text
    per line. A text can be a title, a title plus a parenthesized suffix
    like "(round 1)", or an alias for a group of tasks. Each id appears
    once, in the order first named, even when an alias and one of its
    tasks are both named."""
    deps, unresolved = [], []
    for piece in text.split("\n"):
        piece = piece.strip()
        if not piece or piece == "None":
            continue
        base = re.sub(r"\s*\([^()]*\)$", "", piece)
        if piece in titles:
            deps.append(titles[piece])
        elif base in titles:
            deps.append(titles[base])
        elif piece in DEP_ALIASES:
            deps.extend(DEP_ALIASES[piece])
        else:
            unresolved.append(piece)
    return list(dict.fromkeys(deps)), unresolved


def self_test():
    titles = {"Build the bnaas-spec-review skill": "18", "Write the spec": "5"}
    deps, unresolved = resolve_deps(
        "Write the spec\nThe four review skills above\nBuild the bnaas-spec-review skill (round 1)"
        "\nSomething else", titles)
    assert deps == ["5", "18", "19", "20", "21"], deps
    assert unresolved == ["Something else"], unresolved
    print("self-test ok")


def main():
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("html")
    parser.add_argument("--markdown")
    parser.add_argument("--json", dest="json_out")
    args = parser.parse_args()
    try:
        with open(args.html, encoding="utf-8") as f:
            p = BreakdownParser()
            p.feed(f.read())
    except OSError as e:
        print("cannot read %s: %s" % (args.html, e.strerror), file=sys.stderr)
        sys.exit(2)

    # Sanity checks on the parse before anything is written.
    excluded = [a for a in p.areas if a["title"].strip() == EXCLUDED_AREA]
    if len(excluded) != 1:
        sys.exit("expected exactly one excluded area, found %d" % len(excluded))
    for a in p.areas:
        got = sum(1 for t in p.tasks if t["area"] == a["id"])
        if a["count"] is not None and a["count"] != got:
            sys.exit("area %s says %s tasks, parsed %d" % (a["id"], a["count"], got))
    for t in p.tasks:
        if t["num"].strip() != t["id"]:
            sys.exit("row t%s has num %r" % (t["id"], t["num"]))

    skip = excluded[0]["id"]
    areas = [{"id": a["id"], "title": a["title"].strip()} for a in p.areas if a["id"] != skip]
    rows = [t for t in p.tasks if t["area"] != skip]
    titles = {}
    for t in rows:
        title = t["task"].strip()
        if title in titles:
            sys.exit("duplicate title %r (%s and %s)" % (title, titles[title], t["id"]))
        titles[title] = t["id"]
    ids = {t["id"] for t in rows}
    for table in (TITLE_OVERRIDES, SUMMARY_NOTES,
                  {i: None for v in DEP_ALIASES.values() for i in v}):
        for tid in table:
            if tid not in ids:
                sys.exit("override for task %s, which the breakdown does not have" % tid)

    tasks = []
    odd_estimates = []
    for t in rows:
        est = parse_estimate(t["est"])
        if est is None:
            odd_estimates.append((t["id"], t["est"].strip()))
            est = 0
        deps, unresolved = resolve_deps(t["dep"], titles)
        repos = []
        for r in t["repos"]:
            n = normalize_repo(r)
            if n not in repos:
                repos.append(n)
        summary = flatten_summary(t["com"])
        if t["id"] in SUMMARY_NOTES:
            summary += " | " + SUMMARY_NOTES[t["id"]]
        task = {
            "id": t["id"], "title": TITLE_OVERRIDES.get(t["id"], t["task"].strip()),
            "area": t["area"], "repos": repos, "estimate_days": est,
            "depends_on": deps, "depends_on_unresolved": unresolved,
            "origin": "breakdown",
            "notes": (UNKNOWN_PREFIX + " " + "; ".join(unresolved)) if unresolved else "",
            "summary": summary,
        }
        tasks.append(task)

    md = to_markdown(areas, tasks)
    if args.markdown:
        with open(args.markdown, "w", encoding="utf-8") as f:
            f.write(md)
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump({"source": SOURCE_URL, "date": SOURCE_DATE,
                       "areas": areas, "tasks": tasks}, f, indent=1, ensure_ascii=False)
            f.write("\n")
    if not args.markdown and not args.json_out:
        sys.stdout.write(md)
    report = sys.stderr if not args.markdown and not args.json_out else sys.stdout
    print("%d areas, %d tasks" % (len(areas), len(tasks)), file=report)
    for tid, raw in odd_estimates:
        print("estimate not a number for task %s: %r, wrote 0" % (tid, raw), file=report)
    for t in tasks:
        for u in t["depends_on_unresolved"]:
            print("unresolved dependency on task %s: %r" % (t["id"], u), file=report)


if __name__ == "__main__":
    main()
