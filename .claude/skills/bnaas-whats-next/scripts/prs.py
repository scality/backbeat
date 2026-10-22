#!/usr/bin/env python3
"""List the open D1 PRs and say who must act on each.

Usage:
  prs.py [--epic <branch>] [--login <login>] [--json] [--pr <url>]... <repository>...
  prs.py --self-test

Each <repository> is a name from the Repository column of the Tickets tab,
for example arsenal or backbeat. The script runs `gh pr list --base <epic>`
in each one, then follows stacked PRs: a PR whose base is the head branch
of a D1 PR. It does not use `gh search prs`, because the search index
misses PRs. Each --pr adds one PR of the PR column, open or not, so that
the caller can check every Status cell of the tab.

For each PR it gives the Status words of the Tickets tab ("Waiting for
review from Taylor"), who must act next, and its kind for <login>: review
(it waits for that developer's review), mine (that developer wrote it),
other, or closed (merged or closed, from --pr). An open PR of someone
else also says whether that developer is requested (asked). A PR can be
of kind other and still ask them, when its author has to act first.
Logins compare without case. The epic defaults to the remote branch
epic/*-bnaas-d1 of the git checkout that holds this script. The login
defaults to the gh user.
"""
import contextlib
import fnmatch
import io
import json
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

FIELDS = ("number,title,url,author,isDraft,headRefName,baseRefName,state,"
          "mergedAt,reviews,reviewRequests")
# Keys are lower case: GitHub logins do not depend on case.
NAMES = {"anurag4dsb": "Anurag", "tmacro": "Taylor", "nicolas2bert": "Nicolas",
         "scality/hopocalypse": "the Hopocalypse team"}
# Rule 3: each developer must approve the other's PRs.
PAIR = {"anurag4dsb": "tmacro", "tmacro": "anurag4dsb"}
BOTS = {"claude", "github-actions", "bert-e", "copilot", "copilot-pull-request-reviewer"}
TICKET = re.compile(r"\b([A-Z][A-Z0-9]+-\d+)\b")


def gh(*args):
    r = subprocess.run(["gh", *args], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or "gh %s failed" % args[0])
    return r.stdout


def name(who):
    return NAMES.get(who.lower(), who)


def names(people):
    people = [name(p) for p in people]
    if len(people) < 3:
        return " and ".join(people)
    return ", ".join(people[:-1]) + " and " + people[-1]


def is_bot(login):
    return login.lower() in BOTS or login.lower().endswith("[bot]")


def requested(pr):
    """The people and teams asked to review, without bots."""
    asked = []
    for r in pr.get("reviewRequests") or []:
        who = r.get("login") or r.get("slug") or r.get("name")
        if who and r.get("__typename") != "Bot" and not is_bot(who):
            asked.append(who.lower())
    return asked


def verdicts(pr):
    """The last APPROVED or CHANGES_REQUESTED review of each human reviewer."""
    author, state = pr["author"]["login"].lower(), {}
    for r in sorted(pr.get("reviews") or [], key=lambda r: r.get("submittedAt") or ""):
        who = ((r.get("author") or {}).get("login") or "").lower()
        if not who or who == author or is_bot(who):
            continue
        if r.get("state") in ("APPROVED", "CHANGES_REQUESTED"):
            state[who] = r["state"]
        elif r.get("state") == "DISMISSED":
            state.pop(who, None)
    return state


def classify(pr, me):
    author, me = pr["author"]["login"].lower(), me.lower()
    m = re.match(r"https://github\.com/[^/]+/([^/]+)/pull/(\d+)", pr["url"])
    ticket = TICKET.search(pr.get("headRefName") or "") or TICKET.search(pr.get("title") or "")
    out = {"pr": "%s#%s" % (m.group(1), m.group(2)), "url": pr["url"], "title": pr["title"],
           "author": name(pr["author"]["login"]), "draft": bool(pr.get("isDraft")),
           "ticket": ticket.group(1) if ticket else None,
           "base": pr.get("baseRefName"), "next": [], "kind": "other"}
    if pr.get("state") in ("MERGED", "CLOSED"):
        out["kind"] = "closed"
        out["status"] = ("Merged on " + (pr.get("mergedAt") or "")[:10]
                         if pr["state"] == "MERGED" else "Closed, not merged")
        return out

    asked = requested(pr)
    state = verdicts(pr)
    # A reviewer asked again has the next move, whatever their last review said.
    approved = [u for u, s in state.items() if s == "APPROVED" and u not in asked]
    changes = [u for u, s in state.items() if s == "CHANGES_REQUESTED" and u not in asked]
    mandatory = PAIR.get(author)
    waiting = list(asked)
    if mandatory and mandatory not in waiting and mandatory not in state:
        waiting.insert(0, mandatory)
    if changes:
        status, nxt = "Changes requested by " + names(changes), [author]
    elif len(approved) >= 2 and (not mandatory or mandatory in approved):
        status, nxt = "Approved, not merged yet", [author]
    elif pr.get("isDraft") and not asked:
        status, nxt = "Draft, no review requested yet", [author]
    elif not waiting:
        status, nxt = "Waiting for a reviewer: nobody is requested", [author]
    else:
        more = "one more review" if approved else "review"
        status, nxt = "Waiting for %s from %s" % (more, names(waiting)), waiting
    out["status"], out["next"] = status, [name(p) for p in nxt]

    # The kind follows the status, so that the two never disagree. A
    # request still shows when the author has to act first.
    if author == me:
        out["kind"] = "mine"
    else:
        out["asked"] = me in asked
        if me in nxt:
            out["kind"] = "review"
    return out


def collect(epic, repos, urls, run=gh):
    """Open PRs into the epic, the PRs stacked on them, and the given URLs."""
    found, errors = {}, []

    def listing(repo, base):
        try:
            return repo, json.loads(run("pr", "list", "-R", "scality/" + repo, "--base", base,
                                        "--state", "open", "--json", FIELDS, "--limit", "100"))
        except (RuntimeError, ValueError) as e:
            errors.append("%s, base %s: %s" % (repo, base, e))
            return repo, []

    todo = [(repo, epic) for repo in repos]
    with ThreadPoolExecutor(max_workers=8) as pool:
        while todo:
            heads = []
            for repo, prs in pool.map(lambda t: listing(*t), todo):
                for pr in prs:
                    if pr["url"].lower() not in found:
                        found[pr["url"].lower()] = pr
                        heads.append((repo, pr["headRefName"]))
            todo = heads
        for url in urls:
            if url.lower() in found:
                continue
            try:
                pr = json.loads(run("pr", "view", url, "--json", FIELDS))
            except (RuntimeError, ValueError) as e:
                errors.append("%s: %s" % (url, e))
                continue
            # Key on the URL that gh gives, so that another spelling of a
            # listed PR does not count twice.
            found.setdefault(pr["url"].lower(), pr)
    return list(found.values()), errors


def find_epic():
    here = os.path.dirname(os.path.realpath(__file__))
    r = subprocess.run(["git", "-C", here, "for-each-ref", "--format=%(refname:lstrip=3)",
                        "refs/remotes/origin/epic/"], capture_output=True, text=True)
    epics = [b for b in r.stdout.split() if fnmatch.fnmatch(b, "epic/*-bnaas-d1")]
    if len(epics) != 1:
        raise SystemExit("Cannot find the D1 epic branch here; pass --epic <branch>.")
    return epics[0]


def report(rows, errors):
    groups = [("review", "Waiting for your review"), ("mine", "Your open PRs"),
              ("other", "Other open D1 PRs"), ("closed", "Merged or closed")]
    lines = []
    for kind, title in groups:
        lines.append(title + ":")
        picked = [r for r in rows if r["kind"] == kind]
        for r in picked:
            who = ", by " + r["author"] if kind in ("review", "other") else ""
            draft = ", draft" if r["draft"] else ""
            # Say who acts next only when the status does not already name them.
            nxt = names(r["next"])
            nxt = " Next: %s." % nxt if r["next"] and nxt not in r["status"] else ""
            if kind == "review":
                nxt += " You are requested." if r["asked"] else " You have not approved it yet."
            elif kind == "other" and r.get("asked"):
                nxt += " You are requested, after them."
            lines.append("- %s%s%s: %s. %s.%s %s" % (r["pr"], who, draft, r["title"], r["status"],
                                                   nxt, r["url"]))
        if not picked:
            lines.append("- none")
    for e in errors:
        lines.append("Error: " + e)
    return "\n".join(lines)


def self_test():
    def pr(n, author, reviews=(), asked=(), state="OPEN", **kw):
        p = {"number": n, "title": "ZZ-1: change %d" % n,
             "url": "https://github.com/scality/backbeat/pull/%d" % n,
             "author": {"login": author}, "isDraft": False,
             "headRefName": "improvement/ZZ-1-x%d" % n, "baseRefName": "epic/ZZ-0-bnaas-d1",
             "state": state, "mergedAt": None,
             "reviews": [{"author": {"login": who}, "state": s,
                          "submittedAt": "2026-10-2%dT00:00:00Z" % i}
                         for i, (who, s) in enumerate(reviews)],
             "reviewRequests": [a if isinstance(a, dict) else {"slug": a} if "/" in a
                                else {"login": a} for a in asked]}
        p.update(kw)
        return p

    A, T, N = "anurag4DSB", "tmacro", "nicolas2bert"
    AN, ANH = ["Anurag", "Nicolas"], ["Taylor", "the Hopocalypse team"]
    cases = [
        # Taylor's new PR: Anurag is requested, so it waits for Anurag.
        (pr(1, T, asked=[A, N]), A, "review", "Waiting for review from Anurag and Nicolas", AN),
        # The request is gone once Anurag comments, but Anurag has not approved.
        (pr(2, T, [("claude", "COMMENTED"), (A, "COMMENTED")], [N]), A, "review",
         "Waiting for review from Anurag and Nicolas", AN),
        # Anurag approved: one more review, from Nicolas.
        (pr(3, T, [(A, "APPROVED")], [N]), A, "other",
         "Waiting for one more review from Nicolas", ["Nicolas"]),
        # Anurag asked for changes: the author has the next move.
        (pr(4, T, [(A, "CHANGES_REQUESTED")], [N]), A, "other",
         "Changes requested by Anurag", ["Taylor"]),
        # Asked again after the change request: back to Anurag.
        (pr(5, T, [(A, "CHANGES_REQUESTED")], [A]), A, "review",
         "Waiting for review from Anurag", ["Anurag"]),
        # Anurag's PR, Nicolas approved, Taylor still mandatory.
        (pr(6, A, [(N, "APPROVED"), ("claude", "COMMENTED")], [T]), A, "mine",
         "Waiting for one more review from Taylor", ["Taylor"]),
        # Two approvals with Taylor's: on the author to merge.
        (pr(7, A, [(T, "APPROVED"), (N, "APPROVED")]), A, "mine",
         "Approved, not merged yet", ["Anurag"]),
        # A dismissed approval no longer counts.
        (pr(8, A, [(T, "DISMISSED"), (N, "APPROVED")], ["scality/hopocalypse"]), A, "mine",
         "Waiting for one more review from Taylor and the Hopocalypse team", ANH),
        # Taylor approved, nobody else is requested.
        (pr(9, A, [(T, "APPROVED")]), A, "mine",
         "Waiting for a reviewer: nobody is requested", ["Anurag"]),
        # Anurag approved, then Taylor asked again: that approval waits for a new look.
        (pr(12, T, [(A, "APPROVED"), (N, "APPROVED")], [A]), A, "review",
         "Waiting for one more review from Anurag", ["Anurag"]),
        # Anurag is asked, but Nicolas asked for changes first: the author acts.
        (pr(13, T, [(N, "CHANGES_REQUESTED")], [A]), A, "other",
         "Changes requested by Nicolas", ["Taylor"]),
        # A draft with nobody asked is not waiting for review yet.
        (pr(14, T, isDraft=True), A, "other", "Draft, no review requested yet", ["Taylor"]),
        # A draft plan PR with its reviewers asked is.
        (pr(15, T, asked=[A, N], isDraft=True), A, "review",
         "Waiting for review from Anurag and Nicolas", AN),
        # Bots, with or without a type, and empty requests are not reviewers.
        (pr(16, A, asked=[{"__typename": "Bot", "login": "copilot-pull-request-reviewer"},
                          {"login": "copilot-pull-request-reviewer"}, {}, "claude"]), A,
         "mine", "Waiting for review from Taylor", ["Taylor"]),
        # Logins compare without case, in requests, reviews and the caller.
        (pr(17, "TMacro", [("Nicolas2Bert", "APPROVED")], ["ANURAG4dsb"]), "Anurag4DSB",
         "review", "Waiting for one more review from Anurag", ["Anurag"]),
        # Seen from Taylor's session, PR 1 is Taylor's own.
        (pr(1, T, asked=[A, N]), T, "mine", "Waiting for review from Anurag and Nicolas", AN),
        (pr(10, A, state="MERGED", mergedAt="2026-10-16T02:34:20Z"), A, "closed",
         "Merged on 2026-10-16", []),
        (pr(11, A, state="CLOSED"), T, "closed", "Closed, not merged", []),
    ]
    for p, me, kind, status, nxt in cases:
        got = classify(p, me)
        assert (got["kind"], got["status"], got["next"]) == (kind, status, nxt), (p["number"], got)
    assert classify(pr(1, T, asked=[A]), A)["ticket"] == "ZZ-1"
    # Asked, but the author acts first: kind other, and the request shows.
    got = classify(pr(13, T, [(N, "CHANGES_REQUESTED")], [A]), A)
    assert got["asked"] and "You are requested, after them." in report([got], []), got

    # Listing: two repositories, a PR stacked on another, one failing repository,
    # and merged PRs added from the PR column.
    open_prs = {("backbeat", "epic/E"): [pr(20, A)],
                ("backbeat", "improvement/ZZ-1-x20"):
                    [pr(21, A, baseRefName="improvement/ZZ-1-x20")],
                ("backbeat", "improvement/ZZ-1-x21"): [],
                ("vault", "epic/E"): []}

    def fake(*args):
        if args[:2] == ("pr", "view"):
            n = int(args[2].rstrip("/").split("/")[-1])
            return json.dumps(pr(n, A, state="MERGED", mergedAt="2026-10-15T16:19:09Z"))
        key = (args[3].split("/")[1], args[5])
        if key not in open_prs:
            raise RuntimeError("no such repository")
        return json.dumps(open_prs[key])

    # PR 30 comes twice, spelled two ways: it counts once.
    got, errors = collect("epic/E", ["backbeat", "vault", "nope"],
                          ["https://github.com/scality/backbeat/pull/30",
                           "https://github.com/scality/Backbeat/pull/30/",
                           "https://github.com/scality/backbeat/pull/20"], run=fake)
    assert sorted(p["number"] for p in got) == [20, 21, 30], got
    assert len(errors) == 1 and errors[0].startswith("nope"), errors
    text = report([classify(p, A) for p in got], errors)
    assert "Waiting for your review:\n- none" in text and "Error: nope" in text, text
    text = report([classify(pr(1, T, asked=[A, N]), A), classify(pr(2, T, asked=[N]), A)], [])
    assert "pull/1" in text and "You are requested." in text, text
    assert "You have not approved it yet." in text, text

    # A flag value never starts with a dash, so --json is never taken as one.
    with contextlib.redirect_stderr(io.StringIO()):
        for argv in (["--pr", "--json", "backbeat"], ["--epic"], ["--bogus", "backbeat"], []):
            assert main(argv) == 2, argv
    print("ok: %d PRs classified, listing, report and arguments" % len(cases))


def main(argv):
    if argv[:1] == ["--self-test"]:
        self_test()
        return 0
    epic = login = None
    as_json, urls, repos = False, [], []
    args = iter(argv)
    for a in args:
        if a in ("--epic", "--login", "--pr"):
            value = next(args, None)
            if value is None or value.startswith("-"):
                print(__doc__, file=sys.stderr)
                return 2
            if a == "--epic":
                epic = value
            elif a == "--login":
                login = value
            else:
                urls.append(value)
        elif a == "--json":
            as_json = True
        elif a.startswith("-"):
            print(__doc__, file=sys.stderr)
            return 2
        else:
            repos.append(a)
    if not repos and not urls:
        print(__doc__, file=sys.stderr)
        return 2
    epic = epic or find_epic()
    login = login or gh("api", "user", "--jq", ".login").strip()
    prs, errors = collect(epic, repos, urls)
    rows = sorted((classify(p, login) for p in prs), key=lambda r: r["url"].lower())
    if as_json:
        print(json.dumps({"epic": epic, "login": login, "prs": rows, "errors": errors}, indent=1))
    else:
        print("Epic %s, seen as %s.\n%s" % (epic, name(login), report(rows, errors)))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
