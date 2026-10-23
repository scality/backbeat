#!/usr/bin/env python3
"""Print the state of one D1 PR, or of a stack, as the shepherd needs it.

Usage:
  pr_state.py --repo <owner/repo> --pr <n> [--stack]
  pr_state.py --self-test

One GraphQL read per PR, and one `gh pr list --base <head>` for the PRs
stacked on it. It never writes. The JSON object of a PR holds:

  state          OPEN, MERGED or CLOSED; draft; head (branch), head_sha, base,
                 updated_at
  mergeable      MERGEABLE, CONFLICTING or UNKNOWN; merge_state (CLEAN, DIRTY...)
  review_check   the "Claude Code Review" check and the job that posts it:
                 success, action_required, failure, cancelled, pending or
                 missing, with the check url and the job's run_url
  failed_checks  every other check that failed, with its url; pending_checks
  threads        the open review threads, split into claude (first comment by
                 Claude Code), human (by someone else than the PR author and
                 bots) and other, each with id, path, url, author, created_at and body
  approvals      the distinct humans whose last verdict is an approval (not
                 the author, not bots, not asked again since then),
                 changes_requested the humans whose last verdict asks for
                 changes, even when asked again (GitHub still blocks the
                 merge until they review again), other_dev
                 the mandatory reviewer and other_approved; requested the
                 pending review requests (logins and scality/<team>), reviewed
                 the humans who submitted any review, and scality/hopocalypse
                 when one of them is neither the other developer nor Nicolas
  stacked        the open PRs whose base is this head branch, with their author
  draft_goal     Claude Code's LGTM, no failed or pending check, MERGEABLE and
                 no open Claude Code thread: the draft is ready for its author
  merge_goal     based on the epic or a development/* branch (not stacked),
                 two approvals, Claude Code's LGTM, every check green,
                 MERGEABLE and no open human thread: ready for the human to merge

With --stack, it prints a JSON list instead: this PR, then every PR stacked
above it, depth first. The approval rules are those of prs.py in the
what's next skill, imported from there, so the two never disagree.
"""
import contextlib
import io
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "bnaas-whats-next", "scripts"))
import prs  # noqa: E402  (is_bot, verdicts and PAIR: one definition of a reviewer)

REVIEW_CHECK = "Claude Code Review"
REVIEW_JOB = "claude-review"
CLAUDE = {"claude", "claude[bot]"}
TEAM = "scality/hopocalypse"
FAILED = {"FAILURE", "TIMED_OUT", "CANCELLED", "STARTUP_FAILURE", "ACTION_REQUIRED", "ERROR"}
QUERY = """
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      number url title isDraft state mergeable mergeStateStatus updatedAt
      headRefName headRefOid baseRefName author { login }
      reviews(last: 100) { nodes { author { login } state submittedAt } }
      reviewRequests(first: 20) { nodes { requestedReviewer {
        ... on User { login } ... on Team { slug } } } }
      reviewThreads(first: 100) { nodes { id isResolved
        comments(first: 1) { nodes { author { login } body path url createdAt } } } }
      commits(last: 1) { nodes { commit { statusCheckRollup { contexts(first: 100) {
        nodes { __typename
          ... on CheckRun { name status conclusion detailsUrl }
          ... on StatusContext { context state targetUrl } } } } } } }
    }
  }
}
"""


def gh(*args):
    r = subprocess.run(["gh", *args], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or "gh %s failed" % args[0])
    return r.stdout


def checks(contexts):
    """The review state, the failed checks and the pending ones.

    The review is the "Claude Code Review" check together with the
    workflow job that posts it (its name holds claude-review). A running
    job means pending, whatever an older check says, and a failed job
    with no check means failure. The job's run link is run_url.
    """
    review, failed, pending = {"state": "missing", "url": None, "run_url": None}, [], []
    job = None
    for c in contexts:
        if c.get("__typename") == "StatusContext":
            name, url = c.get("context"), c.get("targetUrl")
            # EXPECTED: a required status that has not reported yet.
            done, result = c.get("state") not in ("PENDING", "EXPECTED"), c.get("state")
        else:
            name, url = c.get("name"), c.get("detailsUrl")
            done, result = c.get("status") == "COMPLETED", c.get("conclusion")
        if name == REVIEW_CHECK:
            review.update(state=(result or "").lower() if done else "pending", url=url)
        elif REVIEW_JOB in (name or "").lower():
            job = {"done": done, "result": result}
            review["run_url"] = url
        elif not done:
            pending.append(name)
        elif result in FAILED:
            failed.append({"name": name, "conclusion": result.lower(), "url": url})
    if job and not job["done"]:
        review["state"] = "pending"
    elif job and review["state"] in ("missing", "pending"):
        # The job ended (failed, cancelled, timed out, or even succeeded)
        # without finishing its check: nothing will move it, so it failed.
        review["state"] = "failure"
    return review, failed, pending


def threads(nodes, author):
    out = {"claude": [], "human": [], "other": []}
    for t in nodes:
        first = ((t.get("comments") or {}).get("nodes") or [{}])[0]
        who = ((first.get("author") or {}).get("login") or "").lower()
        if t.get("isResolved"):
            continue
        group = ("claude" if who in CLAUDE else
                 "other" if not who or who == author or prs.is_bot(who) else "human")
        out[group].append({"id": t["id"], "author": who, "path": first.get("path"),
                           "url": first.get("url"), "created_at": first.get("createdAt"),
                           "body": first.get("body") or ""})
    return out


def state(repo, number, run=gh, stacked=True):
    """The state of one PR; stacked=False skips the read of the PRs on it."""
    owner, name = repo.split("/", 1)
    data = json.loads(run("api", "graphql", "-f", "query=" + QUERY, "-F", "owner=" + owner,
                          "-F", "name=" + name, "-F", "number=%d" % number))
    pr = data["data"]["repository"]["pullRequest"]
    author = pr["author"]["login"].lower()
    reviews = [r for r in pr["reviews"]["nodes"] if r.get("author")]
    verdict = prs.verdicts({"author": {"login": author}, "reviews": reviews})
    # A reviewer asked again owes a new look: their approval waits for it.
    nodes = (pr.get("reviewRequests") or {}).get("nodes") or []
    asked = {((n.get("requestedReviewer") or {}).get("login") or "").lower() for n in nodes}
    teams = {"scality/" + n["requestedReviewer"]["slug"] for n in nodes
             if (n.get("requestedReviewer") or {}).get("slug")}
    # The last review of each human; a dismissed one means they owe a new look.
    last = {}
    for r in sorted(reviews, key=lambda r: r.get("submittedAt") or ""):
        who = (r["author"].get("login") or "").lower()
        if who and who != author and not prs.is_bot(who):
            last[who] = r.get("state")
    reviewed = {who for who, st in last.items() if st != "DISMISSED"}
    # GitHub drops the team request once a member reviews, and the review
    # names the member, not the team: anyone but the two named reviewers
    # reviewed for the team.
    if reviewed - {prs.PAIR.get(author), "nicolas2bert"}:
        reviewed.add(TEAM)
    reviewed = sorted(reviewed)
    approvals = sorted(u for u, s in verdict.items() if s == "APPROVED" and u not in asked)
    commit = (pr["commits"]["nodes"] or [{}])[0].get("commit") or {}
    rollup = (commit.get("statusCheckRollup") or {}).get("contexts") or {}
    review, failed, pending = checks(rollup.get("nodes") or [])
    open_threads = threads(pr["reviewThreads"]["nodes"], author)
    above = json.loads(run("pr", "list", "-R", repo, "--base", pr["headRefName"], "--state",
                           "open", "--json", "number,url,headRefName,isDraft,author")) \
        if stacked else []
    other = prs.PAIR.get(author)
    green = not failed and not pending
    out = {"repo": repo, "number": pr["number"], "url": pr["url"], "title": pr["title"],
           "author": pr["author"]["login"], "state": pr["state"], "draft": pr["isDraft"],
           "head": pr["headRefName"], "head_sha": pr["headRefOid"], "base": pr["baseRefName"],
           "updated_at": pr.get("updatedAt"),
           "mergeable": pr["mergeable"], "merge_state": pr["mergeStateStatus"],
           "review_check": review, "failed_checks": failed, "pending_checks": pending,
           "threads": open_threads, "approvals": approvals,
           "requested": sorted((asked - {""}) | teams), "reviewed": reviewed,
           "changes_requested": sorted(u for u, s in verdict.items() if s == "CHANGES_REQUESTED"),
           "other_dev": other, "other_approved": bool(other) and other in approvals,
           "stacked": [{"number": s["number"], "url": s["url"], "head": s["headRefName"],
                        "draft": s["isDraft"], "author": (s.get("author") or {}).get("login")}
                       for s in above]}
    out["draft_goal"] = (pr["state"] == "OPEN" and review["state"] == "success" and green
                         and pr["mergeable"] == "MERGEABLE" and not open_threads["claude"])
    # A PR stacked on another PR's branch merges only after it is retargeted.
    landing = pr["baseRefName"].startswith(("epic/", "development/"))
    out["merge_goal"] = (pr["state"] == "OPEN" and not pr["isDraft"] and landing
                         and len(approvals) >= 2
                         and not out["changes_requested"] and green
                         and review["state"] == "success"
                         and pr["mergeable"] == "MERGEABLE" and not open_threads["human"])
    return out


def stack(repo, number, run=gh):
    """This PR, then each PR stacked above it, depth first."""
    seen, out, todo = set(), [], [number]
    while todo:
        n = todo.pop(0)
        if n in seen:
            continue
        seen.add(n)
        s = state(repo, n, run)
        out.append(s)
        todo = [x["number"] for x in s["stacked"]] + todo
    return out


def self_test():
    with open(os.path.join(HERE, "..", "evals", "fixtures", "graphql-pr.json")) as f:
        base = json.load(f)

    def pr_data(n, **kw):
        d = json.loads(json.dumps(base))
        p = d["data"]["repository"]["pullRequest"]
        p.update(number=n, url="https://github.com/scality/backbeat/pull/%d" % n,
                 headRefName="improvement/ZZ-1-x%d" % n)
        p.update(kw)
        return d

    datas = {1: pr_data(1), 2: pr_data(2, baseRefName="improvement/ZZ-1-x1"),
             3: pr_data(3, baseRefName="improvement/ZZ-1-x2", state="MERGED")}
    above = {"improvement/ZZ-1-x1": [2], "improvement/ZZ-1-x2": [3]}
    calls = []

    def fake(*args):
        calls.append(args)
        if args[:2] == ("api", "graphql"):
            assert args[3].startswith("query=") and "-F" in args, args
            return json.dumps(datas[int(args[-1].split("=")[1])])
        assert args[:2] == ("pr", "list") and "--state" in args, args
        head = args[args.index("--base") + 1]
        return json.dumps([{"number": n, "url": "u%d" % n, "headRefName": "improvement/ZZ-1-x%d" % n,
                            "isDraft": True, "author": {"login": "anurag4DSB"}}
                           for n in above.get(head, [])])

    s = state("scality/backbeat", 1, fake)
    # The fixture: a draft by anurag4DSB with a Claude Code thread, a human
    # thread, the author's own thread, a resolved one, a green build and a
    # failed lint, the review asking for action, and approvals from Nicolas,
    # a Hopocalypse member and a bot, Taylor's approval dismissed.
    assert s["review_check"]["state"] == "action_required", s["review_check"]
    assert [c["name"] for c in s["failed_checks"]] == ["lint"], s["failed_checks"]
    assert s["pending_checks"] == ["tests"], s["pending_checks"]
    assert [t["id"] for t in s["threads"]["claude"]] == ["T1"], s["threads"]
    assert s["threads"]["claude"][0]["created_at"] == "2026-10-22T08:15:00Z", s["threads"]
    assert [t["id"] for t in s["threads"]["human"]] == ["T2"], s["threads"]
    assert [t["id"] for t in s["threads"]["other"]] == ["T3"], s["threads"]
    assert s["approvals"] == ["hopocalypse-member", "nicolas2bert"], s["approvals"]
    # Taylor's approval was dismissed: Taylor counts as not reviewed, to ask again.
    assert "tmacro" not in s["reviewed"], s["reviewed"]
    assert s["other_dev"] == "tmacro" and not s["other_approved"], s
    assert s["changes_requested"] == [] and s["mergeable"] == "MERGEABLE", s
    assert s["stacked"] == [{"number": 2, "url": "u2", "head": "improvement/ZZ-1-x2",
                             "draft": True, "author": "anurag4DSB"}], s["stacked"]
    assert s["updated_at"] == "2026-10-22T13:30:00Z" and s["review_check"]["run_url"] is None, s
    # Without the stacked read, no pr list call.
    calls.clear()
    assert state("scality/backbeat", 1, fake, stacked=False)["stacked"] == []
    assert [c[:2] for c in calls] == [("api", "graphql")], calls
    assert not s["draft_goal"] and not s["merge_goal"], s

    # Fixed: the review gave its LGTM, the threads are resolved, checks green.
    p = datas[1]["data"]["repository"]["pullRequest"]
    roll = p["commits"]["nodes"][0]["commit"]["statusCheckRollup"]["contexts"]["nodes"]
    for c in roll:
        if c.get("__typename") == "CheckRun":
            c.update(status="COMPLETED", conclusion="SUCCESS")
    for t in p["reviewThreads"]["nodes"]:
        if t["id"] == "T1":
            t["isResolved"] = True
    s = state("scality/backbeat", 1, fake)
    assert s["review_check"]["state"] == "success" and s["draft_goal"], s
    # Not merge ready: a draft, and Taylor's human thread is still open.
    assert not s["merge_goal"], s
    p["isDraft"] = False
    p["reviewThreads"]["nodes"] = [t for t in p["reviewThreads"]["nodes"] if t["id"] != "T2"]
    p["reviews"]["nodes"].append({"author": {"login": "TMacro"}, "state": "APPROVED",
                                  "submittedAt": "2026-10-23T09:00:00Z"})
    s = state("scality/backbeat", 1, fake)
    assert s["merge_goal"] and s["other_approved"], s
    # Stacked on another PR's branch: never a merge call, whatever else holds.
    p["baseRefName"] = "improvement/ZZ-1-lower"
    assert not state("scality/backbeat", 1, fake)["merge_goal"]
    p["baseRefName"] = "development/8.4"
    assert state("scality/backbeat", 1, fake)["merge_goal"]
    p["baseRefName"] = "epic/ZZ-0-bnaas-d1"
    # No merge call while the review asks for action.
    roll[0].update(conclusion="ACTION_REQUIRED")
    assert not state("scality/backbeat", 1, fake)["merge_goal"]
    roll[0].update(conclusion="SUCCESS")
    # A conflict breaks both goals, like a red check.
    p["mergeable"] = "CONFLICTING"
    s = state("scality/backbeat", 1, fake)
    assert not s["draft_goal"] and not s["merge_goal"], s

    # The stack: 1, then 2 on it, then 3 on 2, each read once.
    calls.clear()
    got = stack("scality/backbeat", 1, fake)
    assert [x["number"] for x in got] == [1, 2, 3] and got[2]["state"] == "MERGED", got
    assert not got[2]["draft_goal"], got[2]
    assert sum(1 for c in calls if c[:2] == ("api", "graphql")) == 3, calls
    # The review job running: pending, even over the success of an older check.
    job = {"name": "review / claude-review", "status": "IN_PROGRESS", "detailsUrl": "run/9"}
    for ctx in ([job], [{"name": REVIEW_CHECK, "status": "COMPLETED", "conclusion": "SUCCESS",
                         "detailsUrl": "c"}, job]):
        review, failed, pending = checks(ctx)
        assert review["state"] == "pending" and review["run_url"] == "run/9", review
        assert not pending and not failed, (pending, failed)
    # A required status that has not reported yet is pending, not green.
    review, failed, pending = checks([{"__typename": "StatusContext", "context": "bert-e",
                                       "state": "EXPECTED", "targetUrl": None}])
    assert pending == ["bert-e"] and not failed, (pending, failed)
    # The job failed and posted no check: the review failed, with its run link.
    review, failed, pending = checks([dict(job, status="COMPLETED", conclusion="FAILURE")])
    assert review == {"state": "failure", "url": None, "run_url": "run/9"} and not failed, review
    # The job ended but left its check in progress: failure, with the run link.
    review, failed, pending = checks([
        {"name": REVIEW_CHECK, "status": "IN_PROGRESS", "detailsUrl": "c"},
        dict(job, status="COMPLETED", conclusion="CANCELLED")])
    assert review["state"] == "failure" and review["run_url"] == "run/9", review
    # An approver asked again: the approval waits for the new look.
    p["reviewRequests"] = {"nodes": [{"requestedReviewer": {"login": "Nicolas2bert"}},
                                     {"requestedReviewer": {"slug": "hopocalypse"}}]}
    p["mergeable"] = "MERGEABLE"
    s = state("scality/backbeat", 1, fake)
    assert s["approvals"] == ["hopocalypse-member", "tmacro"], s["approvals"]
    assert s["requested"] == ["nicolas2bert", "scality/hopocalypse"], s["requested"]
    assert s["reviewed"] == ["hopocalypse-member", "nicolas2bert", "scality/hopocalypse",
                             "tmacro"], s["reviewed"]
    # Only the two named reviewers reviewed: the team still owes its look.
    p["reviews"]["nodes"] = [r for r in p["reviews"]["nodes"]
                             if (r.get("author") or {}).get("login") in ("nicolas2bert", "tmacro")]
    assert "scality/hopocalypse" not in state("scality/backbeat", 1, fake)["reviewed"]
    with contextlib.redirect_stderr(io.StringIO()):
        for argv in (["--repo", "scality/backbeat"], ["--pr", "x"], ["--repo", "--pr", "1"],
                     ["--repo", "backbeat", "--pr", "1"], ["--bogus"]):
            assert main(argv) == 2, argv
    print("self-test ok")


def main(argv):
    if argv[:1] == ["--self-test"]:
        self_test()
        return 0
    repo = number = None
    many = False
    args = iter(argv)
    for a in args:
        if a in ("--repo", "--pr"):
            value = next(args, None)
            if value is None or value.startswith("-"):
                return usage()
            if a == "--repo":
                repo = value
            elif not value.isdigit():
                return usage()
            else:
                number = int(value)
        elif a == "--stack":
            many = True
        else:
            return usage()
    if not repo or "/" not in repo or number is None:
        return usage()
    try:
        out = stack(repo, number) if many else state(repo, number)
    except (RuntimeError, ValueError, KeyError, TypeError) as e:
        print("Error: cannot read %s#%d: %s" % (repo, number, e), file=sys.stderr)
        return 1
    print(json.dumps(out, indent=1))
    return 0


def usage():
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
