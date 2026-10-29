#!/usr/bin/env python3
"""One pass of the dog: print one line per change on the developer's D1 PRs.

Usage:
  watch_flock.py [--login <login>] [--epic <branch>] [--state <file>] [--fresh]
  watch_flock.py --self-test

It lists the open D1 PRs with prs.py (the ones the developer wrote, the
ones that wait for their review and the ones they are requested on), reads
each with pr_state.py, compares with the last pass in the state file
(default $BNAAS_HOME/flock/state-<login>.json), prints one line per change and
saves the new state. The first pass prints one summary line. A PR that left
the open lists is read once more, to say whether it merged or closed.

Each line is `<repo>#<n> <event>: <detail> <url>`, with the events review,
check, thread, mergeable, approval, changes, ready, merged, closed, stale (GitHub's
updatedAt two days old), stray (an open PR of the developer with no shepherd marker
younger than three hours in $BNAAS_HOME/shepherds/<repo>-<n>) and error. It
never writes to GitHub. watch_flock.sh runs it every 120 seconds, with --fresh on
the first pass of each dog, so that a new dog says every stray again.
"""
import datetime
import json
import os
import tempfile
import re
import sys
import time

HERE = os.path.dirname(os.path.realpath(__file__))
SKILLS = os.path.join(HERE, "..", "..")
sys.path.insert(0, os.path.join(SKILLS, "bnaas-whats-next", "scripts"))
sys.path.insert(0, os.path.join(SKILLS, "bnaas-shepherd-prs", "scripts"))
import prs  # noqa: E402
import pr_state  # noqa: E402

REPOS = ("vault", "cloudserver", "backbeat", "federation", "integration", "ring", "s3utils")
DEV = ("arsenal", "cloudserverclient")
STALE = 2 * 86400
SHEPHERD_FRESH = 3 * 3600
# The data folder: BNAAS_HOME, which each developer sets in their shell
# profile; never the home folder. main stops without it. A relative
# folder counts as none, because it moves with the cwd.
DATA = os.environ.get("BNAAS_HOME", "")
if not os.path.isabs(DATA):
    DATA = ""
SHEPHERDS = os.path.join(DATA, "shepherds")
URL = re.compile(r"https://github\.com/([^/]+/[^/]+)/pull/(\d+)")
# A Federation pin PR, the bot's or a developer's: no shepherd (Rule 4).
PIN = re.compile(r"-epic-image-pins(-bot)?$")


def snapshot(s):
    """The part of a PR state whose change is worth a line."""
    return {"state": s["state"], "draft": s["draft"], "review": s["review_check"]["state"],
            "failed": sorted(c["name"] for c in s["failed_checks"]),
            "threads": sorted(t["id"] for g in ("claude", "human") for t in s["threads"][g]),
            "mergeable": s["mergeable"], "approvals": s["approvals"], "head": s["head_sha"],
            "changes": s.get("changes_requested", [])}


def diff(key, url, old, new, s, login=""):
    """The lines for one PR, from its last snapshot to this one. The
    developer's own comments and verdicts are not news to them: skipped."""
    me = login.lower()
    out = []

    def say(event, detail):
        out.append("%s %s: %s %s" % (key, event, detail, url))

    if new["state"] != "OPEN":
        say(new["state"].lower(), "the PR is %s" % new["state"].lower())
        return out
    if old["draft"] and not new["draft"]:
        say("ready", "marked ready for review")
    if new["review"] != old["review"]:
        say("review", "Claude Code Review %s -> %s" % (old["review"], new["review"]))
    for c in s["failed_checks"]:
        if c["name"] not in old["failed"]:
            say("check", "%s %s %s" % (c["name"], c["conclusion"], c["url"] or ""))
    for g in ("claude", "human"):
        for t in s["threads"][g]:
            if t["id"] not in old["threads"] and t["author"] != me:
                say("thread", "new %s thread by %s: %s" % (g, t["author"],
                                                           " ".join(t["body"].split())[:80]))
    if new["mergeable"] != old["mergeable"]:
        say("mergeable", "%s -> %s" % (old["mergeable"], new["mergeable"]))
    for who in new["approvals"]:
        if who not in old["approvals"] and who != me:
            say("approval", "%s approved (%d approvals)" % (who, len(new["approvals"])))
    for who in new["changes"]:
        if who not in old.get("changes", []) and who != me:
            say("changes", "%s requested changes" % who)
    return out


def epoch(stamp, default):
    """Seconds since the epoch of a GitHub time stamp, or the default."""
    try:
        return int(datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp())
    except (AttributeError, ValueError):
        return default


def shepherded(repo, number, now):
    path = os.path.join(SHEPHERDS, "%s-%s" % (repo.split("/")[-1].lower(), number))
    # The developer stopped its shepherd: never a stray, until they start one.
    if os.path.exists(path + ".stopped"):
        return True
    try:
        return now - os.path.getmtime(path) < SHEPHERD_FRESH
    except OSError:
        return False


def tick(login, saved, now, list_prs, read, has_shepherd, fresh=False, d1=None):
    """One pass. Returns the lines and the new state; no I/O of its own."""
    # Only a dog with no state file at all says its summary line: an empty
    # flock must stay quiet, or it would wake the session at every pass.
    # The flag survives a failed first pass, so that the pass that finally
    # lists the PRs still says one summary line, not one "new" line per PR.
    # --fresh survives a failed pass too, so that the pass that lists the
    # PRs still says every stray again.
    fresh = fresh or bool((saved or {}).get("fresh"))
    lines, first = [], saved is None or bool(saved.get("first")) or fresh
    state = {"prs": {}, "error": None, "read_errors": {}}
    saved = saved or {"prs": {}, "error": None}
    try:
        rows, errors = list_prs(list(saved["prs"]))
    except Exception as e:  # GitHub down: say it once, keep the last state
        msg = str(e).splitlines()[0][:200] if str(e) else type(e).__name__
        if saved.get("error") != msg:
            lines.append("flock error: cannot list the D1 PRs: %s" % msg)
        saved["error"], saved["first"], saved["fresh"] = msg, first, fresh
        return lines, saved
    # A repository that did not answer: say it once, and keep its PRs as
    # they were, so that a merge during the outage is still reported.
    msg = "; ".join(errors)[:300] or None
    if msg and saved.get("error") != msg:
        lines.append("flock error: cannot list %s" % msg)
    state["error"] = msg
    listed = {r["url"] for r in rows}
    for url, rec in saved["prs"].items():
        if url not in listed:
            state["prs"][url] = rec
    # A PR into development/* (arsenal, cloudserverclient) is listed by author,
    # so it may be any work of the developer: only a D1 ticket makes it D1.
    # A PR already watched stays: a missing or half-written d1-jira.json on
    # one pass must not drop it, or its merge line could be missed.
    rows = [r for r in rows if not (r.get("base") or "").startswith("development/")
            or (r.get("ticket") or "") in (d1 or ()) or r["url"] in saved["prs"]]
    def pin(r):
        return bool(PIN.search(r.get("head") or ""))

    watched = [r for r in rows if r["kind"] in ("mine", "review") or r.get("asked")
               or r["url"] in saved["prs"] or pin(r)]
    for r in watched:
        m = URL.match(r["url"])
        repo, number = m.group(1), int(m.group(2))
        key = "%s#%d" % (repo.split("/")[1], number)
        old = saved["prs"].get(r["url"])
        try:
            s = read(repo, number)
        except Exception as e:  # said once per message, the last state kept
            if old:
                state["prs"][r["url"]] = old
            msg = (str(e).splitlines() or [type(e).__name__])[0][:120]
            if (saved.get("read_errors") or {}).get(r["url"]) != msg:
                lines.append("%s error: cannot read it: %s %s" % (key, msg, r["url"]))
            state["read_errors"][r["url"]] = msg
            continue
        new = snapshot(s)
        if new["state"] != "OPEN":
            if old:  # say it once, then forget it
                lines += diff(key, r["url"], old["snap"], new, s, login)
            continue
        rec = {"snap": new, "stale": old.get("stale") if old else None,
               # A new dog (fresh) says every stray again, once.
               "stray": old.get("stray", False) if old and not fresh else False,
               "mine": r["kind"] == "mine"}
        if old and old["snap"] != new:
            lines += diff(key, r["url"], old["snap"], new, s, login)
        elif not old and pin(r):
            # Said once, on any pass: whoever sees it first takes it.
            lines.append("%s pin: open pin PR, no shepherd: mark it ready, review it, merge "
                         "it once build-tests pass %s" % (key, r["url"]))
        elif not old and not first:
            lines.append("%s new: %s, %s %s" % (key, r["title"], r.get("status", ""), r["url"]))
        # Stale: GitHub's updatedAt is two days old; said once per updatedAt.
        updated = epoch(s.get("updated_at"), now)
        if now - updated > STALE and rec["stale"] != s.get("updated_at"):
            lines.append("%s stale: no change for %d days %s"
                         % (key, (now - updated) // 86400, r["url"]))
            rec["stale"] = s.get("updated_at")
        stray = rec["mine"] and not pin(r) and not has_shepherd(repo, number, now)
        if stray and not rec["stray"]:
            lines.append("%s stray: open, no shepherd runs on it %s" % (key, r["url"]))
        rec["stray"] = stray
        state["prs"][r["url"]] = rec
    if first:
        lines.insert(0, "flock: watching %d PRs: %s" % (
            len(state["prs"]), ", ".join(str(n) for n in sorted(int(URL.match(u).group(2)) for u in state["prs"]))))
    return lines, state


def self_test():
    with open(os.path.join(SKILLS, "bnaas-shepherd-prs", "evals", "fixtures",
                           "pr-state-draft.json")) as f:
        draft = json.load(f)
    U1, U2, U3 = ("https://github.com/scality/backbeat/pull/%d" % n for n in (1, 2, 3))
    # GitHub down on the very first pass: the next good pass is still the first.

    def down0(urls):
        raise RuntimeError("gh: timeout")

    lines, st0 = tick("anurag4DSB", None, 0, down0, None, None)
    assert lines == ["flock error: cannot list the D1 PRs: gh: timeout"] and st0["first"], st0
    rows = [{"url": U1, "kind": "mine", "title": "one", "status": "Draft"},
            {"url": U2, "kind": "review", "title": "two", "status": "Waiting"},
            {"url": U3, "kind": "other", "title": "three", "status": "Waiting"}]
    states = {1: json.loads(json.dumps(draft)), 2: json.loads(json.dumps(draft))}
    shep = {1: False}

    def listing(urls):
        return list(rows), []

    def read(repo, n):
        if n == 3:
            raise AssertionError("PR 3 is neither mine nor asked: never read")
        return states[n]

    def has(repo, n, now):
        return shep.get(n, True)

    t0 = epoch(draft["updated_at"], 0) + 60
    lines, st = tick("anurag4DSB", st0, t0, listing, read, has)
    assert lines[0] == "flock: watching 2 PRs: 1, 2", lines
    assert lines[1].startswith("backbeat#1 stray:") and len(lines) == 2, lines
    # Nothing changed: silence, and the stray is not said twice.
    lines, st = tick("anurag4DSB", st, t0 + 60, listing, read, has)
    assert lines == [], lines
    # A new dog on the same state file says the stray again, once.
    lines, st = tick("anurag4DSB", st, t0 + 61, listing, read, has, fresh=True)
    assert lines[0] == "flock: watching 2 PRs: 1, 2", lines
    assert [l.split()[1] for l in lines[1:]] == ["stray:"], lines
    # A new dog whose first pass cannot list the PRs says the stray at the
    # pass that can.

    def down(urls):
        raise RuntimeError("GitHub down")

    _, sd = tick("anurag4DSB", json.loads(json.dumps(st)), t0 + 62, down, read, has, fresh=True)
    lines, _ = tick("anurag4DSB", sd, t0 + 63, listing, read, has)
    assert [l.split()[1] for l in lines[1:]] == ["stray:"], lines
    # Changes on PR 1: LGTM, lint fixed, a new human thread, an approval, a conflict.
    p = states[1]
    p["review_check"]["state"] = "success"
    p["failed_checks"] = []
    p["threads"]["human"].append({"id": "T9", "author": "tmacro", "body": "Why\n 60?",
                                  "path": "a", "url": "u"})
    p["approvals"] = p["approvals"] + ["tmacro"]
    p["mergeable"] = "CONFLICTING"
    p["changes_requested"] = ["nicolas2bert"]
    p["updated_at"] = "2026-10-22T13:32:00Z"
    shep[1] = True
    lines, st = tick("anurag4DSB", st, t0 + 120, listing, read, has)
    text = "\n".join(lines)
    for want in ("backbeat#1 review: Claude Code Review action_required -> success",
                 "backbeat#1 thread: new human thread by tmacro: Why 60?",
                 "backbeat#1 mergeable: MERGEABLE -> CONFLICTING",
                 "backbeat#1 approval: tmacro approved (3 approvals)",
                 "backbeat#1 changes: nicolas2bert requested changes"):
        assert want in text, (want, text)
    assert "stray" not in text and len(lines) == 5, lines
    # Pin PRs, the bot's and the developer's own: one pin line each, even on a
    # first pass, and never a stray.
    pins = [{"url": "https://github.com/scality/Federation/pull/7300", "kind": "other",
             "title": "pins", "status": "", "base": "epic/E",
             "head": "improvement/ZZ-2-epic-image-pins-bot"},
            {"url": "https://github.com/scality/Federation/pull/7301", "kind": "mine",
             "title": "pins", "status": "", "base": "epic/E",
             "head": "improvement/ZZ-2-epic-image-pins"}]
    states[7300] = states[7301] = json.loads(json.dumps(draft))
    lines, pst = tick("anurag4DSB", None, t0, lambda urls: (pins, []), read, lambda *a: False)
    assert [l.split()[1] for l in lines[1:]] == ["pin:", "pin:"], lines
    lines, pst = tick("anurag4DSB", pst, t0 + 1, lambda urls: (pins, []), read, lambda *a: False)
    assert lines == [], lines
    # A development/* PR of the developer: ignored unless its ticket is D1.
    dev = {"url": "https://github.com/scality/arsenal/pull/9", "kind": "mine", "title": "x",
           "status": "", "base": "development/8.2", "ticket": "ZZ-534"}
    seen = []

    def read_dev(repo, n):
        seen.append(n)
        return read(repo, n)

    rows.append(dev)
    lines, _ = tick("anurag4DSB", st, t0 + 130, listing, read_dev, has)
    assert 9 not in seen and not any("arsenal#9" in l for l in lines), lines
    states[9] = json.loads(json.dumps(draft))
    lines, dst = tick("anurag4DSB", st, t0 + 130, listing, read_dev, has, d1={"ZZ-534"})
    assert any(l.startswith("arsenal#9 new:") for l in lines), lines
    # The Jira copy is missing on the next pass: the watched PR stays.
    del seen[:]
    _, dst = tick("anurag4DSB", dst, t0 + 140, listing, read_dev, has, d1=set())
    assert 9 in seen and dev["url"] in dst["prs"], (seen, list(dst["prs"]))
    rows.remove(dev)
    # The developer's own thread and approval on PR 2 (a review PR): no line.
    states[2]["threads"]["human"].append({"id": "T8", "author": "anurag4dsb",
                                          "body": "my note", "path": "a", "url": "u"})
    states[2]["approvals"] = states[2]["approvals"] + ["anurag4dsb"]
    lines, st = tick("anurag4DSB", st, t0 + 150, listing, read, has)
    assert lines == [], lines
    # A new failed check on PR 2.
    states[2]["failed_checks"].append({"name": "build", "conclusion": "failure", "url": "r"})
    lines, st = tick("anurag4DSB", st, t0 + 180, listing, read, has)
    assert lines == ["backbeat#2 check: build failure r " + U2], lines
    # Two days of quiet: stale once.
    lines, st = tick("anurag4DSB", st, t0 + 180 + STALE + 1, listing, read, has)
    assert sorted(l.split()[1] for l in lines) == ["stale:", "stale:"], lines
    lines, st = tick("anurag4DSB", st, t0 + 240 + STALE + 1, listing, read, has)
    assert lines == [], lines
    # PR 1 merges: one line, then it is gone from the state.
    states[1]["state"] = "MERGED"
    lines, st = tick("anurag4DSB", st, t0 + 300 + STALE, listing, read, has)
    assert lines == ["backbeat#1 merged: the PR is merged " + U1], lines
    assert list(st["prs"]) == [U2], st
    # PR 2 cannot be read: one line per message, its state kept.
    good = states[2]
    del states[2]
    for i in range(2):
        lines, st = tick("anurag4DSB", st, t0 + 350 + STALE + i, listing, read, has)
        assert lines == ([] if i else ["backbeat#2 error: cannot read it: 2 " + U2]), lines
        assert list(st["prs"]) == [U2], st
    states[2] = good
    # An empty flock with a state file stays quiet: no summary at each pass.
    lines, _ = tick("anurag4DSB", {"prs": {}, "error": None}, t0, lambda urls: ([], []),
                    read, has)
    assert lines == [], lines
    # GitHub down: one error line, the state kept, then silence.

    # One repository down: PR 2 is not listed, so it stays as it was.
    lines, st = tick("anurag4DSB", st, t0 + 330 + STALE,
                     lambda urls: ([], ["backbeat, base epic/E: timeout"]), read, has)
    assert lines == ["flock error: cannot list backbeat, base epic/E: timeout"], lines
    assert list(st["prs"]) == [U2], st
    lines, st = tick("anurag4DSB", st, t0 + 340 + STALE, listing, read, has)
    assert lines == [] and st["error"] is None, lines

    def down(urls):
        raise RuntimeError("gh: connection refused")

    lines, st2 = tick("anurag4DSB", st, t0 + 360 + STALE, down, read, has)
    assert lines == ["flock error: cannot list the D1 PRs: gh: connection refused"], lines
    lines, st2 = tick("anurag4DSB", st2, t0 + 420 + STALE, down, read, has)
    assert lines == [] and list(st2["prs"]) == [U2], lines
    # Markers: none is a stray, a fresh one is not, an old one is, and a
    # .stopped one never is.
    global SHEPHERDS
    saved_dir, SHEPHERDS = SHEPHERDS, tempfile.mkdtemp()
    try:
        now, path = time.time(), os.path.join(SHEPHERDS, "backbeat-7")
        assert not shepherded("scality/backbeat", 7, now)
        open(path, "w").close()
        assert shepherded("scality/Backbeat", 7, now)
        assert not shepherded("scality/backbeat", 7, now + SHEPHERD_FRESH + 1)
        open(path + ".stopped", "w").close()
        assert shepherded("scality/backbeat", 7, now + SHEPHERD_FRESH + 1)
    finally:
        SHEPHERDS = saved_dir
    print("self-test ok")


def main(argv):
    if argv[:1] == ["--self-test"]:
        self_test()
        return 0
    if not DATA:
        print("flock error: no data folder: set BNAAS_HOME", file=sys.stderr)
        return 2
    opts, args = {}, iter(argv)
    fresh = False
    for a in args:
        if a == "--fresh":
            fresh = True
            continue
        value = next(args, None) if a in ("--login", "--epic", "--state") else ""
        if not value or value.startswith("-"):
            print(__doc__, file=sys.stderr)
            return 2
        opts[a[2:]] = value
    login = opts.get("login") or prs.gh("api", "user", "--jq", ".login").strip()
    epic = opts.get("epic") or prs.find_epic()
    # The same folder as the pid file of watch_flock.sh, so two dogs with
    # different folders never share one state file.
    path = opts.get("state") or os.path.join(
        os.path.join(DATA, "flock"),
        "state-%s.json" % login)
    try:
        with open(path) as f:
            saved = json.load(f)
    except (OSError, ValueError):
        saved = None

    def listing(urls):
        found, errors = prs.collect(epic, REPOS, urls, dev=DEV)
        if errors and not found:
            raise RuntimeError("; ".join(errors))
        return [prs.classify(p, login) for p in found], errors

    def read(repo, number):
        return pr_state.state(repo, number, stacked=False)

    # The D1 ticket keys: what's next copies its Jira read (jira.json) here.
    try:
        with open(os.path.join(os.path.dirname(path), "d1-jira.json")) as f:
            d1 = {i["key"] for i in json.load(f)}
    except (OSError, ValueError, KeyError, TypeError):
        d1 = set()
    lines, state = tick(login, saved, int(time.time()), listing, read, shepherded, fresh, d1)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path + ".tmp", "w") as f:
        json.dump(state, f)
    os.replace(path + ".tmp", path)
    for line in lines:
        print(line, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
