---
name: bnaas-watch-flock
description: Start the dog that watches every open D1 PR of the developer, and the PRs they are asked to review, across the D1 repositories, and reports each change as one line: the Claude Code review state, a failed check, a new thread, a merge conflict, a new approval, a merge, a PR with no change for two days, and a PR of theirs that no shepherd looks after. It never acts. Use it at the start of a session (what's next starts it), when a PR is opened (the shepherd starts it), or when the developer says "watch my PRs", "watch the flock" or "tell me when something changes".
---

# Watch the flock

The dog watches and barks; it never acts. The shepherd (`/bnaas-shepherd-prs`) acts on what it reports. One dog per machine: what's next starts it at the start of a session when none runs, and the shepherd starts it when it opens a PR. A second session on the same machine starts none, and its shepherd watches its own PRs (shepherd section 0).

`<base>` is the base directory of this skill, which Claude Code prints when it loads the skill.

## 1. Start the dog

1. Check that none runs: `sh <base>/scripts/watch_flock.sh --running` exits 0 when one does. If one runs and this session did not start it, another session on this machine owns it: say so, and start none.
2. Start it with the Monitor tool, with these arguments:
   - `command`: `sh <base>/scripts/watch_flock.sh --fresh --login <login>` for the first dog of the session, with the GitHub login of `gh api user --jq .login`. `--fresh` makes its first pass say every stray again.
   - `description`: `D1 PRs of <login>`.
   - `timeout_ms`: 1800000. If the tool caps it lower, it says so when the monitor expires; re-arm it then.
3. When the Monitor reports that it expired, start it again with the same arguments without `--fresh`, so that a re-arm does not repeat the strays. The dog keeps its state in `~/.bnaas/flock/state-<login>.json` (in `$BNAAS_FLOCK_DIR` when set, next to its pid file), so a new one does not repeat the old events. Stop it with TaskStop when the developer asks.

The first pass prints one line, `flock: watching <n> PRs: <numbers>`, and the strays. After that, the dog stays quiet until something changes.

## 2. What it watches

Every 120 seconds, `scripts/watch_flock.py` lists the open D1 PRs with `prs.py` of the what's next skill. A PR into `development/*` (arsenal, cloudserverclient) counts only when its ticket is a D1 ticket of `d1-jira.json` in the dog's folder, which what's next writes; any other PR of the developer there is ignored. It watches the ones the developer wrote, the ones that wait for their review, and the ones they are asked on, in vault, cloudserver, backbeat, federation, integration, ring and s3utils into the epic, and in arsenal and cloudserverclient into `development/*`, with the PRs stacked on them. It reads each one with `pr_state.py` of the shepherd (without the stacked read, since `prs.py` already follows the stacks), and compares with the last pass. It never writes to GitHub.

Each line is `<repo>#<n> <event>: <detail> <url>`:

| Event | When |
|---|---|
| `review` | the Claude Code Review check changed state (`action_required`, `success`, `failure`, `cancelled`, `pending`) |
| `check` | another check failed |
| `thread` | a new open thread, by Claude Code or by a human |
| `mergeable` | the merge state changed, for example MERGEABLE to CONFLICTING |
| `approval` | a new approval by a human |
| `changes` | a human requested changes, even with no inline thread |
| `ready` | a human marked the draft ready for review |
| `merged`, `closed` | the PR left the open list |
| `new` | a PR joined the flock after the first pass |
| `pin` | an open Federation pin PR (branch `-epic-image-pins` or `-bot`), said once: no shepherd (Rule 4) |
| `stale` | no change on GitHub (its `updatedAt`) for two days; said once per update |
| `stray` | an open PR of the developer, not a pin PR, with no shepherd: no marker `~/.bnaas/shepherds/<repo>-<n>` (repository in lower case) written in the last three hours, and no `~/.bnaas/shepherds/<repo>-<n>.stopped` (the developer stopped its shepherd) |
| `error` | GitHub, or one repository, did not answer; said once until it answers again, and the PRs of that repository keep their last state |

## 3. What the session does with a line

- `pin`: tell the developer: the first developer to see it marks it ready, reviews it and merges it once its `build-tests` pass (Rule 4). Start no shepherd.
- `stray`: start `/bnaas-shepherd-prs` with no question, one shepherd per stack: on the lowest stray of the stack: the stray whose base is not the head of another stray (`prs.py --json` gives `base` and `head`). Its base is usually the epic or a `development/*` branch, but it can be the branch of the other developer's PR (a piggyback); the shepherd then watches that lower PR and retargets after it merges. The shepherd takes the PRs stacked on the stray.
- A line about a PR that a shepherd of this session looks after: hand it to that shepherd.
- Anything else: tell the developer, in one Markdown table What | State | Link, one row per PR (Rule 7). A `review`, `approval` or `thread` on a PR that waits for their review comes first, because the other developer is blocked on it.

## Self-improvement

At the end of every run, ask: did any step, path or assumption here turn out wrong or incomplete, and did the run need a step this file does not mention? If so, fix the section where it belongs, in a worktree of your own on the standing self-improvement ticket, and open a draft PR into the backbeat epic (Rule 8). Find that ticket in Jira with the search `parent = <epic key> AND summary ~ "self-improvements of the D1 skills"`; if there is none, stop and say that the ticket is missing. The key stays out of this file (Rule 7). Keep it lean: no changelog, no list of workarounds. Never edit the shared setup checkout in place.
