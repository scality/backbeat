---
name: bnaas-shepherd-prs
description: Take one D1 PR, or a whole stack of them, from open to merged. It runs the Claude Code review loop on a draft until the review gives its LGTM, the checks are green and the stack is rebased, then hands the draft to its author; on a ready PR it prepares the fixes of the review comments and pushes them only after the developer's yes; it says when a PR can merge, and retargets the next PR of the stack after a merge. It also opens a plan PR for a change that alters the shared contract, and for every Arsenal or cloudserverclient change. Use it right after a D1 PR is opened, when the watch-flock dog reports a stray PR, or when the developer says "shepherd", "babysit", "take care of", "watch" or "land" a PR or a stack.
---

# Shepherd the D1 PRs

The sheep are the PRs. The dog (`/bnaas-watch-flock`) watches them and tells the session what changed. The shepherd acts on that, and reports to the developer and to the journal. Every PR that a D1 session opens gets a shepherd (Rule 2), and the shepherd starts the dog when none runs.

The shepherd never marks a PR ready for review and never merges: a human does both. It never resolves a thread that a human opened.

In this file, `<base>` is the base directory of this skill, which Claude Code prints when it loads the skill. `<setup>` is the folder that holds `.claude/` of the shared setup, three levels above `<base>`. `journal` is `<setup>/.claude/hooks/journal`, and `open_url` is `sh <setup>/.claude/skills/bnaas-whats-next/scripts/open_url.sh`. `<tmp>` is the session scratchpad.

## 0. Take the flock

A Federation pin PR (branch `improvement/<ticket>-epic-image-pins`, or the bot's `-bot`) gets no shepherd (Rule 4): if asked to shepherd one, say so and stop.

1. Find who runs it (`gh api user --jq .login`) and the other developer: `anurag4DSB` and `tmacro` review each other (Rule 4).
2. Read the state of the PR, and of the PRs stacked on it:

   ```
   python3 -I <base>/scripts/pr_state.py --repo scality/<repo> --pr <n> --stack > <tmp>/flock.json
   ```

   It prints a list, the PR first, then each PR stacked above it. See "What pr_state.py gives" below.
3. For each open PR of the list, write the marker that tells the dog a shepherd runs, with the repository name in lower case: `mkdir -p "${BNAAS_HOME:?}"/shepherds && date +%s > "${BNAAS_HOME:?}"/shepherds/<repo>-<n>`. Write it again at each pass and at each poll while it waits, in every stage: the dog calls a PR stray only when its marker is older than three hours. The draft loop refreshes it at each poll. A PR in sections 2 and 3 can wait days for reviewers with no pass, so the shepherd also starts a background Bash command (`run_in_background`) that keeps the markers of its own PRs alive while the session lives: `while :; do touch -c "${BNAAS_HOME:?}"/shepherds/<repo>-<n> ...; sleep 1800; done`, with one path per PR it shepherds, and the Bash timeout at its maximum, `7200000`. When the tool reports that the command stopped (its timeout), start it again with the same list; when a PR joins the flock, stop it with TaskStop and start it again with the new list. `touch -c` never creates a marker, so a PR whose marker was removed stays unshepherded; when the session ends, the loop ends and the markers age into strays for the next session. Remove it when the PR merges or closes. When the developer stops the shepherd, replace it with `"${BNAAS_HOME:?}"/shepherds/<repo>-<n>.stopped`, so that the dog does not take the PR for a stray and start a new shepherd; when the developer starts a shepherd on that PR again, delete the `.stopped` file first.
4. Start the dog if none runs (`/bnaas-watch-flock`, its section 1). The dog is one per data folder (`$BNAAS_HOME`): when another session owns it, this session hears nothing from it, so the shepherd watches its own PRs with a Monitor of its own (`timeout_ms` 1800000, re-armed on expiry) that prints a line each time the stack changes: `while :; do python3 -I <base>/scripts/pr_state.py --repo scality/<repo> --pr <n> --stack | python3 -c 'import json, sys; print(" ".join("%s:%s:%s:%s:%d" % (s["number"], s["state"], s["draft"], s["review_check"]["state"], len(s["approvals"])) for s in json.load(sys.stdin)))'; sleep 120; done | awk '$0 != last { print; fflush(); last = $0 }'` (awk flushes each line, where `uniq` would hold them in its buffer).
5. If the lowest PR is based on the head branch of an open PR by the other developer (a piggyback), read that PR at each pass too (`pr_state.py --repo scality/<repo> --pr <m>`, without `--stack`). Never fold or push it; when it merges, retarget and rebase the lowest PR as section 4 says.
6. Route each PR by its state: a draft goes to section 1, a PR ready for review to section 2, then section 3 checks it for the merge, and section 4 takes a merged or closed one. A PR that appears later in `stacked` joins the flock at the next pass.

## 1. Draft: the Claude Code review loop

Run one Opus subagent (the `general-purpose` agent, default model) for the stack, in the background, and give it [references/review-loop.md](references/review-loop.md), the PR list, the worktree of each branch, and this goal:

> The "Claude Code Review" check is `success` on the head of each draft, every other check is green, every PR of the stack is MERGEABLE, every thread opened by Claude Code is fixed or answered and then resolved, the stack is rebased after any fold, and each PR description matches its branch.

The subagent folds each PR as soon as its own review lands (`action_required`), without waiting for the long CI checks or for the reviews of the rest of the stack, and raises the stall alarm of the reference when a Claude Code thread waits more than ten minutes. It runs the loop until the goal holds on every draft, or until it needs the developer: a failed or cancelled review run, a check that it cannot fix, a conflict that it cannot solve, or a comment that would change the plan. Every fold goes through the gate loop of step 4 of the reference before the push, as Rule 5 says: the dog reports a new Claude Code review, the shepherd folds, runs the local review gate and again with `--only` on the changed commits until a round brings no new real finding or after 3 rounds, then pushes on its own while the PR is a draft; on a ready PR it prepares and asks (section 2). It never marks a PR ready, never pushes to an epic branch, and never pushes a PR that is not a draft. It returns, for each PR, what it fixed, what it answered, and the head it pushed.

When the goal holds for a PR (`draft_goal` true in `pr_state.py`):

1. `open_url <url>`.
2. Tell the developer: "<repo>: <title> is ready for your check, mark it ready when you agree. <url>".
3. `journal note "PR <url> ready for the author"`.

## 2. Ready for review: the human comments

A human marked the PR ready. A draft opens with no reviewers (Rule 4), so the shepherd requests them: the other developer, `nicolas2bert` and the team `scality/hopocalypse`. At every pass, the first one included, it asks only those in neither `requested` (the pending requests of `pr_state.py`) nor `reviewed` (who already reviewed), with `gh pr edit <n> --repo scality/<repo> --add-reviewer <logins>`. A shepherd keeps no state across sessions, and GitHub drops a reviewer from the requests once they review, so asking again a reviewer who already reviewed would set their approval aside.

A ready PR still gets the machine side of the loop, but no push without the developer's yes (Rule 4). When its review check comes back `action_required` (the review runs again after each push), a check fails, or the PR turns CONFLICTING, prepare the fix as steps 3 and 4 of [references/review-loop.md](references/review-loop.md) say: fold it in the worktree and write each reply to a file. Run the local review gate of step 4 on it, looping as that step says. Do not push, and do not rebase the PRs above yet, so that a draft above never carries a fix that is not pushed: the fix goes into the question below. After a yes, rebase the PRs above that the developer wrote, push with lease, then reply to and resolve the Claude Code threads. Skip the draft hand-off of section 1: the PR stays ready, and nobody is told that it is ready for their check again.

At each pass, read the open threads in `threads.human` of `pr_state.py` (a human thread is one whose first comment is by someone else than the PR author and the bots), except the ones listed in `"${BNAAS_HOME:?}"/shepherds/<repo>-<n>.handled` (the developer took them, below; this applies to the Claude Code threads too), and the review bodies (`gh pr view <n> --repo scality/<repo> --json reviews`). Then sort each comment:

- **Mechanical**: a typo, a lint error, a missing link, a wrong word, a broken path. Prepare the fix: fold it into the commit it belongs to in the worktree, and write the reply. Do not push, and do not rebase the PRs above yet, so that a draft above never carries a fix that is not pushed: the fix goes into the question below. After a yes, rebase the PRs above that the developer wrote (list the others for them), push with lease and answer the thread with what changed and the new head, with the reply command of [references/review-loop.md](references/review-loop.md) (the text in a file, never inline). Do not resolve it: its author does.
- **Substantive**: anything else. For each one, prepare a diagnosis (what the reviewer saw, and whether they are right), a proposed fix with the reply that would go on the thread, and one line on why the change let it through. That line also goes to the Self-improvement section of the rule or skill that missed it.

Before the question, run the local review gate (step 4 of [references/review-loop.md](references/review-loop.md)) on each branch with a prepared fix, looping as that step says. Then ask ONE question with AskUserQuestion that shows every prepared fix with its reply, and lists every substantive comment with its diagnosis and proposed fix. Ask it even when every comment is mechanical: a ready PR gets no push without a yes. The options:

- "Review them one by one": ask about each fix and comment in turn, apply or skip each one, run the local review gate on what changed since the question, looping as step 4 of the reference says, then push once.
- "Apply all": make every proposed fix and fold it, run the local review gate on what changed since the question, looping as step 4 of the reference says, then push with the prepared fixes, and post each reply with its reason.
- "I handle them": push nothing, drop the prepared folds (reset the worktree to the head on origin), list the threads with their links, and add their ids to `"${BNAAS_HOME:?}"/shepherds/<repo>-<n>.handled`, one per line, so that later passes leave those threads to the developer and never prepare the same fixes again.

A "Changes requested" by a human who is in `changes_requested` and not in `requested`: address its comments as above, push after the developer's yes, then ask that reviewer again with `gh pr edit <n> --repo scality/<repo> --add-reviewer <login>`. Once asked again, they are in `requested`, so later passes leave that change request alone until they review again.

Every step of this section is a `journal note`: "PR <url>: pushed <n> review fixes after the developer's yes", "PR <url>: <n> comments wait for the author's decision", "PR <url>: review asked again after changes requested". Facts only, no names.

## 3. Ready to merge

When `merge_goal` is true (two approvals from distinct humans, not the author, not bots and not asked again since, every check green, MERGEABLE, no change requested and no open human thread), the PR can merge, with or without the other developer among the approvers (Rule 4):

1. `open_url <url>`, and tell the developer "<repo>: <title> is ready to merge. <url>".
2. `journal note "PR <url> ready to merge"`.
3. If `other_approved` is false: `journal note "PR <url> mergeable without the other developer's review: read it"`. The next what's next lists it under Review, as a read after the merge.

Only the human merges. Keep the shepherd on the PR until it merges or closes.

## 4. Merged or closed

- Merged: `journal note "PR <url> merged"`. If `other_approved` was false at the merge, note "PR <url> merged without the other developer's review: read it". Then take the next PR of the stack (the one in `stacked`): it moves to the branch the merged PR targeted, its `base`, called `<target>` here (the epic, or the development branch in Arsenal and cloudserverclient); the retarget is `gh pr edit <m> --repo scality/<repo> --base <target>`. Fetch first, since the worktree's `origin/<target>` is stale after the merge (`git -C <worktree> fetch origin`), then rebase its branch onto that branch (`git -C <worktree> rebase --onto origin/<target> <old head of the merged PR>`), and solve any conflict there; a conflict fix gets the local review gate (Rule 5) before the push. While it is a draft, retarget it and push with lease at once. When it is ready, ask the developer first, in one question that names the PR, its new base and the rebased head (Rule 4), and retarget and push only after the yes, so that a ready PR never shows the merged commits in its diff while it waits. Then route it again from section 0. Do this only when its `author` is the developer who runs the shepherd; for another author's PR, tell the developer what it needs. Remove the marker of the merged PR.
- Closed without a merge: `journal note "PR <url> closed without a merge"`, remove its marker, tell the developer, and stop shepherding that PR. The PRs stacked on it wait for the developer.

## Plan PRs

A plan PR comes first only when a change alters the shared contract beyond the approved tech spec (Rule 5). Every Arsenal or cloudserverclient change also gets one, opened right after its plan is approved and always before its code PR, because its plan lives only in the backbeat epic (Rule 5). The shepherd opens it into the backbeat epic, then shepherds it like any other PR.

1. The plan file is the path printed when the plan was approved, or else the newest uncommitted file in `$CLAUDE_PROJECT_DIR/.claude/plans/`. When another uncommitted plan in that folder could also match, ask the developer which one, and name the chosen file in the question of step 4. The setup checkout is `git -C "$CLAUDE_PROJECT_DIR/.claude/plans" rev-parse --show-toplevel`, and the epic ref is `git -C <checkout> branch -r --list 'origin/epic/*-bnaas-d1'`. For a contract change the ticket is a BB ticket: ask for it if there is none. For an Arsenal or cloudserverclient change it is the ticket of that change. It goes only in the branch, the commit and the PR title.
2. Rename the plan to `YYYY-MM-DD-<topic>.md`. Check it for em-dashes, credentials and personal data, and fix only those.
3. `git -C <checkout> fetch origin`, then `git -C <checkout> worktree add -b improvement/<ticket>-plan-<topic> <checkout>/.claude/worktrees/<ticket>-plan-<topic> <epic ref>`. Move the plan to `.claude/plans/` there (`mv`, as Rule 5 says, so that no untracked copy stays behind), and commit only that file: subject `<ticket>: plan <topic>`, a body that names the file (Rule 7).
4. Ask one question that names the plan file, the push and the draft PR. After a yes, push and open the draft PR into the epic: title `<ticket>: plan <topic>`, the five sections of Rule 7 (Intent says what the plan decides, Verification how a reviewer checks it), and no reviewers: section 2 requests them when the developer marks it ready.
5. Shepherd it from section 0. When it merges, remove the worktree; the plan comes back to the setup checkout with its next update. The code PR links the plan PR in Arsenal and cloudserverclient, and the plan on the epic elsewhere; if the code moves away from the plan, update the plan PR first.

## What pr_state.py gives

For each PR: `state` (OPEN, MERGED, CLOSED), `draft`, `head`, `head_sha`, `base`, `mergeable` and `merge_state`, `updated_at`, `review_check` (`success`, `action_required`, `failure`, `cancelled`, `pending` or `missing`, with the check `url` and the review job's `run_url`; a running job is `pending`, a failed job with no check is `failure`), `failed_checks` and `pending_checks`, `threads` (the open ones, split into `claude`, `human` and `other`, each with its id, path, url, `created_at` and first comment), `approvals` (distinct humans), `requested` (the pending review requests) and `reviewed` (the humans who reviewed), `changes_requested`, `other_dev` and `other_approved`, `stacked` (the open PRs based on its head, with their author), and the two goals, `draft_goal` and `merge_goal`. `python3 -I <base>/scripts/pr_state.py --self-test` checks it against `evals/fixtures/graphql-pr.json`. It exits 1 when GitHub does not answer: say so, and try again at the next pass.

## Report

After each pass that changed something, tell the developer in one Markdown table, What | State | Link, one row per PR that moved: what the shepherd did (fixed, answered, pushed, rebased, retargeted) or what waits for them. Then the links (Rule 7).

## Self-improvement

At the end of every run, ask: did any step, path or assumption here turn out wrong or incomplete, and did the run need a step this file does not mention? If so, fix the section where it belongs, in a worktree of your own on the standing self-improvement ticket, and open a draft PR into the backbeat epic (Rule 8). Find that ticket in Jira with the search `parent = <epic key> AND summary ~ "self-improvements of the D1 skills"`; if there is none, stop and say that the ticket is missing. The key stays out of this file (Rule 7). Keep it lean: no changelog, no list of workarounds. Never edit the shared setup checkout in place.
