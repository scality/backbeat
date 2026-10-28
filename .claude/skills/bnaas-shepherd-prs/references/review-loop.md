# The Claude Code review loop on a draft

This is the loop of the shepherd's subagent, for every draft of the stack, bottom first. `<base>` is the shepherd skill folder. Each PR has its worktree, at `<repo clone>/.claude/worktrees/<ticket>` (Rule 4).

## One pass

1. Write the shepherd marker again for every open PR of the stack: `for n in <open PR numbers>; do date +%s > ~/.bnaas/shepherds/<repo>-$n; done`.
2. Read the state: `python3 -I <base>/scripts/pr_state.py --repo scality/<repo> --pr <n> --stack`.
3. Act on every draft of the stack whose review has landed, as soon as it lands, bottom first; never wait for the reviews of the other PRs, and never wait for the long CI checks before a fold (they matter only for `draft_goal`):
   - `mergeable` is CONFLICTING (or `merge_state` DIRTY): treat it like a red check. Rebase the branch onto its base in its worktree, solve the conflict and run the tests of that repository; the push goes with the batch of step 4. If the conflict changes what the PR does, stop and ask the developer.
   - `failed_checks` is not empty: open each run (`gh run view <run id> --repo scality/<repo> --log-failed`), find the cause and fix it in the commit it belongs to; the push goes with the batch of step 4. A failure that the PR did not cause (a flaky job, an outage) gets one rerun with `gh run rerun <run id> --failed`; a second failure goes to the developer with the run link.
   - `review_check.state` is `action_required`: act at once, whatever the other checks are doing. Read every open thread of `threads.claude`, and the body of the last review by Claude Code (`gh pr view <n> --repo scality/<repo> --json reviews`). For each comment, fix it and fold the fix into the commit it belongs to (`git commit --fixup=<sha>`, then `GIT_SEQUENCE_EDITOR=: git rebase -i --autosquash $(git merge-base HEAD origin/<base branch>)`, which runs with no editor and starts from the fork point, so the commits below keep their shas even when the lower branch was already rewritten), or decide to reply why it stays. Do not push yet: fold every `action_required` PR of the stack in this pass first, then step 4 rebases and pushes them in one batch. Reply on each thread and resolve it after that push (commands below).
   - `review_check.state` is `failure` or `cancelled`: stop, and give the developer the run link: `review_check.run_url`, else the link of the `review / claude-review` job in `gh pr checks <n> --repo scality/<repo>` (`review_check.url` points at a Claude Code page, not at the run).
   - `review_check.state` is `pending` or `missing`: wait for that PR's review; only the review check itself makes the loop wait. `pending_checks` alone never delays a fold. `missing` waits at most 15 minutes after the last push: some repositories (cloudserverclient) have no Code Review workflow, so then hand the PR link to the developer with that reason, and stop waiting on that PR.
   - Stall alarm: when a thread of `threads.claude` has been open for more than ten minutes (its `created_at`) and no fold has started on it, tell the developer at once, in a What | State | Link table, with the reason (a conflict, a failing test, a comment that would change the plan).
4. After the folds of the pass, rebase upward once, bottom first: each PR above a folded one goes onto the new local head of the PR below it, in its worktree (`git rebase --onto <new head of the lower branch> <old head of the lower branch>`). Just before the batch, read `draft` of each PR again (`pr_state.py`): a PR that a human marked ready since the pass began leaves the batch and goes to the shepherd, which asks the developer (Rule 4). Then push every changed draft with lease in one batch, and only then reply on and resolve the threads of step 3. A fix for a conflict or a failed check goes into the same batch. A ready PR of the stack (`draft` false) is never pushed by this loop: rebase it in its worktree and return its new head to the shepherd, which asks the developer before the push (Rule 4). Rebase and push only the PRs whose `author` is the developer who runs the shepherd; list the others, with the new head they need, for the developer to pass on. Then check `mergeable` on the whole stack again.
5. After any fold, rewrite the PR description so that it matches the branch: the five sections of Rule 7, one to three sentences each, with the link to the task row. `gh pr edit <n> --repo scality/<repo> --body-file <tmp>/body.md`.
6. While some PR still in the loop has its review pending and none needs action, poll the whole stack every 120 seconds with the foreground `until` loop below, under a Bash timeout of ten minutes, then start the next pass. The loop writes the shepherd marker of every open PR at each poll, so that the dog never takes a waiting shepherd for a dead one. A PR leaves the loop when it is handed to its author (`draft_goal`, or answered comments only) or given up after the 15 minutes of `missing`; testing only the PRs still in the loop keeps a handed-off PR from making the wait spin:

   ```
   until python3 -I <base>/scripts/pr_state.py --repo scality/<repo> --pr <n> --stack | python3 -c 'import json, sys; w = {<PR numbers still in the loop>}; r = [s["review_check"]["state"] for s in json.load(sys.stdin) if s["number"] in w]; sys.exit(any(x in ("pending", "missing") for x in r) and not any(x in ("action_required", "failure", "cancelled") for x in r))'; do for m in <open PR numbers>; do date +%s > ~/.bnaas/shepherds/<repo>-$m; done; sleep 120; done
   ```

The loop ends when `draft_goal` is true for every draft of the stack, or when it needs the developer. A review check that stays `action_required` only on comments answered with why they stay also ends it for that PR: the AI review is done, so hand the PR to its author as for `draft_goal`, with the links to those threads, and do not wait for the LGTM. When every review is `success` and only CI checks are pending, wait for them with the same loop, testing `pending_checks` instead.

## Commands

Push with lease, only to a draft, or to a ready PR after the developer's yes, never to an epic branch:

```
git -C <worktree> push --force-with-lease origin <branch>
```

Reply on a thread, then resolve it, only when its first comment is by Claude Code (`claude` or `claude[bot]`):

```
gh api graphql -f query='mutation($id: ID!, $body: String!) { addPullRequestReviewThreadReply(input: {pullRequestReviewThreadId: $id, body: $body}) { comment { url } } }' -F id=<thread id> -F body=@<tmp>/reply.md
gh api graphql -f query='mutation($id: ID!) { resolveReviewThread(input: {threadId: $id}) { thread { isResolved } } }' -F id=<thread id>
```

Write the reply to `<tmp>/reply.md` with the Write tool first, never inline in the command: a quote in the text would break the shell. A reply names the fix and the new head, or says in one or two sentences why the code stays. No ticket key, no em-dash (Rule 7).

## Never

- Mark the PR ready (`gh pr ready`), merge it, or close it.
- Resolve a thread whose first comment is by a human.
- Push to an epic branch, or push without lease.
- Push to a PR that is not a draft: the shepherd asks the developer first.
- Add a fix-the-fix commit: every fix folds into the commit it belongs to (Rule 7).
