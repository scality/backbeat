---
name: bnaas-plan-pr
description: Commit an approved D1 plan to the plans folder on the backbeat epic and open a plan PR for human review. Use right after the developer approves a plan in plan mode, or when the user asks to save, commit, share or update a plan.
---

# Plan PR

Rule 2: humans review the plan, AI reviews the code. This skill turns an approved plan into a plan PR on the backbeat epic. It works in a temporary worktree, so the developer's own checkout and its uncommitted work stay as they are.

## Find the pieces

- The plan file: the newest file in `$CLAUDE_PROJECT_DIR/.claude/plans/` that is not committed yet, or the path the session printed when the plan was approved.
- The setup checkout: the git checkout that `.claude/plans` really lives in (`cd "$CLAUDE_PROJECT_DIR/.claude/plans" && git rev-parse --show-toplevel`). In the workspace layout, it is the `bnaas-team-setup` worktree of backbeat. In the one-repository layout, it is the developer's backbeat clone.
- The epic ref: `git -C <checkout> branch -r --list 'origin/epic/*-bnaas-d1'`. It prints the full remote ref, for example `origin/epic/S3C-10971-bnaas-d1`. Use it as printed, without adding `origin/`.
- The ticket: a BB ticket (the backbeat Jira project), because plans live in the backbeat epic. If this plan has none, ask the developer for one. Use it only in the branch name, the commit message and the PR title.

## Steps

1. Pick a short topic in kebab case from the plan's title. Rename the file to `YYYY-MM-DD-<topic>.md` with today's date. Do not change its content.
2. Check the plan: no em-dashes, no credentials, no personal or customer data. Fix only what breaks these rules, and tell the user.
3. Pick the worktree path once: run `echo "${TMPDIR:-/tmp}/plan-<topic>"`, and write the printed path in full in every later command. These steps call it `<worktree>`.
   Create a temporary worktree on a new ticket branch from the epic:
   `git -C <checkout> fetch origin`, then `git -C <checkout> worktree add -b improvement/<ticket>-plan-<topic> <worktree> <epic ref>`.
   Every ticket branch is named `improvement/<ticket>-<message>`, and Bert-E refuses unknown prefixes such as `plan/`.
4. Copy the plan file to `<worktree>/.claude/plans/`. In that worktree, `git add` that file only and commit it. Commit message (Rule 6): the subject `<ticket>: plan <topic>`, at most 55 characters, a blank line, then a body that names the plan file, wrapped at 72 characters.
5. Ask the user one question that names both actions: push the branch, and open the draft PR. After a yes, push and open a draft PR into the epic branch (the epic ref without `origin/`):
   - Title: `<ticket>: plan <topic>` (Rule 6).
   - Body: five sections, each a `##` heading followed by one to three sentences. Intent says what the plan decides. Verification says how a reviewer checks the plan.

     ```
     ## Intent: why does this change exist?
     ## System impact: what's affected, including downstream?
     ## Preserved behavior: what explicitly stays the same?
     ## Intended change: what's different after this PR?
     ## Verification: how do we know this worked, or how would we know if it didn't?
     ```

   - Reviewers (Rule 3, at least two approvals): if `gh api user --jq .login` is `anurag4DSB`, request `tmacro`. If it is `tmacro`, request `anurag4DSB`. Also request `nicolas2bert`. If he cannot review, request the team `scality/hopocalypse` instead.
6. Clean up: `git -C <checkout> worktree remove <worktree>`. Then delete the untracked plan file from the setup checkout. The file is on the plan branch now, and it comes back with the next update of the setup after the merge.
7. Report the PR link and the plan path.

## Updating a plan after review

Pick `<worktree>` as in step 3, and add a temporary worktree on the existing plan branch (`git -C <checkout> worktree add <worktree> improvement/<ticket>-plan-<topic>`). Edit the plan there, and commit the change as a new commit (never amend). Ask before the push, then remove the worktree. The PR history then shows each version, and `git revert` undoes one.

## Code PRs

The code PR's Intent section links the plan file on the epic. If the code moves away from the plan, update the plan first.
