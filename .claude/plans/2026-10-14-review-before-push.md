# PR 2887: keep .claude out of the markdown lint, and run the CI review before every push

## Context

Two things on the setup PR (BB-916, scality/backbeat#2887):

- The `lint` job fails. `lint_md` runs `mdlint $(git ls-files '[^bucket-scanner/]*.md')`, which takes in the 9 files under `.claude/` (MD013 line length, MD041 in the agent file). `development/9.6` has no `.claude/` folder.
- The CI review (`Claude Code Review`) posted one finding: `$TMPDIR` is unset on Linux, so the `bnaas-plan-pr` worktree path can become `/plan-<topic>`.

Anurag wants a rule that we run the same review ourselves before every push, enforced by a Claude Code hook, for every repository.

Facts (read only):
- CI runs `/review-pr` from the plugin `scality-skills@scality-agent-hub` (repository scality/agent-hub), through `scality/workflows/.github/workflows/claude-code-review.yml@v2`, on `claude-opus-5-5`.
- The skill has a local mode. On a branch, it reviews the commits that no other local branch has, prints the review as text and posts nothing.
- The skill sets `disable-model-invocation: true`, so the model cannot start it with the Skill tool. A headless `claude -p "/scality-skills:review-pr"` runs it as a user command.
- The plugin is not installed on this machine. A nested session in arsenal or integration does not read backbeat's project settings, so the plugin must be installed at user scope.

## Changes, all on `improvement/BB-916-shared-claude-setup` (PR 2887)

1. Lint. `lint_md` becomes `mdlint $(git ls-files '[^bucket-scanner/]*.md' ':!:.claude/')`. 22 files stay linted instead of 31. The exclusion goes away with the `.claude/` folder before the final merge.
2. `.claude/hooks/review-branch.sh [repo dir]`. It runs `claude -p "/scality-skills:review-pr" --model opus` in that repository. It saves the output as `<git common dir>/bnaas-review/<HEAD sha>.md`, then prints the path and the review. It records nothing if HEAD moves during the run or the output is empty. If the plugin is missing, it prints the two install commands and stops.
3. `.claude/hooks/review-gate.sh`, a `PreToolUse` hook on `Bash`, registered in `.claude/settings.json`.
   - It acts only on a `git push`. A push with `--delete`, `-d` or `--dry-run` passes.
   - It finds the repository from `git -C <dir>`, from a leading `cd <dir> &&`, or else from the hook's `cwd`. If it cannot resolve the directory (for example `$r` in a loop), it refuses and asks for one push per command.
   - It finds the commit from the refspec source (a local branch), or else from HEAD.
   - With no review file for that sha, it denies the push with a reason that names the command to run. A new commit needs a new review.
4. Rule 7 in `.claude/rules/bnaas-d1.md`: "Review locally before every push." Before every push, run `review-branch.sh` on the branch. Fix each finding, or tell the developer why it stays, in the same question that asks for the push. The hook enforces the rule for every repository of the session. Rule 2's AI review line names the local run too.
5. README: install the plugin at user scope (`claude plugin marketplace add scality/agent-hub`, then `claude plugin install scality-skills@scality-agent-hub`). The daily flow gets the review step. Troubleshooting covers "push refused: no local review".
6. The CI finding: `${TMPDIR:-/tmp}` in the four places of `bnaas-plan-pr/SKILL.md`.
7. A plan record, `.claude/plans/2026-10-14-review-before-push.md`.

Scope: the gate catches a forgotten review. It is not a security boundary, and a command written to evade it can get through.

Why a `PreToolUse` gate on `git push`, and not the other hooks: a Stop hook fires at the end of every turn, with no link to a push. A commit gate would start a review of about two minutes for every commit. The push is the point where the CI review would otherwise be the first review. The review runs as its own visible step, not inside the hook, so that its findings stay in the session.

Commits (Rule 6), after the branch was rebuilt into three commits:
- `BB-916: Add the shared Claude Code setup for BNaaS D1` (holds item 6)
- `BB-916: Run the CI review locally before a push` (items 2 to 5 and 7)
- `BB-916: Keep .claude out of the markdown lint` (item 1)

## Also on this machine

- Install the plugin at user scope with the two commands above. This changes `~/.claude`.


## Verification

- Lint: `git ls-files` with the new pathspec lists 22 files and nothing under `.claude/`. The `lint` job passes on PR 2887 after the push.
- Gate smoke tests with JSON payloads on stdin:
  - A command that is not a push passes with no output.
  - `git push -u origin <branch>` with no review file is denied.
  - The same push with a review file passes.
  - `git -C ../arsenal push` and `cd ../integration && git push` check the right repository.
  - `--delete` passes, and a `$r` directory is denied.
- End to end: a fresh `claude -p` session in the workspace tries `git push` in a scratch repository and gets the denial reason.
- Real use: `review-branch.sh` on the BB-916 branch creates the review file, and I fix or report its findings.
- Rule 4: an Opus verifier at `high` effort checks the hook behavior, the commits, the rule text and the README against this plan.
