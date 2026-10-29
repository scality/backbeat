# Three journal fixes found in a fresh setup

## Context

A fresh D1 setup on Arch Linux, and the session that followed it, hit three gaps in the journal:

- The capture hook does not see a check run inside a shell's `-c` string. `fish -c 'nvm use 24; and yarn test'` logs nothing, so a red test run under nvm.fish never reaches the other developer. The setup PR (backbeat#2913) works around it with a `PATH=` prefix and says in the README that a run inside `fish -c` goes unlogged.
- `journal note` cuts a note at 300 characters with no warning. A 560-character setup note would have reached the Journal tab cut mid-path ("so ~/."); it was caught only by reading the buffer.
- The `bnaas-journal` agent's first append was refused in nearly every run of the day, because it sent the Log list's id in the short form that an outline read prints (`.1408`); each retry with the full id (`m1yth80jczv.1408`) worked.

Branch `improvement/BB-932-journal-fixes`, stacked on `improvement/BB-932-linux-fish-setup` (backbeat#2913): this PR rewrites that PR's README sentence, and `test-journal.sh` passes on Linux only with that PR's `sha1sum` change. Worktree `backbeat/.claude/worktrees/BB-932-journal-fixes`, with no upstream tracking. The shepherd retargets it to the epic when #2913 merges.

## Design

1. **Checks inside `sh -c`, `bash -c`, `zsh -c`, `dash -c` and `fish -c` are logged.** In `segments()` (`hooks/journal`), a segment whose first word is one of those shells (by basename), followed by `-c` or a short-option cluster that ends in `c` (`-lc`, `-ec`), is replaced by the segments of the string after it, by recursion; a shell with no word after its `-c` stays one segment, as today. The unwrap feeds every step of the string to `from_bash()`, so a commit, a push, a `gh pr` step or a `cd` inside a `-c` string is read too. For fish, a leading `and` or `or` word is dropped from each inner segment, and so are the `VAR=value` prefixes after it; a shell behind the `and` is then unwrapped too. The string is not stripped of here-documents a second time: the outer pass already took them out, and a second pass would swallow every line after a `<<EOF`. The last inner segment takes the outer segment's `piped`, so `bash -c '...' | tail` still logs exit unknown. `from_bash()` needs no change: the exit code still belongs to the last segment, and `nvm` already counts as a setup step, so `fish -c 'nvm use 22; and yarn test'` failing with code 2 logs `yarn test exit 2`.
2. **`journal note` refuses a note over 300 characters.** In `note()`, after the `--why` and `--tasks` text is added, the note is measured with its blanks folded, as `append()` folds them. Over `MAX_TEXT`, it prints to stderr how long the note is and the limit, appends nothing and exits 2, so the session shortens it. `append()` keeps cutting the facts of the hooks at 300 characters: a hook must never fail. The longest note a skill writes is about 115 characters with a PR URL in it, so no caller breaks.
3. **The journal agent names the Log list by its full id.** Step 3 of `agents/bnaas-journal.md` gets one sentence, in the Docs guide's own terms: an outline prints a session's first id in full and the later ones short, `.1408` standing for the prefix of the last full id above it plus `.1408`; send the full form, since a short one is refused.

## Changes

- `hooks/journal`: `segments()` unwraps a shell's `-c` string; `note()` refuses a note over the limit.
- `hooks/test-journal.sh`, next to the nvm cases, each with an exit code or script name that no earlier fact has, so that no check passes on a leftover line: `fish -c 'nvm use 22; and yarn test'` failing with code 4 logs `yarn test exit 4`; `bash -lc 'cd <repo> && yarn lint'`, run from `$TMP`, which is not a clone, failing with code 5 logs `yarn lint exit 5` under the repository it moved to; `sh -c 'yarn lint:md' | tail -3` logs `yarn lint:md exit unknown`; `fish -c 'nvm use 22; and S3BACKEND=mem yarn mocha tests/x.js'` failing with code 6 logs `mocha exit 6`; `fish -c 'nvm use 22; and bash -c "yarn test:nested"'` failing with code 8 logs `yarn test:nested exit 8`; `bash -c` with a here-document before `yarn typecheck:heredoc` logs `yarn typecheck:heredoc exit 0`; and, as a guard, `echo "fish -c 'yarn test'"` logs nothing. Next to the note cases: a 301-character note exits 2, names 301 and 300 on stderr and leaves the buffer as it was; a 200-character note with a 200-character `--why`, and a 290-character note with `--tasks`, are refused too, with the buffer unchanged; and, as guards, a 300-character note lands whole, and so does one of 398 characters as typed that folds to 299. Next to the wrapper cases, as a guard: `bash -c < x.sh && yarn test:after` keeps `bash -c` as one step and still logs `yarn test:after exit 0`.
- `skills/bnaas-journal/evals/`: a sixth grader in `append-five-new-facts`, the append names the list by its full id and no update is refused for its id, and the same clause in that case's expected outcome. The practice Log of every case is reset by deleting the items after its two seed bullets, keeping the list, so that an outline still prints the list's id short as in the real Log; a list replaced by a new session would print in full, so the grader also checks that the outline printed it short, and fails when it did not. When it prints in full, the runner rewrites the Log heading and its list in one write, heading first, so that the list's id prints short below the heading's full one; the cases' precondition says so.
- `agents/bnaas-journal.md`: the full-id sentence in step 3.
- `skills/bnaas-journal/SKILL.md` step 1, Rule 2 of `rules/bnaas-d1.md` and the `hooks/journal` row of `README.md`: a note is at most 300 characters with the ` | why: ` and ` | tasks: ` parts that the journal adds, and a longer one is refused.
- `README.md`: the nvm.fish line drops "but not one inside `fish -c`".

All of it is inside backbeat's `.claude/`, which already has its Remove before merge row.

## Commits

1. The plan, under `.claude/plans/2026-10-29-journal-fixes.md`.
2. Tests: the wrapper and note-length cases (each one but the guards red before commit 3), the eval grader and the reset wording of the cases.
3. Code: `segments()` and `note()`.
4. Docs: the agent, the skill, Rule 2 and the README.

Subjects `BB-932: ...`, at most 55 characters; no ticket keys in code, tests or skill text; no em-dashes; no AI attribution trailer.

## Out of scope

- A cut of hook facts at a word boundary instead of at 300 characters.
- A `cd` inside a `-c` string still carries over to the segments after the shell, although a child shell's `cd` does not move its parent, so `bash -c 'cd ../vault' && git push` reads the push in vault. A `cd` inside `( ... )` has the same flaw today; scoping both needs markers that `from_bash()` handles.
- A here-document whose terminator shares its line with the closing quote of the `-c` string (`EOF'`): `strip_heredocs()` does not see the terminator and swallows the rest of the command, as it does outside a `-c` string today.
- Commands behind `env`, `exec`, `nohup` or `xargs`, and other forms of a shell's command string: `bash -cl`, `bash -e -c`, `fish --command`, `sh -c -- '...'`. A fish `set -gx PATH` step is not a setup step either, so a failed check after it logs "may not have run".
- The two shepherd findings of the setup PR's review loop (its poll loop outlives the Bash timeout; `gh run view --log-failed` failed where `gh api .../actions/jobs/<id>/logs` worked): a separate change to the shepherd skill.

## Verification

- `hooks/test-journal.sh` from the worktree: the new cases but the guards fail on commit 2, all pass on commit 3, and the suite ends with "all journal tests passed".
- The capture hook fed this session's real command, `fish -c 'nvm use 24; and yarn test'`, with a scratch `BNAAS_HOME`, logs `yarn test exit <code>`.
- `journal note` with a 301-character note prints the refusal and exits 2; the buffer is unchanged.
- `bnaas-reviewer` on the diff before each commit, then the local review gate loop before the push: the whole range and one run per commit, about 2.5 to 3.5 USD each, so 12.5 to 17.5 USD a round, at most 3 rounds. Then a draft PR into `improvement/BB-932-linux-fish-setup`, with no reviewers, handed to the shepherd.
- The agent fix is a text change. Before the first eval run, one outline read of body node `f093d6dc-8533` shows that the practice Log list's id prints short; if it prints in full, rewrite the heading and the list as the precondition says, since grader 06 fails until then. The new grader then checks the fix when the evals run on the practice tab, and after the merge the next real batches should append on the first try.
