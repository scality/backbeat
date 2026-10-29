# One local data folder for the D1 setup

## Context

The D1 hooks, scripts and skills keep their local data in `~/.bnaas`: the journal buffer and last-read, the dog's state and pid, the shepherd markers, and the backlog cache. Anurag wants every chat to keep that data in the workspace folder instead, never in the home folder. Today two folders have overrides (`BNAAS_JOURNAL_DIR`, `BNAAS_FLOCK_DIR`), but the shepherd markers and the backlog cache have none, and the default is always `~`.

Each developer sets one variable, `BNAAS_HOME`, in their shell profile, so that every process of the session, hooks, Bash commands, the dog under Monitor and background agents, inherits the same absolute folder whatever folder Claude Code started in. Without it, the D1 workflow refuses to work instead of guessing a folder.

Ticket: the standing self-improvement ticket, on branch `improvement/BB-932-one-data-folder`, in the worktree `bnaas-team-setup/.claude/worktrees/BB-932-one-data-folder`, cut from the backbeat epic head.

## Design

- One variable, `BNAAS_HOME`: the folder of all local D1 data, an absolute path that each developer exports in their shell profile. Nothing derives it: not the folder where Claude Code started, and never the home folder.
- Every script uses `$BNAAS_HOME/<sub>`; the old per-folder overrides go, and the tests set `BNAAS_HOME` to a temp folder. A relative value counts as unset, because it moves with the cwd. Without a folder, `journal` and the dog stop with "set BNAAS_HOME", and the capture and Stop hooks log nothing and exit 0.
- A hook cannot refuse a session, so a new SessionStart hook, `hooks/bnaas-home.sh`, checks the variable. Set, absolute and writable: it creates the folder quietly, with a `.gitignore` of `*` only in a folder it made, so that a path pointed at a repository by mistake never hides its files. Otherwise it prints a warning into the session that the D1 journal, the dog and the D1 skills are off. It always exits 0.
- `/bnaas-whats-next`, the start of every session, checks `BNAAS_HOME` first and stops with the same message.
- Skill text uses `"${BNAAS_HOME:?}"` in commands that write a file, so an unset variable stops the command. It does not catch a relative value; the scripts and the SessionStart warning do.
- Every D1 session still starts in the workspace folder, because the project settings, hooks and skills load only from its `.claude/`. The data no longer depends on that folder.
- zsh users set the variable in `~/.zshenv`, so that Claude Code started from an IDE or the desktop app sees it too.

## Changes

- `settings.json`: the SessionStart hook, in the same `"$CLAUDE_PROJECT_DIR"/.claude/hooks/...` form as the others.
- New `hooks/bnaas-home.sh`; `hooks/journal`, `hooks/journal-capture.sh`, `hooks/journal-flush.sh`; `skills/bnaas-watch-flock/scripts/watch_flock.sh` and `watch_flock.py`, whose `SHEPHERDS` constant and state path move under `$BNAAS_HOME`.
- Skill text: `bnaas-whats-next/SKILL.md` (the check, last-read, the flock copy of the Jira list, the backlog cache), `bnaas-watch-flock/SKILL.md`, `bnaas-shepherd-prs/SKILL.md` and `references/review-loop.md` (the shepherd markers). The journal agent and its evals pass `BNAAS_HOME` instead of the removed `BNAAS_JOURNAL_DIR`.
- `README.md`: the shell-profile export as a required install step, the start-in-the-workspace rule, and a one-time merge of an old `~/.bnaas` into the new folder.
- `hooks/test-journal.sh`: tests for each folder case and for the SessionStart hook.

All of this is inside backbeat's `.claude/`, which already has its Remove before merge row.

## Commits

1. The plan, under `.claude/plans/`.
2. Tests: `test-journal.sh` cases for the data folder and the SessionStart hook (red first).
3. Code: the hook, `settings.json`, the scripts.
4. Docs: the skill text and the README.

Subjects `BB-932: ...`, at most 55 characters; no ticket keys in code or skill text; no em-dashes.

## Out of scope

Each is a separate change, noted for later: the dog starts before what's next writes the Jira list, so it counts non-D1 `development/*` PRs for one pass; `sprint.py --jira -` input shape is not documented; read 2 breaks in zsh when the `--pr` flags are built in a string.

## Verification

- `hooks/test-journal.sh` ends with "all journal tests passed". Its folder cases run with a clean environment: `BNAAS_HOME` alone, a relative value, none (`journal` stops with the message; every hook exits 0 and writes nothing anywhere), and a `CLAUDE_PROJECT_DIR` that is never used as a fallback.
- The SessionStart hook exits 0 in every case. With `BNAAS_HOME` set it creates the folder quietly, with a `.gitignore` only in a folder it made; missing, relative or not writable, it prints the warning and creates nothing.
- `watch_flock.sh --running` exits 2 with no `BNAAS_HOME`, and the dog's self-test passes.
- `grep -rn '~/.bnaas\|HOME/.bnaas\|BNAAS_JOURNAL_DIR\|BNAAS_FLOCK_DIR' .claude --exclude-dir=plans` finds only the README migration lines.
- `bnaas-reviewer` on the diff, the local review gate loop before the push, then the shepherd on the draft.
- After the merge: put the export in the shell profile, drop the `env` block of the workspace `settings.local.json`, restart Claude Code in the workspace folder, and check that `echo $BNAAS_HOME` prints the workspace `.bnaas`, that the dog and a background `bnaas-journal` agent both find it, and that `~/.bnaas` does not come back.
