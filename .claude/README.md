# BNaaS D1 shared Claude Code setup

This folder holds the Claude Code setup that both D1 developers use, in Paris and in San Francisco. It lives only on the backbeat epic branch, `epic/S3C-10971-bnaas-d1`, and the final merge removes it.

The idea is simple. The two developers never work at the same time, so the shared state must be enough to start from. The Backlog tab of the tech spec doc holds the work and its dependencies, Jira holds who does what, GitHub holds the code, the other tabs of the doc hold the human view and the progress charts, and the journal holds what moved and what each session decided. Hooks write the journal as work happens, so nobody writes an end-of-day note. Four commands move a session through that state: `/bnaas-whats-next` at the start, `/bnaas-shepherd-prs` for each PR, `/bnaas-watch-flock` to watch them, and `/bnaas-pin-federation` when a service epic moves.

## What is here

| Path | What it does |
|---|---|
| `rules/bnaas-d1.md` | Team rules 1 to 8. Claude Code loads them at the start of each session. |
| `bnaas/` | The importer and backlog scripts: seed the Backlog tab from the breakdown, and list the tasks that are ready, blocked or with dependencies unknown. |
| `skills/bnaas-whats-next/` | `/bnaas-whats-next`: what moved since your last run, the PRs to review, the work to take next, and the ticket for a new task. |
| `hooks/journal-capture.sh` | `PostToolUse` hook: logs a fact for each git commit and push, `gh pr create`, `ready`, `merge` and `close`, test and lint run with its exit code, Jira ticket created, moved or edited, and write to the tech spec doc. Repositories outside `github.com/scality` are skipped. |
| `hooks/journal-flush.sh` | `Stop` hook: puts the logged facts in a batch and asks the session to start the `bnaas-journal` agent on it in the background. |
| `hooks/journal` | `journal note "<text>"` (`--decision` for a decision), `journal take`, `journal batch <id>`, `journal flushed <id>`, `journal tail --doc <export>`. `BNAAS_JOURNAL_CONFIG` points it at another `journal.json`, such as the practice tab of the evals. |
| `hooks/test-journal.sh` | Tests the hooks on sample input, without writing to the doc. |
| `bnaas/scripts/local-review.sh` | The local review gate (Rule 5): runs the `/review-pr` skill of the Code Review workflow on the whole range and on each commit, all in parallel, each in a scratch clone (about 1 USD per run), prints one combined report and posts nothing. It loops (Rule 5): fold what it finds, re-run with `--only <sha>...` on the changed commits, and stop when a round brings no new real finding or after 3 rounds. `--self-test` checks it without calling Claude. |
| `bnaas/journal.json` | The Journal tab of the tech spec, the D1 docs whose writes are logged, and the nodes of the what's next renders (the Tickets tab, the two charts and the Journal tab), whose writes are not. |
| `skills/bnaas-shepherd-prs/` | `/bnaas-shepherd-prs`: takes a PR or a stack from open to merged: the Claude Code review loop on the draft, the reviewers and the human comments on the ready PR, the merge call and the stack retarget. It also opens plan PRs. |
| `skills/bnaas-watch-flock/` | `/bnaas-watch-flock`: the dog, one Monitor per `$BNAAS_HOME` (in practice one per machine) that reports each change on the developer's PRs, the PRs they are asked to review, open pin PRs and the PRs with no shepherd. |
| `skills/bnaas-pin-federation/` | `/bnaas-pin-federation`: pins the CI images of the service epics in Federation and opens the pin PR. |
| `agents/bnaas-journal.md` | The journal writer: an Opus agent, run in the background, that appends one batch of facts at the end of the Log of the Journal tab, after it checks the Log for the facts shown before. |
| `skills/bnaas-journal/` | `/bnaas-journal`: records a note or a decision and starts the journal agent; it also answers the Stop message. |
| `agents/bnaas-reviewer.md` | Reviewer for diffs and plans. It runs the diff's tests and dry runs, and never edits files or writes to GitHub, Jira or the doc. |
| `settings.json` | Fable as the advisor, `plans/` as the plans folder, the journal hooks, and the permissions of the background journal agent, which cannot show a permission prompt: the `journal` command and the Claude Docs guide, read and update tools are pre-approved, and that update allow covers any Claude Doc the session can write. |
| `plans/` | Approved plans, one file per change. |

## Before you start

The command blocks of this README are bash, and fish cannot parse them: from fish, run `bash` first.

- Claude Code, logged in with a claude.ai account, in a version that has the Monitor tool (the dog of `/bnaas-watch-flock` runs in one), and with access to Fable, the advisor of `settings.json`.
- `git` with SSH access to the scality repositories, and `gh` logged in to GitHub.
- `python3` (3.10 or later), for the skill scripts. No package is needed.
- `docker`, logged in once with `docker login ghcr.io`, for the image checks of `/bnaas-pin-federation`.
- In the workspace layout, one clone per D1 repository, in folders with these names: `arsenal`, `vault`, `cloudserver`, `cloudserverclient`, `backbeat`, `federation`, `integration`, `ring` and `s3utils`.
- `yarn`, for the repository tests. It may live under nvm and be missing from non-interactive shells, so run `source ~/.nvm/nvm.sh` first. With nvm.fish, the nvm of the fish shell, there is no `~/.nvm/nvm.sh` and `nvm` runs only inside fish, so put the `bin` folder of the newest installed Node of the major version that the repository's CI uses (the `node-version` of its workflows) first on `PATH` instead, here for Node 22: `PATH="$(ls -d ~/.local/share/nvm/v22.* | sort -V | tail -n 1)/bin:$PATH" yarn test`. The journal hooks see a test run in this form, but not one inside `fish -c`. A fresh worktree needs `yarn install` before its tests run.
- For Vault, a GitHub Packages token in `~/.npmrc`, or `yarn install` fails: `//npm.pkg.github.com/:_authToken=<classic PAT with read:packages, SSO-authorized>`. The Vault tests do run on Node 22 on a Mac, despite what its CLAUDE.md says.
- The Atlassian connector in Claude Code, for Jira reads and writes. The Claude Docs connector, for the tech spec doc and its Journal tab.
- Nothing else for the journal: the hooks keep the facts in `$BNAAS_HOME/journal/buffer.jsonl` until the background journal agent appends them to the Journal tab, and log their errors to `$BNAAS_HOME/journal/errors.log`; they never stop a session.
- These skill sets, installed at user scope at the versions in use today. A bump of one is a setup PR.
  - scality-skills (`review-pr`, for the local review gate): the `scality-skills` plugin of the `scality-agent-hub` marketplace, `claude plugin marketplace add git@github.com:scality/agent-hub.git` (the repository is private, so use the SSH URL: the HTTPS one fails where git has no HTTPS credentials) then `claude plugin install scality-skills@scality-agent-hub`. `claude plugin install` takes the version that the marketplace has today, so check that `claude plugin list` shows 1.8.0; if it shows another version, stop and open a setup PR for the bump.
  - superpowers (`brainstorming`, `test-driven-development`, `systematic-debugging`, `verification-before-completion`): the plugin `superpowers@claude-plugins-official`, at version 7.0.0. If `claude plugin list` shows a superpowers plugin from another marketplace (an older version of this README installed it from https://github.com/obra/superpowers), remove it first with `claude plugin uninstall superpowers@<that marketplace>`, or every skill shows up twice. Then: `claude plugin marketplace add anthropics/claude-plugins-official` (skip it when `claude plugin marketplace list` already shows that marketplace), then `claude plugin install superpowers@claude-plugins-official`, and check that `claude plugin list` shows 7.0.0; if it shows another version, stop and open a setup PR for the bump.
  - Matt Pocock's skills (`grill-with-docs`, `implement`, `retro`, `handoff`), at commit `5beb35b040e3de97e54fa9e386d7e42d15c295c2`. The repository keeps them under `skills/<group>/<name>/`, and Claude Code finds a user skill only directly under `~/.claude/skills/<name>/`, so copy the four folders there, on Linux and macOS. The first line removes the clone that an older version of this README put one level too deep, and each folder is removed before its copy, so that a second run leaves no old file behind:

    ```
    rm -rf ~/.claude/skills/matt
    tmp=$(mktemp -d)
    git clone -q https://github.com/mattpocock/skills "$tmp/skills"
    git -C "$tmp/skills" checkout -q 5beb35b040e3de97e54fa9e386d7e42d15c295c2
    mkdir -p ~/.claude/skills
    for s in engineering/grill-with-docs engineering/implement engineering/retro productivity/handoff; do
      rm -rf ~/.claude/skills/"${s#*/}"
      cp -R "$tmp/skills/skills/$s" ~/.claude/skills/
    done
    rm -rf "$tmp"
    ```

If Claude Code in auto mode denies one of these installs, a plugin command or the skill block, run it yourself in a terminal, or with `!` in front of it in the Claude Code prompt.

## Install

Pick the layout that matches how you work.

### One repository

Work in a backbeat clone that is checked out on the epic branch. Claude Code loads the setup from it, with no other step.

```
git -C backbeat fetch origin epic/S3C-10971-bnaas-d1
git -C backbeat switch epic/S3C-10971-bnaas-d1
cd backbeat && claude
```

### A workspace with all D1 repositories

This is the usual layout: one folder that holds the clones of all D1 repositories, backbeat among them. Run these commands from that folder. They create a worktree of the backbeat epic, `bnaas-team-setup`, and link the workspace `.claude/` folder to it.

```
git -C backbeat fetch origin epic/S3C-10971-bnaas-d1
git -C backbeat worktree add --detach ../bnaas-team-setup origin/epic/S3C-10971-bnaas-d1
mkdir -p .claude
for d in rules agents skills plans bnaas hooks; do ln -sfn ../bnaas-team-setup/.claude/$d .claude/$d; done
ln -sfn ../bnaas-team-setup/.claude/settings.json .claude/settings.json
claude
```

The `hooks` link matters: `settings.json` runs the journal hooks from `.claude/hooks/` of the folder where Claude Code starts, so without the link the hooks fail and log nothing. If the workspace `.claude/` already has real folders with these names, move them away first. Otherwise `ln` puts the link inside the old folder.

Keep personal settings in the workspace `.claude/settings.local.json`. Git never commits it.

### Local data

The hooks and skills keep their local data (the journal buffer, the dog's state, the shepherd markers, the backlog cache) in one folder, `BNAAS_HOME`. Nothing goes to the home folder.

Set it once in your shell profile (`~/.zshenv` for zsh, so that Claude Code started from an IDE or the desktop app sees it too, `~/.config/fish/config.fish` for fish, where the same `export` line works, or `~/.bashrc`), as an absolute path to the `.bnaas` folder of the workspace folder, or of the backbeat clone in the one-repository layout:

```sh
export BNAAS_HOME="<workspace>/.bnaas"
```

This step is required. The SessionStart hook creates the folder and puts a `.gitignore` in it, so that git ignores the folder in both layouts. Without an absolute `BNAAS_HOME`, the hook warns the session, the journal logs nothing, the dog does not start and `/bnaas-whats-next` stops: nothing falls back to another folder.

Start every Claude Code session for D1 in the workspace folder, however small the task, never in a subfolder: the project settings, the hooks and the skills load only from the workspace `.claude/`. Remove `BNAAS_JOURNAL_DIR` and `BNAAS_FLOCK_DIR` from the `env` block of your settings if an older setup put them there: nothing reads them anymore.

## Make sure that it works

Start `claude` in the workspace folder, or in the backbeat clone for the one-repository layout, and run these commands in the session:

| Command | Expected result |
|---|---|
| `/memory` | `rules/bnaas-d1.md` is in the list. |
| Ask "which agent types can you start?" (`/agents` no longer lists them) | `bnaas-reviewer` and `bnaas-journal` are in the answer. |
| `/advisor` | The advisor is Fable. If it says that Fable bills to usage credits, run `/model fable` once to enable them, then `/advisor` again, then `/model` to set your own model back, since `/model fable` also makes Fable your default model. |

If one is missing, check that Claude started in that folder, and that `ls -l .claude` shows each link pointing into `../bnaas-team-setup/.claude/`; run the `ln` lines of the install again for a broken one.

Then run `.claude/hooks/test-journal.sh`: it ends with "all journal tests passed". In a new session, ask "what's next". The answer lists what moved since your last run, the PRs that wait for your review, the ready tasks and the other developer's work. Then export the Backlog tab to Markdown and run `python3 -I .claude/bnaas/scripts/backlog.py --backlog <export.md> check`. It reports the task count and no cycle.

## Daily flow

1. `/bnaas-whats-next`. Review first, then claim one task. The skill creates the ticket and fills the row, and starts the dog, `/bnaas-watch-flock`, which reports each change on your PRs.
2. Plan in plan mode, in a worktree of the ticket. The plan ships as the first commit of the PR. In Arsenal and cloudserverclient, the shepherd opens it as a plan PR into the backbeat epic before the code PR, and the code PR links it.
3. Build with `test-driven-development`, review the diff with `bnaas-reviewer` before each commit, run the local review gate before each push and loop it (Rule 5), then open a draft PR with no reviewers and hand it to `/bnaas-shepherd-prs` (Rule 5). The shepherd fixes what the Claude Code review raises and tells you when the draft is ready for your check; you mark it ready, the shepherd requests the three reviewers of Rule 4 and from then on asks you before each push, and you merge when it says the PR can merge.
4. When a service epic moves, `/bnaas-pin-federation` updates the Federation pins in one PR.
5. When a decision, a blocker or a surprise happens, say it to `/bnaas-journal` at that moment. The hooks log the rest, and a background agent writes it all to the Journal tab.

## Update the setup

In the workspace layout, update the worktree: `git -C bnaas-team-setup fetch origin epic/S3C-10971-bnaas-d1 && git -C bnaas-team-setup checkout --detach origin/epic/S3C-10971-bnaas-d1`. Then re-run the `for d in ...` link loop of "Install" from the workspace folder: the update does not create the links of folders that the setup gained since your install, such as `bnaas`. In the one-repository layout, `git pull` on the epic. A change to the setup is a PR into the backbeat epic, like any other change.

If an older setup left a `~/.bnaas` folder, stop its dog (TaskStop) and quit every Claude Code session. If `$BNAAS_HOME` does not exist yet, move the old folder there: `mv ~/.bnaas "$BNAAS_HOME"`. If it exists, add the old journal buffer to the new one, copy the rest without replacing the files already there, then delete the old folder: `mkdir -p "$BNAAS_HOME/journal" && { [ ! -f ~/.bnaas/journal/buffer.jsonl ] || { cat ~/.bnaas/journal/buffer.jsonl >> "$BNAAS_HOME/journal/buffer.jsonl" && rm ~/.bnaas/journal/buffer.jsonl; }; } && rsync -a --ignore-existing ~/.bnaas/ "$BNAAS_HOME"/ && rm -rf ~/.bnaas`. If the folder sits inside a git clone, add a `.gitignore` of `*` to it yourself: the hook adds one only to a folder it creates.

## Where things live

- Code: the epic branches of Vault, CloudServer, backbeat, Federation, Integration and RING, and the development branches of Arsenal and cloudserverclient.
- Work: the Backlog, Tickets and Progress tabs of the tech spec doc, and the Jira epic. A dependency or scope fix is an edit of the Backlog tab, with no PR.
- What moved and what was decided: the Journal tab of the tech spec doc (https://claude.ai/artifact/QhKsCqtGNzaVice35nvip9). Its Log is one append-only list, oldest first, each bullet with its UTC time and a fact id, so that sessions on both machines can append at once. The log before 2026-10-28 stays in the old journal doc (https://claude.ai/artifact/Bgoru7knkQrfMGVAbDoGEM), read-only.
- Deployment: the Federation epic, pinned to the CI images of the service epics by commit.
