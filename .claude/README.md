# BNaaS D1 shared Claude Code setup

This folder holds the Claude Code setup that both D1 developers use, in Paris and in San Francisco. It lives only on the backbeat epic branch, `epic/S3C-10971-bnaas-d1`, and the final merge removes it.

The idea is simple. The two developers never work at the same time, so the shared state must be enough to start from. The Backlog tab of the tech spec doc holds the work and its dependencies, Jira holds who does what, GitHub holds the code, the other tabs of the doc hold the human view and the progress charts, and the journal holds what moved and what each session decided. Hooks write the journal as work happens, so nobody writes an end-of-day note. One skill moves a session through that state: `/bnaas-whats-next` at the start.

## What is here

| Path | What it does |
|---|---|
| `rules/bnaas-d1.md` | Team rules 1 to 8. Claude Code loads them at the start of each session. |
| `bnaas/` | The importer and backlog scripts: seed the Backlog tab from the breakdown, and list the tasks that are ready, blocked or with dependencies unknown. |
| `skills/bnaas-whats-next/` | `/bnaas-whats-next`: what moved since your last run, the PRs to review, the work to take next, and the ticket for a new task. |
| `hooks/journal-capture.sh` | `PostToolUse` hook: logs a fact for each git commit and push, `gh pr create`, `ready`, `merge` and `close`, test and lint run with its exit code, Jira ticket created, moved or edited, and write to the tech spec doc. Repositories outside `github.com/scality` are skipped. |
| `hooks/journal-flush.sh` | `Stop` hook: asks the session to append the logged facts to the Log of the journal doc, then to run `journal flushed`. |
| `hooks/journal` | `journal note "<text>"` (`--decision` for a decision), `journal flush`, `journal flushed`, `journal tail --doc <export>`. |
| `hooks/test-journal.sh` | Tests the hooks on sample input, without writing to the doc. |
| `bnaas/journal.json` | The journal doc, the D1 docs whose writes are logged, and the nodes of the what's next renders (the Tickets tab and the two charts), whose writes are not. |
| `skills/bnaas-shepherd-prs/` | `/bnaas-shepherd-prs` (next PR): shepherds each PR the session opens through its review loop, and opens a plan PR for a change that alters the shared contract beyond the tech spec. |
| `skills/bnaas-watch-flock/` | `/bnaas-watch-flock` (next PR): the watch dog that what's next starts over the open PRs. |
| `skills/bnaas-pin-federation/` | `/bnaas-pin-federation` (added by a later PR of this stack): pins the CI images of the service epics in Federation and opens the pin PR. |
| `agents/bnaas-reviewer.md` | Reviewer for diffs and plans. It runs the diff's tests and dry runs, and never edits files or writes to GitHub, Jira or the doc. |
| `settings.json` | Fable as the advisor, `plans/` as the plans folder, and the journal hooks. |
| `plans/` | Approved plans, one file per change. |

## Before you start

- Claude Code, logged in with a claude.ai account.
- `git` with SSH access to the scality repositories, and `gh` logged in to GitHub.
- `python3` (3.10 or later), for the skill scripts. No package is needed.
- `docker`, for the image checks of `/bnaas-pin-federation`.
- `yarn`, for the repository tests. It may live under nvm and be missing from non-interactive shells, so run `source ~/.nvm/nvm.sh` first. A fresh worktree needs `yarn install` before its tests run.
- For Vault, a GitHub Packages token in `~/.npmrc`, or `yarn install` fails: `//npm.pkg.github.com/:_authToken=<classic PAT with read:packages, SSO-authorized>`. The Vault tests do run on Node 22 on a Mac, despite what its CLAUDE.md says.
- The Atlassian connector in Claude Code, for Jira reads and writes. The Claude Docs connector, for the tech spec doc and the journal doc.
- Nothing else for the journal: the hooks keep the facts in `~/.bnaas/journal/buffer.jsonl` until the session appends them to the journal doc, and log their errors to `~/.bnaas/journal/errors.log`; they never stop a session.
- These skill sets, installed at user scope and pinned to the commits in use today. A bump of either sha is a setup PR.
  - superpowers (`brainstorming`, `test-driven-development`, `systematic-debugging`, `verification-before-completion`): the Claude Code plugin from https://github.com/obra/superpowers, at commit `2ce8aa15d05bd57213e6f746f953aafb7ded7a8b`.
  - Matt Pocock's skills (`grill-with-docs`, `implement`, `retro`, `handoff`): `git clone https://github.com/mattpocock/skills ~/.claude/skills/matt && git -C ~/.claude/skills/matt checkout 5beb35b040e3de97e54fa9e386d7e42d15c295c2` on Linux and macOS, then follow its README.

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

## Make sure that it works

Start `claude` in the workspace folder, or in the backbeat clone for the one-repository layout, and run these commands in the session:

| Command | Expected result |
|---|---|
| `/memory` | `rules/bnaas-d1.md` is in the list. |
| `/agents` | `bnaas-reviewer` is in the list. |
| `/advisor` | The advisor is Fable. |

If one is missing, check that Claude started in that folder, and that `ls -l .claude` shows each link pointing into `../bnaas-team-setup/.claude/`; run the `ln` lines of the install again for a broken one.

Then run `.claude/hooks/test-journal.sh`: it ends with "all journal tests passed". In a new session, ask "what's next". The answer lists what moved since your last run, the PRs that wait for your review, the ready tasks and the other developer's work. Then export the Backlog tab to Markdown and run `python3 -I .claude/bnaas/scripts/backlog.py --backlog <export.md> check`. It reports the task count and no cycle.

## Daily flow

1. `/bnaas-whats-next`. Review first, then claim one task. The skill creates the ticket and fills the row.
2. Plan in plan mode, in a worktree of the ticket. The plan ships as the first commit of the PR.
3. Build with `test-driven-development`, review the diff with `bnaas-reviewer` before each commit, then open a draft PR with no reviewers. Fix what the Claude Code review on the PR raises (Rule 5); the developer then marks it ready, and the three reviewers of Rule 4 are requested then.
4. When a service epic moves, `/bnaas-pin-federation` (added by a later PR of this stack) updates the Federation pins in one PR.
5. When a decision, a blocker or a surprise happens, `.claude/hooks/journal note "<one sentence>"` at that moment, from the folder the session started in (the path is relative to it). The hooks log the rest.

## Update the setup

In the workspace layout, update the worktree: `git -C bnaas-team-setup fetch origin epic/S3C-10971-bnaas-d1 && git -C bnaas-team-setup checkout --detach origin/epic/S3C-10971-bnaas-d1`. Then re-run the `for d in ...` link loop of "Install" from the workspace folder: the update does not create the links of folders that the setup gained since your install, such as `bnaas`. In the one-repository layout, `git pull` on the epic. A change to the setup is a PR into the backbeat epic, like any other change.

## Where things live

- Code: the epic branches of Vault, CloudServer, backbeat, Federation, Integration and RING, and the development branches of Arsenal and cloudserverclient.
- Work: the Backlog, Tickets and Progress tabs of the tech spec doc, and the Jira epic. A dependency or scope fix is an edit of the Backlog tab, with no PR.
- What moved and what was decided: the journal doc (https://claude.ai/artifact/Bgoru7knkQrfMGVAbDoGEM).
- Deployment: the Federation epic, pinned to the CI images of the service epics by commit.
