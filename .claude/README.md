# BNaaS D1 shared Claude Code setup

This folder holds the Claude Code setup that every BNaaS D1 developer uses. It lives only on the backbeat epic branch, `epic/S3C-10971-bnaas-d1`, and the final merge removes it.

## What is here

| Path | What it does |
|---|---|
| `rules/bnaas-d1.md` | Team rules 1 to 7. Claude Code loads them at the start of each session. |
| `agents/bnaas-reviewer.md` | Read-only reviewer for diffs and plans. |
| `skills/bnaas-plan-pr/` | `/bnaas-plan-pr`: commits an approved plan and opens a plan PR. |
| `hooks/review-branch.sh` | Runs the CI review on the current branch of a repository and saves it (Rule 7). |
| `hooks/review-gate.sh` | Refuses a `git push` until a saved review exists for the pushed commit (Rule 7). |
| `settings.json` | Fable as the advisor, `plans/` as the plans folder, and the hooks. |
| `plans/` | Approved plans, one file per change. |

## Before you start

You need these tools:

- Claude Code, logged in with a claude.ai account. The Fable advisor runs only on the Anthropic API.
- `git` with SSH access to the scality repositories on GitHub.
- `gh`, logged in to GitHub.
- `python3`, for the hooks.
- The `scality-skills` plugin from the Scality agent hub, for the review before each push. The install section shows how to get it.
- The Atlassian connector, or an Atlassian MCP server, in Claude Code. Without it, Claude cannot create or update Jira tickets for you.

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
for d in rules agents skills hooks plans; do ln -sfn ../bnaas-team-setup/.claude/$d .claude/$d; done
ln -sfn ../bnaas-team-setup/.claude/settings.json .claude/settings.json
claude
```

If the workspace `.claude/` already has real folders with these names, move them away first. Otherwise `ln` puts the link inside the old folder.

Keep personal settings in the workspace `.claude/settings.local.json`. Git never commits it.

### The review skill, for both layouts

Install the plugin once, at user scope, so that the review runs in every repository:

```
claude plugin marketplace add scality/agent-hub
claude plugin install scality-skills@scality-agent-hub
```

## Make sure that it works

Start `claude` in the workspace folder, or in the backbeat clone for the one-repository layout. Then run these commands in the session:

| Command | Expected result |
|---|---|
| `/memory` | `rules/bnaas-d1.md` is in the list. |
| `/agents` | `bnaas-reviewer` is in the list. |
| `/advisor` | The advisor is Fable. |
| `/hooks` | `review-gate.sh` runs on PreToolUse for Bash. |
| `! claude plugin list` | `scality-skills@scality-agent-hub` is enabled. |
| Type `/bnaas` | `/bnaas-plan-pr` appears. |

## If a skill or a rule is missing

1. Make sure that Claude started in the right folder. Exit, run `pwd`, and start `claude` again from the workspace folder or the backbeat clone.
2. Make sure that the links work. `ls -l .claude` must show each entry pointing into `../bnaas-team-setup/.claude/`. If a link is broken, run the two `ln` lines of the install again. Do not run `worktree add` again, because the worktree already exists.
3. Make sure that the worktree is on the latest epic. Fetch first, then compare. The last two commands must print the same commit:

   ```
   git -C backbeat fetch origin epic/S3C-10971-bnaas-d1
   git -C bnaas-team-setup rev-parse --short HEAD
   git -C backbeat rev-parse --short origin/epic/S3C-10971-bnaas-d1
   ```

   If they differ, update the worktree as the next section shows.
4. Start a new session after any fix. A session reads the setup only at its start.

## If a push is refused

The message starts with "Rule 7". It names the repository and the command to run.

1. If it says "no local review", run `.claude/hooks/review-branch.sh <repository>`. Fix or report each finding, then push again. A new commit needs a new review.
2. If it says that the gate cannot check the push, push one branch of one repository per command: `git -C <literal path> push -u origin <branch>`. The gate cannot check a path with a shell variable, `GIT_DIR`, `--git-dir`, or a push behind another command.
3. If it says that the command runs other steps before the push, run the push as a command of its own. Only read-only steps, such as `cd` or `git fetch`, can come before it.
4. If it names `--all`, `--mirror` or `--tags`, push one branch per command.
5. If it cannot resolve a ref, check the branch name in the push command.

The saved reviews are in `.git/bnaas-review/` of each repository. Git never commits them.

`review-branch.sh` can also stop with one of these messages:

- "No commit of <branch> is missing from the other local branches": another local branch holds all the commits of this branch, for example the next branch of a stack. Review and push a branch before you stack the next one on it.
- "This output is not a review": the nested session did not finish the review. Read the printed output, then run the script again.
- "The review-pr skill is not installed": run the two install commands of the install section.

## Update the setup

When a PR changes the setup on the epic, update it and start a new session.

In the workspace layout, move the worktree to the new epic head:

```
git -C backbeat fetch origin epic/S3C-10971-bnaas-d1
git -C bnaas-team-setup switch --detach origin/epic/S3C-10971-bnaas-d1
```

In the one-repository layout, pull the epic branch in your clone:

```
git -C backbeat switch epic/S3C-10971-bnaas-d1
git -C backbeat pull --ff-only
```

## Daily flow

1. Plan the change in plan mode. When you approve the plan, run `/bnaas-plan-pr`. It needs a BB ticket, because plans live in the backbeat epic.
2. After the humans approve the plan PR, cut a ticket branch from the epic: `improvement/<ticket>-<message>`. The ticket comes from the Jira project of that repository, for example ARSN for arsenal or BB for backbeat.
3. After each change, run the `bnaas-reviewer` agent on the diff.
4. When the work is done, an Opus subagent checks it against the plan (Rule 4).
5. Before each push, run `.claude/hooks/review-branch.sh <repository>`. It runs the same review as CI, on your branch, and posts nothing (Rule 7). It takes 5 to 15 minutes, so Claude runs it in the background.
6. Open a PR into the epic. It needs two approvals.

## Where things live

Both developers have access to the tech spec doc. You do not need to update these places yourself. Claude updates Jira and the Tickets tab as part of the work (Rule 5), and tells you what it changed, with links.

| What | Where |
|---|---|
| Tech spec | https://claude.ai/code/artifact/bfe390f9-d819-4a93-9050-ffd5eee1b80a. The main tab holds the contracts. The Merge strategy tab holds decisions M1 to M9. The Tickets tab lists all the work, with owners and tickets. |
| Breakdown (frozen, v55) | https://claude.ai/artifact/QC6Cbrfm7a5hAfRw752c7L |
| Jira | The D1 epic is the key in the epic branch name. The search `parent = <that key>` lists its tickets. |
| Epic branches | `epic/S3C-10971-bnaas-d1` in arsenal, vault, cloudserver, cloudserverclient, backbeat, federation, ring and integration. |
| Rules in full | `rules/bnaas-d1.md` |

To see what to work on next, ask Claude "What's next?" (Rule 5).
