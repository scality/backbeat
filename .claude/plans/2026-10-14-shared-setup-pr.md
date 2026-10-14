# Task 22a: the shared Claude Code setup PR into the backbeat epic, with a README

> Later change: the release watch (its hook and its skill) was dropped before this PR merged. The developers decide on upstream releases themselves.

## Context

The shared setup (team rules, the `bnaas-reviewer` agent, the release-watch hook, the `bnaas-plan-pr` and `bnaas-release-watch` skills, and two plan files) exists only in Anurag's local backbeat checkout. It sits uncommitted on `improvement/S3C-10971-shared-claude-setup`, at `85aaab3c`, which is also the head of `epic/S3C-10971-bnaas-d1`. Until it merges into the backbeat epic, nobody else has it. Anurag also could not reach one of the skills, and nothing tells a developer how to install the setup. This PR brings everything into the backbeat epic and adds a README with the install steps.

Bootstrap exception to Rule 2: the plan PR flow needs this setup on the epic first. So this PR carries its own plans (the shared setup plan, the task 3 plan, and this plan) instead of going through a separate plan PR.

## Step 1: BB ticket (Atlassian MCP)

Task in BB: "BNaaS D1: shared Claude Code setup on the backbeat epic". Parent S3C-10971, assignee Anurag, priority Medium, Severity Medium, Development Type BUILD, sprint "Hopocalypse 2026-10-05" (12750), then moved to In Progress. BB tasks have no Release notes field. Done when: the PR merges into `epic/S3C-10971-bnaas-d1` with two approvals, and a fresh session started as the README says loads the rules, the agent, both skills and the hook.

## Step 2: branch

`git -C backbeat branch -m improvement/S3C-10971-shared-claude-setup improvement/BB-<n>-shared-claude-setup`. The branch already sits on the epic head, so it needs no new cut. The uncommitted files stay.

Fixes from the last Opus verification, made in the same branch:
- The shared setup plan (line 97) and both copies of the task 3 plan (lines 10 and 44) name the branch `improvement/S3C-10971-shared-claude-setup`. They get the new BB name.
- `hooks/release-watch.sh` line 103 says "No action needed for a preview unless a trial merge shows conflicts." It becomes "No action needed for a preview.", because the skill makes no recommendation about taking in development.

## Step 3: README

New file `backbeat/.claude/README.md`, in plain English:
- What the folder holds: rules (Rules 1 to 4), the `bnaas-reviewer` agent, the two skills, the release-watch hook, `settings.json` (Fable advisor, plans folder, hooks) and `plans/`.
- Prerequisites: Claude Code, a claude.ai login (the Fable advisor runs only on the Anthropic API), `git` with SSH access to scality repositories, `gh` logged in, and the Atlassian connector for tickets.
- Install, two layouts:
  - Single repository: work in a backbeat clone checked out on `epic/S3C-10971-bnaas-d1`. Everything loads by itself.
  - Multi-repository workspace (one folder with all D1 clones): create a backbeat worktree of the epic, `git -C backbeat worktree add --detach ../bnaas-team-setup origin/epic/S3C-10971-bnaas-d1`. Then symlink `rules`, `agents`, `skills`, `hooks`, `plans` and `settings.json` from the workspace `.claude/` into it. Personal settings stay in the workspace `.claude/settings.local.json`, which is never committed. Start `claude` from the workspace folder.
- How to tell it works: `/memory` lists `rules/bnaas-d1.md`, `/agents` lists `bnaas-reviewer`, `/advisor` shows Fable, `/hooks` lists the release-watch hook, and `/bnaas-plan-pr` and `/bnaas-release-watch` exist.
- If a skill is missing: Claude was started outside the workspace or the backbeat checkout, a symlink is broken, or the worktree is not on the epic. The section gives the command that checks each case.
- Daily flow, in one short list: plan mode, then `/bnaas-plan-pr` (BB ticket), then a ticket branch `improvement/<ticket>-<message>` from the epic, then `bnaas-reviewer`, then the Opus verifier (Rule 4), then a PR with two approvals. Keep the worktree updated with `git -C bnaas-team-setup fetch && git -C bnaas-team-setup switch --detach origin/epic/S3C-10971-bnaas-d1`.
- Where to read more: `rules/bnaas-d1.md`, and the tech spec doc (Merge strategy and Tickets tabs).

## Step 4: review, commit, push, PR

- Run `bnaas-reviewer` on the full diff and fix what it finds. Also run `bash -n` on the hook, check that `settings.json` parses, and search for em-dashes and ticket keys outside plans.
- Commit only `.gitignore` and the `.claude/` files that git does not ignore: README, settings, rules, agent, hook, two skills, three plans. The plans are the shared setup plan, the task 3 plan, and this plan as `2026-10-14-shared-setup-pr.md`. Message: `BB-<n>: Add the shared Claude Code setup for BNaaS D1`, with a short body and no AI attribution.
- Ask Anurag before the push. After a yes: push `improvement/BB-<n>-shared-claude-setup` and open a PR into `epic/S3C-10971-bnaas-d1`, with reviewers `tmacro` and `nicolas2bert`. The PR description uses the five sections, and its Intent points new users to the README.
- After the PR: the Tickets tab row 22a gets Owner Anurag and the BB link, and a Jira comment links the PR. Log time spent on the ticket.

## Verification

- Rule 4: one Opus subagent does all the verification:
  - It checks the ticket fields, the branch name and base, and the committed file list (nothing personal, no state files).
  - It checks the README steps against the real layout, and the PR target and reviewers.
  - As part of the same check, it starts a headless Claude Code session from the workspace (`claude -p`). It confirms that a brand-new session loads the rules, the `bnaas-reviewer` agent and both `bnaas-*` skills. Reading the files alone cannot show what a new session loads.
- After the merge (not in this session): Anurag re-points the workspace symlinks to the `bnaas-team-setup` worktree, as the README says, and runs the README checks once.
