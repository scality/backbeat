# D1 shared Claude Code setup, upstream release watch, and release-line findings

> Later change: the release watch (its hook and its skill) was dropped before this PR merged. The developers decide on upstream releases themselves.

## Context

Anurag and Taylor build BNaaS D1 together with Claude Code. They want four things:
1. A small shared setup in git, so that both sessions follow the same rules. Rule 1: Fable is the advisor, and subagents do the review, analysis and cross-cutting checks.
2. An answer to this question: the D1 base branches are still in preview (backbeat `development/9.6` and others), so does that affect D1?
3. A release watch: when a release lands on a base branch (for example a preview that becomes GA), Claude tells the user and summarizes the effect on the D1 code.
4. Plans first, kept in git. Every change starts with a plan. The plan is saved in a `plans/` folder in the backbeat epic, and the humans review the plan. AI reviews the code (the `bnaas-reviewer` agent locally, the review-pr skill in CI).

Decided with Anurag: the home is backbeat, on `epic/S3C-10971-bnaas-d1`. The workspace `bnaas-d1-delivery/.claude` symlinks to a backbeat worktree. Taylor uses a claude.ai account, so the advisor (Anthropic API only) works for both people. This change is the seed of task 22a. Tasks 18 to 21 come later.

## Part 1: release-line findings (answer to "does the preview affect us?")

Checked on 2026-10-14 with `gh api` and `git ls-remote`.

| Repo, D1 base | Latest release on that base | What S3C (Federation `development/10`, 10.1.0.0, preview week 6) deploys today |
|---|---|---|
| backbeat `development/9.6` | `9.6.0-preview.5` (5 previews since 2026-09-21, about one a week, no GA). Branch is 8 commits past it. | `9.5.1-federation` |
| cloudserver `development/9.5` | `9.5.0-preview.5`, no GA. Branch is 104 commits past 9.4.7. | `9.4.4-federation` |
| arsenal `development/8.4` | `8.4.27` (GA line). The 8.6 line is `8.6.0-preview.5`. | Vault 7.94.0 pins `8.4.24` (34 commits behind) |
| vault `development/7` | `7.94.0`, branch equals the tag | `7.94.0` |
| federation, ring, integration `development/10` | `10.1.0.0_pw6` (weekly previews) | itself |
| cloudserverclient `development/1.0`, s3utils `development/1` | npm-style versions, low churn | not deployed |

Effect on D1, most important first:
1. **Arsenal line mismatch: decided, Arsenal 8.6 everywhere.** CloudServer 9.5 and backbeat 9.6 install `@scality/arsenal` `8.6.0-preview.5` from npm, so an 8.4 epic pin would downgrade them. Anurag decided: the Arsenal epic starts from `development/8.6`, and every consumer pins its prereleases, Vault included. The Vault epic takes the Arsenal jump from 8.4.24 to 8.6, which is 110 or more commits, Prettier included, and the Vault tests show any break early. The Vault team must know before the final merge into Vault's latest development branch. This changes the kickoff (Arsenal base 8.4, prereleases like `8.4.25-bnaas.1`) and task 3 step 2b. Those changes go to Anurag as proposals.
2. **No S3C line deploys our bases yet.** Federation 10.1 deploys CloudServer 9.4 and backbeat 9.5. Task 3 pins epic images built from CloudServer 9.5 and backbeat 9.6, so the Federation epic carries the whole preview train: Node 24, a Debian trixie base image, a compiled TypeScript image, the new env-var configuration path in backbeat, Prettier on all of CloudServer, and Arsenal 8.6. Integration failures can come from that train and not from D1. Open question for the team: who moves Federation `development/10` onto these lines, and which S3C release ships D1?
3. **Preview churn in the files D1 uses.** Since 9.5.3, backbeat changed `lib/queuePopulator` (16 commits), `lib/BackbeatConsumer.js` (9) and `lib/config.joi.js` (5). `extensions/index.js` did not change, so new extensions are still picked up automatically. `extensions/notification` got 3 mechanical commits only. In CloudServer, the D1 files changed only through the Arsenal rename, Prettier and Orbit removal. Every weekly preview adds to the work of the final merge.
4. **Good side.** Basing on 9.6 gives D1 the pass-through of rdkafka parameters on every consumer (useful for task 53a), the config override path, and Node 24 now. Task 11 (local rig) must use Node 24.

## Part 2: shared setup (backbeat, PR into the epic)

Facts (code.claude.com/docs/en/advisor, settings, sub-agents, memory, hooks): `"advisorModel": "fable"` is valid in a committed `.claude/settings.json`, as a default and not a lock. Subagents inherit the advisor. CI on Vertex ignores it. `.claude/rules/*.md` loads at launch, and symlinked rule files and directories are documented as supported. The docs say nothing about symlinked `agents/` or `settings.json`, so a live session must test them. A plugin cannot carry `advisorModel` or rules, so the setup does not use a plugin.

```
backbeat/
  .gitignore                          + .claude/settings.local.json, .claude/worktrees/, .claude/release-watch.state
  .claude/settings.json               advisorModel: fable, plansDirectory: .claude/plans, hooks from Part 3
  .claude/rules/bnaas-d1.md           team rules: Rule 1, Rule 2 (Part 4), then a footer for later rules
  .claude/agents/bnaas-reviewer.md    model: inherit, read-only tools
  .claude/hooks/release-watch.sh      Part 3
  .claude/skills/bnaas-release-watch/SKILL.md   Part 3
  .claude/skills/bnaas-plan-pr/SKILL.md         Part 4
  .claude/plans/2026-10-14-shared-setup-and-release-watch.md   this plan, the first entry
```
All of it sits under `.claude/`, so the final merge (task 93) removes one folder and the plan history stays on the epic.

Rule 1 (in `rules/bnaas-d1.md`, about 15 lines): the advisor is Fable, and the session consults it before a design choice, before a non-trivial change, and when stuck. Fable never writes code and never runs as a subagent. The main session and the subagents use the default model. The session delegates side work to subagents and runs independent subagents in parallel: `Explore` for analysis and cross-repo search, `Plan` for change design, and `bnaas-reviewer` for a diff review and cross-cutting checks. The session reports findings, not transcripts.

`bnaas-reviewer`: its description starts "Use proactively after a code change and before a commit or PR". Checklist: correctness; contract values match the approved spec version; new behavior is behind the flag or the setting, and nothing changes while they are off; legacy notification code is untouched; no D2 to D4 work; secrets are never logged; tests use real services; no ticket references or em-dashes in code. It reports findings ranked by severity, with `file:line`. Task 21 can reuse the checklist.

## Part 3: release watch (hook detects, skill analyzes, human decides)

**Hook `release-watch.sh` (SessionStart, matcher `startup|resume`, `"async": true`, plus UserPromptSubmit, `"async": true`).** The docs say that an async hook never delays the session, and its `additionalContext` reaches Claude on the next turn. The docs do not say whether SessionStart shows `systemMessage` to the user. For that reason, the context also tells Claude to put the notice at the top of its next reply. The live test checks whether `systemMessage` appears.
- It uses `git ls-remote --tags` against the GitHub URLs, so it needs no local clone and works from the workspace or from inside a repo. It checks the repos in parallel.
- Watch table inside the script: backbeat `^9\.6\.`, cloudserver `^9\.5\.`, arsenal `^8\.6\.`, vault `^7\.`, federation, ring and integration `^10\.1\.`, cloudserverclient, s3utils. A tag with `-preview` or `_pw` is a preview. Any other tag is GA.
- It compares the tags with `${CLAUDE_PROJECT_DIR}/.claude/release-watch.state` (local, never committed). It checks at most every 6 hours. If the network fails, it exits 0 and stays silent.
- When it finds a new tag, it prints a `systemMessage` for the user, for example "backbeat 9.6.0 released (GA, was 9.6.0-preview.5) on development/9.6. Run /bnaas-release-watch for the effect on D1." It also adds `additionalContext`, so that Claude offers to run the skill. A preview gets one short line. A GA release gets the full message.
- On UserPromptSubmit, the same 6-hour limit applies, so a session that stays open for days still sees a new release. No timer hook exists, so a prompt is the trigger.

**Skill `bnaas-release-watch` (manual, or offered by the hook).** For each new tag:
1. It runs `git fetch` on the tag, the base branch and the epic.
2. Upstream: the commits from the epic's merge base to the tag, grouped by area, with the dependency, Node, Dockerfile and config-schema changes called out.
3. Ours: `git diff --stat` from the merge base to the epic.
4. Overlap: the files changed on both sides, and a trial merge with `git merge-tree --write-tree`, which predicts conflicts without touching the working tree.
5. Stability: preview or GA, whether S3C (Federation `development/10` group_vars) deploys that line, and the CI result of the tag.
6. A short summary of the effect on D1. The skill changes no branch.
7. It marks the tag as seen in the state file, or snoozes it.

## Part 4: plans first, humans review plans, AI reviews code

Facts (code.claude.com/docs/en/settings-reference, claude-directory): `plansDirectory` is valid in a committed `.claude/settings.json`. A relative path resolves from the project root. A path outside the project root falls back to `~/.claude/plans`. The docs do not say how plan files are named, or whether the 30-day `cleanupPeriodDays` sweep also covers a custom plans folder.

Rule 2 (in `rules/bnaas-d1.md`): every change starts in plan mode. When Anurag or Taylor approves the plan in the session, the session runs `bnaas-plan-pr`, and code starts only after the humans approve that plan PR. The code PR links its plan file. AI reviews the code: `bnaas-reviewer` locally, and the review-pr skill in CI (task 21).

`plansDirectory: ".claude/plans"`: a session started in the workspace writes through the symlink into the setup worktree. A session started inside backbeat writes into that checkout directly.

Skill `bnaas-plan-pr`: it renames the plan file from Claude's random name to `YYYY-MM-DD-<topic>.md`. In the setup worktree, it cuts `improvement/<ticket>-plan-<topic>` from the epic and commits only that file. Then it asks before the push, opens a draft PR into the backbeat epic with the five-section description, titled "<ticket>: plan <topic>", and with Taylor and Nicolas Humbert as reviewers. At the end it puts the worktree back on the epic. A change to the plan after review is a new commit on the same PR, so `git log` shows every version and `git revert` undoes one.

The review model changes the kickoff ("humans review every PR") and the review allowances in tasks 102, 103 and 103a. The final merges into development still go through Bert-E with human approval.

## Local wiring (each person, not committed)

```
cd ~/capsule-corp/bnaas-d1-delivery
git -C backbeat worktree add --detach ../bnaas-team-setup origin/epic/S3C-10971-bnaas-d1
for d in rules agents skills hooks plans; do ln -s ../bnaas-team-setup/.claude/$d .claude/$d; done
ln -s ../bnaas-team-setup/.claude/settings.json .claude/settings.json
```
The personal `.claude/settings.local.json` stays where it is. These steps go in the PR description for Taylor.

## Order of work

1. Clone the repos (kickoff step 1, still pending), and report the head commits.
2. Task 3 creates the epics, with Arsenal from `development/8.6`. Its plan goes to Anurag before any push.
3. Write the files on `improvement/BB-916-shared-claude-setup`, cut from the backbeat epic, and verify them.
4. Ask Anurag before the push, and open the PR into the epic with the five-section description. Reviewers: Taylor and Nicolas Humbert.
5. Write the proposed changes to the breakdown for Anurag, and do not republish the page: Arsenal base 8.6 and Vault pins the Arsenal epic (kickoff and task 3 step 2b); one source plus symlinks (task 22a); no force push of an epic (task 93); Node 24 for the rig (task 11); humans review plans and AI reviews code (kickoff working method, tasks 102, 103 and 103a).
6. Raise the open question with the team: who moves Federation `development/10` onto CloudServer 9.5 and backbeat 9.6, and which S3C release ships D1.

## Verification

- Start `claude` in `bnaas-d1-delivery`. `/memory` lists `rules/bnaas-d1.md`, `/agents` lists `bnaas-reviewer`, `/advisor` shows Fable, and `/hooks` lists the release-watch hook on SessionStart and UserPromptSubmit. If a symlinked piece does not load, copy that piece instead and record the gap.
- Start the same checks inside `backbeat` on the branch, without symlinks.
- Release watch: delete the state file and start a session. The message lists the current tags as new. A second start within 6 hours prints nothing. With the network off, the session starts silently.
- Skill: run it on backbeat `9.6.0-preview.5` against the epic. The summary must show the queuePopulator, BackbeatConsumer and config.joi overlap, and no files change.
- Plans: enter plan mode in the workspace. The plan file appears in `bnaas-team-setup/.claude/plans/`, not in `~/.claude/plans`. If the symlink makes Claude Code fall back to the home folder, set `plansDirectory` in the workspace's `settings.local.json` to a real folder and let the skill copy from there. Run `bnaas-plan-pr` up to the push question, and make sure that the branch holds exactly one commit with one file.
- Reviewer: plant a ticket number in a code comment. The session delegates to `bnaas-reviewer`, and the reviewer flags the comment.

## Changes after approval (2026-10-14)

- Epic branches are cut from the development branch. Nothing is pushed to an epic, and an epic is never force pushed. Every change reaches an epic through a PR with at least two approvals: Taylor (`tmacro`) and Nicolas Humbert (`nicolas2bert`) review Anurag's PRs, and Anurag (`anurag4DSB`) and Nicolas review Taylor's PRs. This is Rule 3 in `rules/bnaas-d1.md`.
- The release watch notifies the developers and reports the effect on D1. When an epic needs to take in development, the team decides how at that time.
- CloudServer `development/9.6` was created on 2026-10-14, at the same commit as `development/9.5`. D1 uses `development/9.6` as the CloudServer base, because new features go to the newest development branch and Bert-E carries fixes from 9.5 forward. The release watch follows `^9\.6\.` tags for CloudServer.
- Every ticket branch, plan PRs included, is named `improvement/<ticket>-<message>` and cut from the epic branch. Bert-E does not accept the `plan/` prefix. The ticket comes from the repository's own Jira project, for example BB for backbeat, so plan PRs use a BB ticket.
- A bot protects the epic branches on GitHub. Each epic branch is created once, by one push from its pulled development branch.
- Rule 4 added: an Opus subagent verifies every piece of work against its plan and reports deviations first.
