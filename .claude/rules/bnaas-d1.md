# BNaaS D1 team rules

These rules apply to every Claude Code session on D1, for every developer. They live on the D1 epic branch of backbeat (`epic/*-bnaas-d1`) and are removed before the final merge into development.

## Rule 1: Fable advises, subagents do the side work

- The advisor is Fable (`advisorModel` in `.claude/settings.json`). Consult it before a design choice and before a non-trivial change. If you are stuck, consult it too. Fable never writes code and never runs as a subagent.
- The main session and every subagent use the default model, Opus. Do not set a subagent to Fable.
- Delegate side work to subagents, so that the main context stays on the task. Run independent subagents in parallel.
  - Analysis and search across the repositories: the built-in `Explore` agent.
  - Design of a change: the built-in `Plan` agent.
  - Review of a diff, and cross-cutting checks (contracts, flags, legacy code, security, conventions): the `bnaas-reviewer` agent, after every code change and before every commit or PR.
- Report what the subagents found. Do not paste their transcripts.

## Rule 2: plan first, humans review the plan, AI reviews the code

- Every change starts in plan mode. The plan goes to `.claude/plans/` (`plansDirectory`). The one exception is the draft PR that a skill opens for its own self-improvement: its description says what it fixes and why, and the reviewers judge that instead of a plan.
- When the developer approves the plan in the session, run the `bnaas-plan-pr` skill. It commits the plan and opens a plan PR into the backbeat epic. Write code only after the reviewers approve that plan PR.
- The code PR links its plan file. If the work moves away from the plan, update the plan on its PR first.
- AI reviews the code: `bnaas-reviewer` in the session, the review-pr skill on the branch before each push (Rule 7), and the same skill in CI.

## Rule 3: an epic changes only through a reviewed PR

- Never push to an epic branch (`epic/*`), and never force push one. The epic branches are created once, in task 3, by one push from the pulled development branch.
- Every change reaches the epic through a PR from a ticket branch, `improvement/<ticket>-<message>`, cut from the epic branch, with at least two approvals.
- The ticket is a Jira ticket from the repository's own project. Arsenal uses ARSN, vault uses VAULT, cloudserver uses CLDSRV, cloudserverclient uses CLDSRVCLT and backbeat uses BB. Federation uses S3C, ring uses RING, integration uses INTGR and s3utils uses S3UTILS. Plan PRs go to the backbeat epic, so they use a BB ticket.
- Reviewers: the other developer is mandatory. Taylor (`tmacro`) reviews Anurag's PRs, and Anurag (`anurag4DSB`) reviews Taylor's PRs. Find the author with `gh api user --jq .login`.
- For the second approval, request Nicolas Humbert (`nicolas2bert`), who volunteered. If he cannot review, request the Hopocalypse team (`scality/hopocalypse`) instead.
- When a new upstream release lands, the developers decide whether and how the epic takes it in.
- Ask the developer before any push and before opening a PR. The one exception is the push and the draft PR that a skill makes for its own self-improvement: a draft cannot merge without the two approvals.

## Rule 4: verify every piece of work against its plan

- At the end of each piece of work, a verification subagent checks the result against the approved plan: what matches, what deviated and why, and what is missing.
- The verifier reads the real state (git remotes, Jira, the tech spec doc, CI, files), not the session's own summary.
- The verifier runs on Opus (`model: opus`), never on Fable. Its effort follows the Anthropic effort guide:
  - `medium`, the Opus 5.5 default, for checks of files, records and doc text.
  - `high` for checks that reason about code behavior, run tests or span many systems.
  - Before you use `xhigh` or `low`, tell the developer.
- Report the verdict to the developer, deviations first, before calling the work done.

## Rule 5: Jira and tracking

Claude does the bookkeeping, so the developer never has to know where things are recorded.

- Every PR and every release has a Jira ticket. At the start of the work, create it through the Atlassian connector, in the repository's own project (Rule 3).
- Ticket fields:
  - Type Task. Parent: the D1 Jira epic, whose key is in the epic branch name. Assignee: the developer.
  - Priority Medium, Severity Medium, Development Type BUILD.
  - Sprint: the active sprint. Read it from an open D1 ticket (`parent = <epic key> AND sprint in openSprints()`).
  - Release notes: No, wherever the field exists (S3C, RING). Only the epic carries the release note.
  - Components: RING requires one, so use "S3C". For federation work in S3C, use "Federation".
- The description lists "Done when" and links the ticket's row in the Tickets tab.
- When work starts, move the ticket to In Progress. Log the time spent for the work done. Close the ticket only after its whole scope is done. In RING, Done needs Release notes set first.
- After a branch cut, add a Jira comment. It gives the branch link, the cut sha, the last release tag on that line and the GitHub compare link.
- Every ticket has a row in the Tickets tab of the tech spec doc: https://claude.ai/code/artifact/bfe390f9-d819-4a93-9050-ffd5eee1b80a. The row gives the ID, To do, Repository, Owner / Author, the ticket as a link, PR, Status and Sprint, the sprint in which its work started. When Claude creates a ticket, it adds or fills the row through the Claude Docs connector.
- After any change in Jira, the doc or GitHub, tell the developer what changed and where. Give the links to the ticket, the doc tab, the branch and the PR. Name the task they are working on.
- "What's next": run the `bnaas-whats-next` skill. It lists the PRs that wait for the developer's review, then the work they can pick up, ready or blocked by its dependencies, then what the other developer is working on. When the developer picks a row with no ticket that nobody else owns, it creates the ticket and fills the row, after one question.

## Rule 6: commits, tags and releases

- A commit message has a subject line, a blank line, then a body. The subject is `<ticket>: <what the commit does>`, at most 55 characters. Wrap the body at 72 characters. Add no AI attribution trailer.
- A PR title starts with the ticket: `<ticket>: <what the PR is for>`. A plan PR is `<ticket>: plan <topic>`.
- Ticket keys appear only in branch names, commit messages and PR titles. Never put them in code, code comments or PR comments. Write no em-dashes.
- A PR description has five `##` sections, each with one to three sentences: Intent, System impact, Preserved behavior, Intended change and Verification. The `bnaas-plan-pr` skill shows the exact headings.
- An epic tag is `<next version of the base line>-bnaas.<N>`, and N counts up from 1 in each repository. Release a tag from the epic head only for a consumer that needs it. The first tags come at setup, in task 3.
- Each release is one ticket: a PR that bumps `package.json` to the tag, then the repository's release workflow, run from the epic branch.
- Chain order:
  1. Arsenal releases its tag.
  2. Vault, CloudServer, cloudserverclient and backbeat pin the Arsenal tag and release their own tags.
  3. Federation pins their images in `group_vars/all`: Vault `<tag>`, CloudServer, scality-kms and backbeat `<tag>-federation`.
  4. One manual Integration run uses the artifacts link of the Federation PR build.
- Every bump and pin commit ends with four provenance lines. A pin commit also names the tag it pins and that repository's compare link.

  ```
  Epic branch: epic/S3C-10971-bnaas-d1
  Cut from: <base branch> at <short sha> on <date>
  Last release on that line: <tag>
  Changes since that release: https://github.com/scality/<repo>/compare/<tag>...<short sha>
  ```

- cloudserverclient can release from an epic only after the change to its release workflow, which publishes under the dist-tag `prerelease`.
- A CloudServer release also starts the scality-kms release, with the same version. scality-kms is part of CloudServer and needs no separate step.
- The merge decisions (M1 to M9) are in the Merge strategy tab of the tech spec doc.

## Rule 7: review locally before every push

- Before every push, run the CI review on the branch: `.claude/hooks/review-branch.sh <repository>`. It runs the `review-pr` skill of `scality-skills@scality-agent-hub`, the skill that the Code Review workflow runs on each PR. It posts nothing to GitHub. It takes 5 to 15 minutes, so run it in the background.
- Fix each finding, or tell the developer why it stays, in the same question that asks for the push.
- The `review-gate.sh` hook refuses a `git push` until a saved review exists for the exact commit being pushed. A new commit needs a new review.
- The hook runs in each session that loads this setup, in the workspace layout or in the backbeat clone. It reads the target repository from the push command, so it covers every D1 repository of that session. A session started inside another clone does not load it.
- The hook catches a forgotten review. It is not a security boundary, and a command written to evade it can get through.

## Adding a rule

Add the next rule as a new `## Rule N` section in this file, through a plan PR like any other change. Keep each rule short and testable.
