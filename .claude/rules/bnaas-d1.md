# BNaaS D1 team rules

Two developers build D1 in two time zones: Anurag in Paris and Taylor in San Francisco. The work never stops, so each session must be able to start from the shared state alone, with no chat and no meeting. These rules say where that state lives and how a session reads and moves it. They live on the backbeat epic branch, `epic/*-bnaas-d1`, and the final merge removes them.

## Rule 1: shared state, not shared memory

Four places hold the truth, and a session reads them before it does anything else:

- The Backlog tab of the tech spec doc: every D1 task with its area, repository, estimate and dependencies, as a table that people edit and scripts read. It was seeded from the breakdown.
- Jira: who does what, and whether it is done. A task is claimed when a D1 ticket for it exists and is assigned. A task is done when that ticket is Done.
- GitHub: the code and its review state. A PR exists for every change that reaches an epic.
- The journal: what moved and what each session decided or hit, as facts with no names. It lives in the journal doc only, named in `.claude/bnaas/journal.json`: the current state of each active branch, the pivots, and the log by day.

The tech spec doc (https://claude.ai/code/artifact/bfe390f9-d819-4a93-9050-ffd5eee1b80a) also holds the human view: the Tickets tab lists the claimed tasks, and the Progress tab charts the progress and the scope. Claude keeps them current, so that neither developer has to.

Session memory, plan files and chat history are not shared state. If something matters to the other developer, it goes into a ticket, a PR, the Backlog tab or the journal.

## Rule 2: the day starts with what's next, and the journal records as you go

- Start every session with `/bnaas-whats-next`. It first catches up the journal from the session before it, then reads the journal, the Backlog tab, Jira, GitHub and the local clones, and gives: what moved since your last run, the PRs that wait for your review, the tasks you can take (ready, blocked, or dependencies unknown), and what the other developer is on. It also re-renders the Tickets tab, the charts of the Progress tab and the journal doc.
- Reviews come first, because the other developer is asleep and blocked on them.
- Take a task by claiming it (Rule 3), never by starting the code.
- The journal records as you go. Hooks log commits, pushes, PRs, test and lint results, Jira ticket changes and doc writes, and at the end of each turn the Stop hook asks the session to append them to the Log of the journal doc. When a decision, a blocker or a surprise happens, run `.claude/hooks/journal note "<one sentence>"` from the folder the session started in (the path is relative to it) at that moment (`--decision` for a decision or a change of plan), never at the end of the day. Facts only, no names. The other developer's next run of what's next reads it.
- While the developer waits on CI, a review or a subagent, the agent ends its waiting message with one line: "Meanwhile you could: <the top next action and why>; say go, or run /bnaas-whats-next for the full picture." The action comes from the last analysis, refreshed only when local work or the PRs changed or the last run is older than 30 minutes.
- What's next starts the watch-flock dog (`/bnaas-watch-flock`), and every PR the session opens gets a shepherd (`/bnaas-shepherd-prs`), which takes it to merged. A PR that the dog finds with no shepherd gets one.
- When a session gets long, use Matt Pocock's `/handoff` to carry the context into a fresh session.

## Rule 3: claim before you build

- A task is claimed by its Jira ticket: Task, parent the D1 epic (its key is in the epic branch name), assigned to you, in the current sprint, moved to In Progress. `/bnaas-whats-next` creates it after one question, and fills the row in the Tickets tab.
- One task, one owner. Never claim a task that has a ticket assigned to someone else, and never claim a task whose dependencies are not done unless you also own them. If the backlog says "dependencies unknown", read the breakdown comment (https://claude.ai/artifact/QC6Cbrfm7a5hAfRw752c7L, frozen v55) and decide.
- A dependency or scope fix is an edit of the Backlog tab, with no PR: fix the Depends on cell, or add a row with Origin "new scope". Re-seeding the tab from the breakdown is a deliberate step (`.claude/bnaas/README.md`).
- Before you claim, read Jira again. The other developer may have claimed it an hour ago.
- Hand a task over by reassigning its ticket and noting it with `journal note`.
- Jira is read and written only through the Atlassian MCP connector, never through its REST API.
- Claude does the Jira bookkeeping: the ticket fields (Priority Medium, Severity Medium, Development Type BUILD, Release notes No where the field exists, component S3C in RING and Federation in S3C), the "Done when" list and the row link in the description, the worklog, and the Done transition when the whole scope is done. Each repository uses its own project: ARSN, VAULT, CLDSRV, CLDSRVCLT, BB, S3C (Federation), RING, INTGR, S3UTILS.

## Rule 4: where the code goes

- Vault, CloudServer and backbeat: the epic branch `epic/S3C-10971-bnaas-d1`. Nothing is released from it. Federation deploys the CI images of its commits (Rule 6).
- Arsenal and cloudserverclient: the development branches. Their D1 changes are additive (new IAM actions and errors, new client APIs), so they ship in normal releases from their development branches, and the services pin the released npm versions. The breakdown rows that still mention epic prereleases are superseded by the task titles in the Backlog tab.
- Federation, Integration and RING: their epic branches, for the deployment role, the end-to-end runs and the alert rules.
- Never push to an epic branch, and never force push one. Every change reaches an epic or a development branch through a PR from a ticket branch, `improvement/<ticket>-<message>`, cut from that branch, with two approvals from distinct humans; if the other developer is not one of them, their post-merge read is journaled. Every PR gets three reviewers: the other developer (Taylor `tmacro` for Anurag's PRs, Anurag `anurag4DSB` for Taylor's; find the author with `gh api user --jq .login`), Nicolas Humbert (`nicolas2bert`) and the Hopocalypse team (`scality/hopocalypse`). A draft opens with no reviewers; they are requested when the developer marks the PR ready, by the shepherd when it sees the draft turn ready, so a draft never lands on anyone as review work. Pin PRs (Rule 6), the bot's or a developer's, are the one exception: no shepherd; a pin PR needs its `build-tests` green and one approval from either developer (the other developer when a developer opened it), the watch-flock dog reports an open pin PR, and the first developer to see it marks it ready, reviews it and merges it.
- Ask the developer before the first push of a branch and before opening a PR. Pushing folded review fixes to your own PR, with lease, needs no question. The one exception is the push and the draft PR that a skill makes for its own self-improvement: a draft cannot merge without the approvals.
- Work in one git worktree per ticket, at `<repo>/.claude/worktrees/<ticket>`. Add `.claude/worktrees/` to `.git/info/exclude` in clones that do not ignore it.
- Every epic-only change adds its row to the Remove before merge tab, in the same PR, except changes inside a folder that already has its row: backbeat's `.claude/` and each repository's `.claude/plans/` are one row each.

## Rule 5: plan in the PR, review by AI then by humans

- A change starts in plan mode. The approved plan goes to `.claude/plans/` and ships as the first commit of the code PR, so the reviewer reads the plan and the code together and nobody waits a day for a plan approval across time zones. Plan mode writes the plan, untracked, into the `.claude/plans/` of the folder the session started in: the setup checkout in the workspace layout, the backbeat clone in the one-repository layout. Move it (not copy) into `<repo>/.claude/worktrees/<ticket>/.claude/plans/` before the plan commit, so that the next update of the setup does not stop on it. In Arsenal and cloudserverclient, paste it into the PR description and delete the file instead.
- A plan PR comes first only when a change alters the shared contract beyond what the approved tech spec says. A value the spec leaves open (a method key, a monitoring name, a list resource) is proposed in the plan commit, marked provisional, and written into the tech spec before the PR leaves draft; the tech spec is the one place that names the glue between repositories. The shepherd (`/bnaas-shepherd-prs`) opens the plan PR, into the backbeat epic.
- In a repository on an epic branch, the plan file ships in the PR under `.claude/plans/`, and that folder is one Remove before merge row per repository. In Arsenal and cloudserverclient, which work on development branches, the plan goes into the PR description instead of a file.
- Before a commit, run the `bnaas-reviewer` agent on the diff. Then push, open the PR as a draft, and hand the PR to the shepherd, `/bnaas-shepherd-prs`. The Claude Code review on the PR is the review stage; there is no other local review.
- The shepherd runs the Claude Code review loop on the draft: it waits for the "Claude Code Review" check (`action_required` means comments to address, `success` is Claude Code's LGTM, `failure` or `cancelled` goes to the developer with the run link), fixes each comment and folds the fix into the commit it belongs to, or replies why it stays, and resolves only the threads that Claude Code opened, never a human's. On a stack, it rebases every PR above a fold, and treats a CONFLICTING PR like a red check. If the review check stays `action_required` only on comments that the shepherd answered with why they stay, the AI review is done: it tells the developer, with the links to those threads, and does not wait for the LGTM. When the review, the checks and the merge state are all green, it opens the PR in the browser and tells the developer that it is ready for their check. It never marks a PR ready and never merges: the developer marks it ready, and a human merges it after two approvals from distinct humans.
- On a ready PR, the shepherd requests the reviewers (Rule 4), fixes the mechanical human comments (a typo, a lint error, a missing link) and answers them, and asks the developer one question about the others, each with a diagnosis, a proposed fix and why it was missed. After a merge it retargets the next PR of the stack to the branch the merged PR targeted (the epic, or the development branch in Arsenal and cloudserverclient) and rebases it. Each step is a `journal note`.
- Delegate side work to subagents: `Explore` for searches, `Plan` for designs, `bnaas-reviewer` for reviews. Report what they found, never paste their transcripts. The advisor is Fable: consult it before a design choice, before a non-trivial change and when stuck. It advises, never writes code and never runs as a subagent.
- Use the installed skills instead of ad hoc process: `brainstorming` before new work, `test-driven-development` for code, `systematic-debugging` for bugs, `verification-before-completion` before you say that something works, `grill-with-docs` for the implementation specs, `retro` at the end of a long session.

## Rule 6: Federation follows the epics by commit

- Federation pins the images that CI builds for every epic commit: `cloudserver:<sha>-federation`, `backbeat:<sha>-federation` and `vault:<sha>`, where `<sha>` is the full head of each service epic. The three keys in `group_vars/all` are `cloudserver_image_version`, `defaults.vault.og_docker_image_version` and `defaults.backbeat.og_docker_image_version`. scality-kms (`defaults.kms.og_docker_image_version`) stays pinned to a released version such as `9.4.4-federation`, because no scality-kms image exists per commit and D1 does not change it.
- `/bnaas-pin-federation` updates the pins: it reads the three epic heads, checks that the images exist, edits `group_vars/all` on the branch `improvement/<ticket>-epic-image-pins`, and opens or updates one draft PR into the Federation epic. When the CI pin PR (branch `improvement/<ticket>-epic-image-pins-bot`) is open, review and merge it instead of opening a second one: CI and developers never share a branch. `/bnaas-whats-next` runs it in dry run and says when the pins are behind. The first developer to see an open pin PR marks it ready, reviews it and merges it once its `build-tests` pass, with one approval from either developer (the other developer when a developer opened it; Rule 4).
- The same script runs in Federation CI as the `bnaas-pins` workflow, from the epic, on its own branch `improvement/<ticket>-epic-image-pins-bot`. Its `setup_ref` input pins the script to a backbeat epic commit: `gh workflow run bnaas-pins.yaml --repo scality/Federation --ref epic/S3C-10971-bnaas-d1 -f setup_ref=$(git ls-remote https://github.com/scality/backbeat refs/heads/epic/S3C-10971-bnaas-d1 | cut -f1)`. A schedule does not fire from an epic branch, so a person or what's next starts it after an epic moves. Once the `notify-federation` job lands (plan, "To apply outside this PR"), each service epic's CI starts it after a push.
- One manual Integration run uses the artifacts link of the Federation PR build.
- No tags, no GitHub releases and no npm publish from any epic. A production release goes through the official workflow of its repository, from its development branch.

## Rule 7: commits, PRs and words

- One PR is one logical change. Before any push, the branch tells a commit story: a plan commit when there is a plan, then tests, then code, each commit one step a reviewer can read on its own; review fixes are folded into the commit they fix, never added as fix-the-fix commits. Work in one git worktree per ticket (Rule 4), so that a session never pushes a mix of two changes.
- A commit message has a subject line, a blank line, then a body. The subject is `<ticket>: <what the commit does>`, at most 55 characters. Wrap the body at 72 characters. Add no AI attribution trailer.
- A PR title starts with the ticket: `<ticket>: <what the PR is for>`. A plan PR is `<ticket>: plan <topic>`.
- A PR description has five sections, each a `##` heading followed by one to three sentences: `## Intent: why does this change exist?`, `## System impact: what's affected, including downstream?`, `## Preserved behavior: what explicitly stays the same?`, `## Intended change: what's different after this PR?` and `## Verification: how do we know this worked, or how would we know if it didn't?`. It links the task row in the Tickets tab. In Arsenal and cloudserverclient, the plan follows the five sections as a `## Plan` section in a collapsed `<details>` block (Rule 5).
- Ticket keys appear only in branch names, commit messages and PR titles. Never put them in code, code comments, skill text or PR comments. Write no em-dashes.
- After any change in Jira, the doc or GitHub, tell the developer what changed and where, with the links. After a batch of PRs, give a short Slack message that lists them in reading order.

## Rule 8: verify against the plan

- At the end of each piece of work, a verification subagent on Opus checks the result against the plan: what matches, what deviated and why, and what is missing. It reads the real state (git remotes, Jira, the doc, CI), not the session's summary. Report deviations first.
- A skill that finds its own instructions wrong fixes them in a worktree of its own, on the standing self-improvement ticket, and opens a draft PR into the backbeat epic. Find the standing self-improvement ticket in Jira with the search `parent = <epic key> AND summary ~ "self-improvements of the D1 skills"`; if there is none, stop and say that the ticket is missing. It never edits the shared setup checkout in place.
- Every rule and every skill in this setup is self-learning: a skill ends with a Self-improvement section, and a session that finds a rule wrong or missing proposes the change the same way, as a draft PR into the backbeat epic on the standing ticket.

## Adding a rule

Add the next rule as a new `## Rule N` section in this file, through a PR into the backbeat epic. Keep each rule short and testable.
