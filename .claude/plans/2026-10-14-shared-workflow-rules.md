# Shared workflow: rules for tracking and releases, "where things live", and a ticket for "What's next"

## Context

Anurag and Taylor must run the same D1 workflow, and neither should need to know where to record things. Today, part of that workflow lives only in Anurag's private memory and in the tech spec doc: the Jira conventions, the Tickets tab, the release chain, the commit provenance lines and the PR format. Taylor's sessions would not know any of it. Claude must do the bookkeeping itself, and then tell the developer what it changed and where.

Anurag also wants a "What's next" command. It would show which task to work on next and which PRs wait for the developer's review, with PR review columns in the Tickets tab that Claude keeps up to date.

## Decision: split by size

The setup PR (BB-916, 11 files, verified) gets the conventions only: two short rules and a "Where things live" README section. That is about 60 lines of text, and it is what makes Taylor's sessions behave like Anurag's.

Full "What's next" automation is a feature of its own. It needs a skill that reads the Tickets tab, Jira and GitHub, new PR and review columns in the doc, and edit access for Taylor. It would make BB-916 much longer and reopen a verified PR, so it gets its own ticket and row now and is built later. Until then, a one-line rule gives the simple version: the Tickets tab link, plus the open D1 PRs that wait for your review.

## Step 1: Rule 5 "Jira and tracking" in `backbeat/.claude/rules/bnaas-d1.md`

- Every PR and every release has a Jira ticket, created through the Atlassian connector when its work starts. The project is the repository's own (Rule 3).
- Fields: Task; parent is the D1 Jira epic (the key in the epic branch name); assignee is the developer; priority Medium; Severity Medium; Development Type BUILD; Sprint is the active sprint, read from an open D1 ticket. Release notes is No on child tickets where the field exists (S3C, RING); only the epic carries the release note. Components are required in RING ("S3C") and set to "Federation" in S3C for federation work.
- The description lists "Done when" and links the ticket's row in the Tickets tab.
- Move the ticket to In Progress when work starts, and log time spent for the work done. The ticket closes only when its whole scope is done. In RING, Done needs Release notes set first.
- After a branch is cut, add a Jira comment with the branch link, the cut sha, the last release tag on that line, and the GitHub compare link.
- Every ticket has a row in the Tickets tab of the tech spec doc: ID, To do, Repository, Owner, and the ticket as a link. Claude adds or fills the row when it creates the ticket.
- After any change in Jira, the doc or GitHub, tell the developer what changed and where, with links (ticket, doc tab, branch, PR), and name the task they are on.
- "What's next" for now: give the Tickets tab link, the rows the developer owns that are not done, and the open D1 PRs that wait for their review: `gh search prs --review-requested=@me --state=open --owner=scality --base epic/S3C-10971-bnaas-d1`.

## Step 2: Rule 6 "Commits, tags and releases" in the same file

- Commit message: `<ticket>: <subject>`, with no AI attribution trailer. Ticket keys appear only in branch names and commit messages, never in code, code comments or PR comments. No em-dashes.
- PR description: the five `##` sections (Intent, System impact, Preserved behavior, Intended change, Verification), one to three sentences each.
- Epic tag: `<next version of the base line>-bnaas.<N>`, N counting from 1 in each repository. It is released from the epic head only when a consumer needs it, and the first tags come at setup. Each release is a ticket, a PR that bumps `package.json`, then the repository's release workflow run from the epic branch.
- Chain order: Arsenal, then Vault, CloudServer, cloudserverclient and backbeat, then the Federation pins in `group_vars/all`, then one manual Integration run with the artifacts link of the Federation PR build.
- Every bump and pin commit carries four provenance lines: epic branch, cut from (base, sha, date), last release on that line, and the compare link.
- Special cases: cloudserverclient releases from an epic only after its workflow change (dist-tag `prerelease`); a CloudServer release also starts the scality-kms release, with the same version. Decisions M1 to M9 are in the Merge strategy tab.

## Step 3: README "Where things live"

- The tech spec doc and its three tabs: main tab (contracts), Merge strategy (decisions M1 to M9), Tickets (all work, owners, tickets).
- The breakdown (frozen v55): https://claude.ai/artifact/QC6Cbrfm7a5hAfRw752c7L
- Jira: the D1 epic is the key in the epic branch name, and `parent = <that key>` lists its tickets.
- The eight epic branches.
- Access: both developers already have access to the doc. Each developer needs the Atlassian connector (or an Atlassian MCP server) in Claude Code. Without it, Claude cannot create or update Jira tickets for them. This also goes in "Before you start".

## Step 4: commit

Second commit on `improvement/BB-916-shared-claude-setup`: `BB-916: Add the tracking and release rules and where things live`. This plan is saved as `.claude/plans/2026-10-14-shared-workflow-rules.md` in the same commit.

## Step 5: ticket and row for "What's next"

- New BB Task, in the backlog: no sprint, status To Do, assignee Anurag, fields as Rule 5. Summary: "BNaaS D1: What's next skill and PR review tracking".
- Done when:
  - The Tickets tab has a PR column and a "Review needed from" column (Anurag, Taylor, Nicolas).
  - A `/bnaas-whats-next` skill reads the Tickets tab, Jira and GitHub, updates those columns, and tells the developer the next task and the PRs waiting for their review.
  - It works the same from Taylor's session.
- Tickets tab: new row "22b (new scope)", owner empty, with the ticket link. Row 22a gets Owner Anurag and its link to BB-916.

## Verification

- Rule 4, with the Opus verifier at `medium` effort (files, records and doc text):
  - Rules 5 and 6 and the README section match this plan, and they agree with Rules 1 to 4, both skills and the Merge strategy tab.
  - No ticket keys outside branch names and plans, no em-dashes, and the writing lint is clean.
  - The new ticket's fields, its backlog state, and rows 22a and 22b in the Tickets tab.
  - A fresh `claude -p` session still loads the rules, and answers "what's next" with the Tickets tab link.
- After that, ask Anurag before the push and the PR.
