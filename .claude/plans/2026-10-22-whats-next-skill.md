Superseded on 2026-10-22 by 2026-10-22-round-the-clock.md: the Tickets tab is now rendered, dependencies come from the backlog, and there is no local review gate.

# What's next skill

## Context

Rule 5 gives a simple "What's next" for now: the Tickets tab link, the developer's open rows, and a `gh search prs` for the PRs that wait for their review. Anurag wants one command that checks the remaining work, the current work and the open PRs, and that creates the Jira ticket for the next piece of work, assigned to whoever runs it and put in the current sprint.

Anurag asked to build it in a worktree first and to open the PR later, so this plan record comes with the code on the same branch.

Facts (read only, on 2026-10-22):
- The Tickets tab table has the columns ID, To do, Repository, Owner / Author, Ticket, PR, Status and Sprint. Owner / Author is a dropdown with Anurag, Taylor, Nicolas and Not assigned. The line under the table fixes the Status words. Sprint is the sprint in which the work on a row started, and it does not change later.
- `gh search prs --base epic/...` missed a merged Arsenal PR of the epic. `gh pr list -R scality/Arsenal --base epic/...` found it.
- The active sprint is "Hopocalypse 2026-10-19". Jira gives it in `customfield_10004`, with a start on Monday 07:00 UTC and an end on the Monday two weeks later. The team counts the sprint from Monday to the Friday of the second week.
- `openSprints()` also matches future sprints. The team sprints are on Jira board 339.

## Changes, on `improvement/BB-917-whats-next-skill`

1. `.claude/skills/bnaas-whats-next/SKILL.md`, triggered by "what's next", "what should I work on", "what is left", "what do I need to review" and similar.
   - It finds who runs it from `gh api user` and the Atlassian connector's own user.
   - It reads the Tickets tab, the open D1 tickets in Jira, the open D1 PRs, and the sprint.
   - It reports, in this order: the PRs that wait for the developer's review; the work they can pick up (their open PRs and rows, and the next rows with no owner), each row marked ready, blocked (by what, and who is on it) or dependencies unknown; what the other developer is working on; and what is out of sync between the tab, Jira and GitHub.
   - It reads the dependencies from a Depends on column of the tab (row IDs, comma separated). Until that column exists, it says "dependencies unknown" instead of guessing.
   - The Owner / Author of a row decides. The skill creates a ticket only for a row with no ticket, never for a row that already has a ticket or that someone else owns, whoever runs it.
   - When the developer picks such a row, it asks once. Then it reads the table again and stops if a cell it would write changed. It claims the Owner / Author cell first, creates the ticket with the Rule 5 fields in the current sprint, moves it to In Progress (status Ongoing), and fills the Ticket and Sprint cells. It never changes a Sprint cell that has a value.
2. `scripts/prs.py`: lists the open PRs into the epic with `gh pr list` in each repository of the tab, follows stacked PRs, and gives each PR its status in the words of the tab, who must act next, and whether it waits for the caller's review. It has a self-test.
3. `scripts/sprint.py`: the sprint of a date, from an anchor Monday in two-week steps, and the active team sprint of Jira sprint fields. Jira wins when the two disagree. It has a self-test.
4. A Self-improvement section. After a run that found a wrong or missing step, the skill fixes itself through a draft PR into the backbeat epic, which it opens without asking, under one standing BB ticket, which it reads from the Ticket cell of row 22d in the Tickets tab, so that the key stays out of the skill (Rule 6). It makes the edit in a worktree of its own, never in the shared setup checkout. Rule 3 gets this one exception to "ask before a push or a PR", and Rule 2 to "plan first", because a draft cannot merge without two approvals.
5. Test cases in `evals/`, in the skill-creator format: "what's next" for Anurag and for Taylor, "what do I need to review", and picking a row that someone else owns, with a ticket and without one, which must create no ticket. Each is a read-only test run.
6. Rule 5's "What's next" line points at the skill, and its Tickets tab line names the current columns. The README lists the skill.

Scope: in Jira and the doc, the skill writes only after one question, and only the new ticket and the cells of the picked row, plus the out-of-sync cells that the developer approves. Its only push and PR are the self-improvement draft PR of item 4. It never creates a sprint.

## Verification

- `python3 scripts/prs.py --self-test` and `python3 scripts/sprint.py --self-test` pass.
- The test cases run with the skill and without it, and the skill-creator benchmark compares them.
- A read-only dry run for Anurag gives the report with no write in Jira or the doc.
- The `bnaas-reviewer` agent and the Rule 7 review run on the branch before the push.
