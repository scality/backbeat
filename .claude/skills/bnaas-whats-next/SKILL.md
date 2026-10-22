---
name: bnaas-whats-next
description: Tell a D1 developer what to do next. It lists the PRs that wait for their review, then the work they can pick up (their own open PRs and rows, and the rows with no owner, each marked ready or blocked by its dependencies), then what the other developer is working on. Use it when the developer asks "what's next", "what should I work on", "what is left", "what do I need to review", "what is waiting on me", or asks for the state of the D1 work in other words. When they pick a row with no ticket, it creates the Jira ticket in the current sprint, assigns it to them and fills the row, after one question.
---

# What to do next on D1

Rule 5 says that Claude does the bookkeeping, so that the developer never has to know where things are recorded. This skill answers from three sources, in one short report:

- The Tickets tab of the tech spec doc: the epic work, in work order, with the owner of each row.
- Jira: the status of each ticket, and the current sprint.
- GitHub: the review state of each D1 PR.

Reading changes nothing. Creating a ticket and editing the doc happen only after one question to the developer.

In this file, `<base>` is the base directory of this skill. Claude Code prints it when it loads the skill.

## 1. Find who is asking

Run `gh api user --jq .login`.

| GitHub login | Name in the doc | Jira name | Other developer |
|---|---|---|---|
| `anurag4DSB` | Anurag | Anurag Mittal | `tmacro` |
| `tmacro` | Taylor | Taylor McKinnon | `anurag4DSB` |

For Jira, call `atlassianUserInfo` of the Atlassian connector. Each developer's connector logs in as that developer, so it gives the account to assign. If its name is not the Jira name of the table, look the account up by that name with `lookupJiraAccountId`, and say so. Anyone who is not in the table gets the report only.

Find the epic: `git -C <base> branch -r --list 'origin/epic/*-bnaas-d1'`. The D1 Jira epic key is the part between `epic/` and `-bnaas-d1`.

## 2. Read the state

Read the Tickets tab first, because the other reads need its repositories and PR links. Then run the other three reads in parallel. None of them changes anything.

1. The Tickets tab, through the Claude Docs connector. Load the docs skill or the connector guide first, as the connector asks. The doc is https://claude.ai/code/artifact/bfe390f9-d819-4a93-9050-ffd5eee1b80a. Read the doc to list its tabs, then read the body of the "Tickets / Tasks" tab. Find the columns of the "Epic work" table by the names in its header row, because columns get added. Today they are ID, To do, Repository, Owner / Author, Ticket, PR, Status and Sprint, and a Depends on column is planned:
   - Owner / Author names the developer who owns the row and writes its PR. It is a dropdown. A row has no owner when the dropdown has no `index`, or when it says "Not assigned".
   - Status uses the words that the line under the table lists.
   - Sprint is the sprint in which the work on the row started. It never changes after that.
   - Depends on lists the IDs of the rows that must be done first, separated by commas. An empty cell means no dependency.

   Keep this read. Before any write, section 4 compares the cells that it writes with this read. Note the full id of the table and the enum id of the Owner / Author dropdowns too.
2. Jira, two searches, with `maxResults` 100 each:
   - The open D1 tickets: `parent = <epic key> AND statusCategory != Done ORDER BY key`, with only the fields `summary`, `status`, `assignee` and `customfield_10004` (the sprint).
   - The done D1 tickets: `parent = <epic key> AND statusCategory = Done`, with only the field `summary`.

   Each D1 ticket has the epic as parent (Rule 5). A row ticket in the first answer is open, and one in the second is done. A row ticket in neither answer is "not found": Jira does not know it, or it is not under the epic. The filters and the short field lists keep the answers small enough to read.
3. GitHub, with the GitHub login of section 1, and one `--pr <link>` for each link of the PR column:

   ```
   python3 <base>/scripts/prs.py --login <login> <each repository of the Repository column> --pr <link> --pr <link>
   ```

   The script lists the open PRs into the epic in each repository, and the PRs stacked on them. It does not use `gh search prs`, because the search index misses PRs: it once missed a merged Arsenal PR of the epic. For each PR, it prints its status in the words of the tab, who must act next, and the ticket key of its branch or title. Add `--json` for the details. Pass `--login` even though it defaults to the gh user, so that the report is right for whoever the developer is.
4. The sprint that the dates give: `python3 <base>/scripts/sprint.py`.

## 3. Report

Keep it short: one line per item, each with its link. Use these sections, in this order. Write "none" in an empty section, and leave out the last one when it is empty.

1. **Waiting for your review**: the PRs of kind `review`. Say who wrote each one, whether it is a draft, and whether you are requested or only have not approved yet. They come first, because the other developer is blocked until you review (Rule 3). After them, list the other open PRs where you are requested (`asked` in the JSON), with who acts first, for example "requested, but Nicolas asked for changes first".
2. **What you can pick up next**: the work that you can move now, ready work first.
   - Your open PRs (kind `mine`): first the ones where you must act, then one line each for the ones that wait on someone else, with who.
   - Your rows that are not done: their ticket is open or not found in Jira, or they have no ticket yet. Give the ID, the To do in a few words, the Jira status with the ticket link, and what is left from the Status cell. Jira decides whether a row is done, not the Status cell: a merged PR can still leave work, such as a tag to push.
   - The first three to five rows with no owner, in table order. The table order is the work order.

   Mark each row with its dependencies:
   - "ready": each row of its Depends on cell is done, that is, its ticket is in the done search. A row with an empty cell is ready.
   - "blocked": name each dependency that is not done, and who is on it: its Owner / Author, or "nobody yet". For example: "blocked: Anurag is on 28, the Arsenal tag that Vault needs".
   - "dependencies unknown": the table has no Depends on column yet, or the cell names an ID that the table does not have. Do not guess the dependencies from the To do text, because a wrong "ready" sends the developer to work that cannot start.
3. **What <the other developer> is working on**: their rows that are not done and their open PRs, with the status of each. Each developer then sees the other's work, and what a blocked row waits for.
4. **Out of sync**: one line for each place where two sources disagree.
   - GitHub and the tab: a PR or Status cell that the script contradicts, an Owner / Author cell that is not the author of the row's PR, or an open D1 PR that no row lists. Match each PR to its row by the ticket key that the script prints and the Ticket cell of the row: a row that says "No PR yet" can have an open PR.
   - Jira and the tab: an Owner / Author cell that is not the ticket's assignee, an open D1 ticket with no row, or a row ticket that is not found.
   - Jira and the sprint: an open D1 ticket that is not in the current sprint.
   - Jira and GitHub: a ticket still in review after its PR merged or closed. It may be ready to close, but only when its whole scope is done (Rule 5).

The script has three states that the Status words of the tab do not cover: "Approved, not merged yet", "Draft, no review requested yet" and "Waiting for a reviewer: nobody is requested". Report them, but do not count a Status cell as out of sync because of them, and never write them into the tab.

End with the doc link, name its "Tickets / Tasks" tab (the connector gives no link to a single tab), and ask which row the developer takes next.

## 4. When the developer picks a row

The developer can name a row in the first message ("I'll take 28"). Then run the reads of section 2, answer for that row first, and give the report of section 3 after it.

The Owner / Author of a row decides about its work. So a row that already has a ticket never gets another one, and a row that someone else owns never gets one from this skill, whoever runs it. The skill creates a ticket only for a row with no ticket, and it makes the developer the Owner / Author in the same step.

- The row has an Owner / Author who is not the developer: say who, and change nothing. That person decides. Say how a handover works: the owner clears the Owner / Author cell, or sets it to the developer, and the next run prepares the ticket.
- The row has a ticket assigned to someone else: say who, and change nothing.
- The row has a ticket with no assignee: if the Owner / Author cell is empty, offer to make the developer the Owner / Author and then to assign the ticket to them with `editJiraIssue`, in that order, as in steps 1 and 2 below. Otherwise, leave it.
- The row has a ticket assigned to the developer: check that it is in the current sprint and in the status Ongoing, because Rule 5 moves a ticket to In Progress when its work starts. Offer the missing changes.
- The row has no ticket ("Not created yet"), and no owner or the developer as owner: prepare the ticket below. But first look at the open D1 tickets with no row (section 3, Out of sync). Read each one with `getJiraIssue` and the field `description`, because only its "Tracked in ... row <ID>" line names the row. If one names this row, an earlier run created it and could not fill the cell: offer to fill the Ticket cell with it, and create nothing.

When there is a write to propose, ask one question with the AskUserQuestion tool. When there is none, for example because someone else owns the row, say so and ask nothing. The question lists every write: the ticket with its project, summary, assignee and sprint, the move to In Progress, the Owner / Author, Ticket and Sprint cells of the row, and the out-of-sync cells to fix, if any. The answers are "Do it" and "Nothing now". The developer approves all the writes at once, and nothing is half done. If you cannot ask, for example in a test run, stop after the report and change nothing.

### The ticket

| Field | Value |
|---|---|
| Project | The Jira project of the row's repository: ARSN for arsenal, VAULT for vault, CLDSRV for cloudserver, CLDSRVCLT for cloudserverclient, BB for backbeat, S3C for federation, RING for ring, INTGR for integration, S3UTILS for s3utils. |
| Issue type | Task |
| Parent | The D1 Jira epic. |
| Summary | `BNaaS D1: <the To do in a few words>`, like the other D1 tickets. |
| Assignee | The Jira account of section 1. |
| Priority | Medium |
| Severity, `customfield_10800` | `{"id": "10859"}` (Medium) |
| Development Type, `customfield_13078` | `{"id": "12026"}` (BUILD) |
| Release notes, `customfield_12101` | `{"id": "12053"}` (No), only in S3C and RING. The other projects do not have the field, and only the epic carries the release note. |
| Components | `[{"name": "Federation"}]` in S3C. `[{"name": "S3C"}]` in RING, which requires one. None in the other projects. |
| Sprint, `customfield_10004` | The id of the current sprint, as a number. |
| Description | What the row asks. Then "Done when:" and a numbered list of results that a reviewer can check. Then the line "Tracked in the Tickets tab of the BNaaS D1 tech spec, row <ID>: <doc link>". If the scope is not clear from the row and the tech spec, ask the developer. |

Find the current sprint in Jira, because the sprint calendar can move:

1. Search `parent = <epic key> AND sprint in openSprints()`, with `maxResults` 10 and only the field `customfield_10004`. Put the values of that field together into one JSON array of sprints, and pass it on stdin, through a quoted heredoc, because a sprint name or goal can hold an apostrophe:

   ```
   python3 <base>/scripts/sprint.py --jira - <<'EOF'
   <the JSON array>
   EOF
   ```

   The script keeps only the active sprints of the team board, because `openSprints()` also matches future sprints and a ticket can also be in a sprint of another board. It says whether the name matches the dates.
2. If the script finds no active sprint, for example because the D1 tickets are only in a future sprint, search `sprint = "<the name that sprint.py prints>"` in the same way.
3. If Jira still gives no active sprint, ask the developer. Do not create a sprint.

When the name of the Jira sprint does not match the dates, Jira wins. Use its sprint, and tell the developer, so that the next setup PR moves the anchor date in `sprint.py`.

### After the yes

1. Read the whole table again with `{"kind": "view", "parentId": "<table id>"}`, because the other developer may have changed it since section 2. Compare each cell that you are about to write with the read of section 2: the Owner / Author, Ticket and Sprint cells of the row, and each out-of-sync cell. If one of them changed, for example because the other developer took the row, stop before any write, and show the developer the new state. Step 2 uses the `rev` of this read as `ifRev`, and counts rows and columns in this read.
2. Claim the row: set its Owner / Author cell to the developer, in an `update` of its own. Do it before the ticket exists. If the connector refuses it, nothing is written yet, and no ticket is left without its row.
3. Create the ticket with `createJiraIssue`.
4. Read the transitions of the ticket, and use the one named "In Progress" (id 21 in backbeat today, but the id can differ in another project). The ticket then has the status "Ongoing".
5. Read the whole table again, because step 2 changed it. Check that the row is still yours and still says "Not created yet", and that each out-of-sync cell is as section 2 read it. Then fill the Ticket cell, the Sprint cell and the approved out-of-sync cells in one `update`, with the `rev` of this read as `ifRev`. If the connector refuses it, read and try once more, and leave out each out-of-sync cell that changed in between. If the Ticket cell still cannot be written, give the developer the ticket key and the row, and say that the cell needs it: the next run then finds the ticket as an open D1 ticket with no row.
6. Report what changed and where (Rule 5): the ticket link, the doc link with the name of its "Tickets / Tasks" tab, and the row. Then give the next steps: the work starts in plan mode (Rule 2), and the ticket branch is `improvement/<key>-<message>`, cut from the epic of that repository (Rule 3).

Each `update` is a prose `update` on the tab body, with one cell replacement per cell. `<row>` is the place of the row in the read just before the update (step 1 for step 2, step 5 for step 5), counted from 0 for the header row. `<rev>` is the `rev` of that read. `<owner col>`, `<ticket col>` and `<sprint col>` are the places of those columns in the header row of that read, counted from 0. Today they are 3, 4 and 7, but a new column such as Depends on moves them.

The claim, step 2:

```
{"ops": [
  {"op": "replace", "target": {"kind": "cell", "table": "<table id>", "row": <row>, "col": <owner col>}, "ifRev": <rev>,
   "with": {"as": "markdown", "blocks": {"o": {"type": "dropdown", "enum": "<enum id>", "index": <option>}},
            "from": {"kind": "inline", "content": "<?claude block o?>"}}}
]}
```

The other cells, step 5:

```
{"ops": [
  {"op": "replace", "target": {"kind": "cell", "table": "<table id>", "row": <row>, "col": <ticket col>}, "ifRev": <rev>,
   "with": {"as": "markdown",
            "from": {"kind": "inline", "content": "[<key>](https://scality.atlassian.net/browse/<key>)"}}},
  {"op": "replace", "target": {"kind": "cell", "table": "<table id>", "row": <row>, "col": <sprint col>}, "ifRev": <rev>,
   "with": {"as": "markdown", "from": {"kind": "inline", "content": "<current sprint name>"}}}
]}
```

- `<option>` is the place of the developer's name in the Owner / Author enum. Read the enum to find it.
- Write the Sprint cell only when it is empty. Sprint records when the work started, so never change a Sprint cell that has a value.
- Use full ids, as the connector guide explains. The same replacement fixes an out-of-sync PR or Status cell.
- If the connector refuses a write, follow its guide for that refusal. Never force a write over a cell that someone else changed.

## If a source does not answer

- Jira: an Atlassian connector can time out. If the session has a second Atlassian connector, try it. Without Jira, report from the doc and GitHub, say that the Jira statuses are missing, and create nothing.
- The doc: report from Jira and GitHub, and create nothing, because a new ticket needs its row.
- GitHub: `prs.py` prints an "Error:" line for each repository that it could not read. Name those repositories in the report, because their PRs are missing.

## Self-improvement

This skill is a living document. At the end of every run, before you finish, ask two questions. Did any command, path or assumption in this skill or its scripts turn out wrong or incomplete? Did the run need a step or a check that this skill does not mention? If so, fix the section of this SKILL.md, or the script, where it belongs, so that the next run benefits. Keep it lean. Put each correction into its existing section, not into a changelog or a list of workarounds, and delete guidance that proved useless.

Never edit the files of the shared setup checkout in place. That checkout is what every session loads, and an uncommitted edit there blocks the next setup update. The fix goes to the epic through a push and a draft PR that you make yourself, without asking first (Rules 2 and 3 make this one exception): a draft cannot merge without two approvals, so the reviewers still decide. In a test run, say what you would change, and change nothing.

1. Take the key of the standing BB ticket for the fixes of this skill from the Ticket cell of row 22d in the Tickets tab, which section 2 has read. The key stays out of this file (Rule 6). If row 22d has no ticket, stop before any edit, and tell the developer what you would change and that row 22d needs its ticket.
2. Find the setup checkout and the epic ref as `bnaas-plan-pr` does. Pick the worktree path once with `echo "${TMPDIR:-/tmp}/whats-next-<topic>"`, and write it in full in every later command, because the push gate refuses a path with a shell variable (Rule 7). Then run `git -C <checkout> fetch origin` and `git -C <checkout> worktree add -b improvement/<ticket>-<topic> <worktree> <epic ref>`.
3. Make the fix in that worktree, and commit it (Rule 6).
4. Run `.claude/hooks/review-branch.sh <worktree>` in the background (Rule 7). It takes 5 to 15 minutes. Fold a fix for each finding into the commit. A folded fix makes a new commit, and the push gate wants a review of the exact commit that you push, so run `review-branch.sh` again after each fold.
5. Tell the developer the findings and what you did with each, then push with `git -C <worktree> push -u origin improvement/<ticket>-<topic>`. Open a draft PR into the epic branch. The title is `<ticket>: <what the fix does>`. The body has the five sections that `bnaas-plan-pr` shows, and the findings that stay. Request the reviewers of Rule 3: the other developer, and `nicolas2bert` or the team `scality/hopocalypse`.
6. Remove the worktree, and give the developer the PR link. The fix reaches their setup when the PR merges and they update the setup (README, "Update the setup").
