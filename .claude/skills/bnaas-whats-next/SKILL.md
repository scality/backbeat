---
name: bnaas-whats-next
description: Tell a D1 developer what to do next, and do it. It catches up the journal, reads the journal, the local clones, the Backlog tab, Jira and GitHub, ranks the next actions by critical path (reviews that unblock the other developer, local work to push, pins and Jira fixes, then the ready task that unblocks the most), asks which one to run, runs it and asks again. Use it when the developer asks "what's next", "what should I work on", "what is left", "what do I need to review", "what is waiting on me", or asks for the state of the D1 work in other words. When they start a task with no ticket, it creates the Jira ticket in the current sprint and assigns it to them, after one question.
---

# What to do next on D1

Rule 2 says that every session starts here, and Rule 3 says that Claude does the bookkeeping, so that the developer never has to know where things are recorded. This skill reads five sources, ranks the next actions of the developer by critical path, asks which one to run, runs it, and asks again:

- The journal: the facts that the hooks logged as work happened (commits, pushes, PRs, test results, Jira and doc writes) and the notes that sessions added, in the Log section of the journal doc named in `<setup>/.claude/bnaas/journal.json`.
- The local clones: the branches with commits that no remote holds yet.
- The Backlog tab of the tech spec doc: every D1 task with its area, estimate and dependencies. People edit it there; it was seeded from the breakdown.
- Jira: who claimed which task (the assignee of its ticket), the status of each ticket, and the current sprint.
- GitHub: the review state of each D1 PR, and whether the Federation pins are current.

The "Tickets / Tasks" tab of the doc is a rendered view of those sources, written by `render_tickets.py`, and so are the two charts of the Progress tab. It holds no truth of its own: never read ownership or status from its cells, and never edit a cell. The reads change nothing. The renders of section 3 need no question. Every other write happens only inside an action that the developer picked, and each Jira write of a claim takes one question of its own, except the one auto-fix of section 4.

In this file, `<base>` is the base directory of this skill. Claude Code prints it when it loads the skill. `<tmp>` is the session scratchpad. `journal` is `<setup>/.claude/hooks/journal`.

## 0. Catch up the journal, find who is asking, then delegate the reads

In the main session, before any delegation: write to the journal what this session decided or hit that the hooks could not see. Run `journal note "<one sentence>"` for each blocker or surprise, `journal note --decision "<one sentence>"` (with `--why "<text>"` and `--tasks <ids>` when known) for each decision or change of plan, then `journal flush`: it prints the facts that wait. Append them to the Log section of the journal doc through the Claude Docs connector, newest day first, under the day heading it prints, then run the `journal flushed` command it prints. If the write fails, leave them: the next Stop or run retries. No names, facts only. Skip it in a fresh session.

Then run section 1 in the main session: the login, the Jira account and the epic that it finds are needed here too, for the claim and the ranking. Section 2 runs in one Opus subagent (the `general-purpose` agent, default model), so that the main session stays small. Give it this file, the GitHub login, the Jira account and the epic, and ask for the paths of the files of reads 0 to 6 (`decisions.json` included) and the context table of section 3 back. Section 3 runs `next_actions.py` in the main session, and the questions, every Jira write, every doc write and `journal mark-read` stay there too.

## 1. Find who is asking

Run `gh api user --jq .login`.

| GitHub login | Name in the doc | Jira name | Other developer |
|---|---|---|---|
| `anurag4DSB` | Anurag | Anurag Mittal | `tmacro` |
| `tmacro` | Taylor | Taylor McKinnon | `anurag4DSB` |

For Jira, call `atlassianUserInfo` of the Atlassian connector. Each developer's connector logs in as that developer, so it gives the account to assign. If its name is not the Jira name of the table, look the account up by that name with `lookupJiraAccountId`, and say so. Anyone who is not in the table gets the context table only, with no action.

Find the epic: `git -C <base> branch -r --list 'origin/epic/*-bnaas-d1'`. The D1 Jira epic key is the part between `epic/` and `-bnaas-d1`. `<setup>` is the folder that holds `.claude/` of the shared setup, three levels above `<base>` (`<base>` is `<setup>/.claude/skills/bnaas-whats-next`). The D1 repositories are arsenal, vault, cloudserver, cloudserverclient, backbeat, federation, integration, ring and s3utils.

## 2. Read the state

Run reads 1 and 6 first, then read 0, which checks the journal against them, then reads 2 to 4 in parallel, because read 2 needs the PR links of read 0. Read 5 comes last, because it needs the files of reads 1 and 2. None of them changes anything.

0. The journal since the last run. `export` the journal doc once, with `format` markdown, decode its `bytes_b64` into `<tmp>/journal.md`, then run `journal tail --new --doc <tmp>/journal.md`. It prints the bullets of the Log section that `~/.bnaas/journal/last-read` has not seen yet (all of them on a first run), each under its `### <date>` heading, one bullet per fact, `<repo> <branch>: <fact>`: they are what moved. The state comes from the whole Log, not from that unread tail, because last-read is kept per machine: run `journal tail 1000 --doc <tmp>/journal.md`, group its facts by repository and branch, and derive for each active branch: the last commit, the last test or lint result, the PR (its link from a `PR create` fact), and the open notes and decisions. Then verify each against the real state: `gh pr view <link> --json state,isDraft,reviewDecision` for each PR, the Jira status in `<tmp>/jira.json` (read 1) for each Jira fact, and read 6 for each commit that should still be local. Where the journal and the real state disagree, the real state wins: note the correction. Write the verified state to `<tmp>/journal-state.json` (a list of `{repo, branch, last_commit, tests, pr, notes}`), and write the decision notes to `<tmp>/decisions.json` (a list of `{date, text, why, tasks}`) for the Pivots table of the journal doc render ([references/renders.md](references/renders.md)). If the doc cannot be exported, say so in one line and go on. In the same step, collect the PR links of the journal facts and of the "Tickets / Tasks" tab (one `view` read of the tab through the Claude Docs connector; load the docs skill or the connector guide first, as the connector asks; the doc is https://claude.ai/code/artifact/bfe390f9-d819-4a93-9050-ffd5eee1b80a): they keep merged PRs visible to read 2.
1. Jira, through the Atlassian connector: `searchJiraIssuesUsingJql` with the JQL `parent = <epic key> ORDER BY key`, the fields `summary`, `status`, `assignee`, `customfield_10004` (the sprint) and `description`, and `maxResults` 100; follow `nextPageToken` until the last page. Each D1 ticket has the epic as parent (Rule 3), so this answer holds every claimed task, open and done. Write `<tmp>/jira.json` yourself: a JSON list with one issue per line, each holding only `key` and those five `fields`, in the shape of `evals/fixtures/jira-sample.json` (`status.name`, `assignee.displayName`, the sprint list, the description as text).
2. GitHub, with the GitHub login of section 1, and one `--pr <link>` for each PR link of read 0:

   ```
   python3 -I <base>/scripts/prs.py --json --login <login> vault cloudserver backbeat federation integration ring s3utils --dev arsenal --dev cloudserverclient --pr <link> ... > <tmp>/prs.json
   ```

   The script lists the open PRs into the epic in each repository given by name. Arsenal and cloudserverclient take their D1 work on their development branches (Rule 4), so for them `--dev` lists the open PRs of the two developers into any `development/*` branch. It also follows the PRs stacked on all of those. It does not use `gh search prs`, because the search index misses PRs. For each PR it gives its status words, who must act next, its kind for `<login>` (review, mine, other, closed), whether it is a draft, and the ticket key of its branch or title. Pass `--login` even though it defaults to the gh user, so that the ranking is right for whoever the developer is.
3. The current sprint: Jira's own name first, `python3 -I <base>/scripts/sprint.py --jira -` fed the `customfield_10004` values of read 1, and the date math (`python3 -I <base>/scripts/sprint.py`) only when that exits 1. Jira wins over the dates, so a renamed sprint never turns every open ticket into an out-of-sync line.
4. The Federation pins, read-only: `git -C <the Federation clone> fetch -q origin && python3 -I <setup>/.claude/skills/bnaas-pin-federation/scripts/pins.py --dry-run --json --federation <the Federation clone> > <tmp>/pins.json`. The fetch matters: the script reads the pins from the local epic ref and the heads live from GitHub, so a stale clone says the pins are behind after a pin PR merged. It says for each service whether the pin is current, behind, or waiting for a CI image.
5. The rendered view. First get the backlog, from a cache when the tab did not change:
   - Read the Backlog tab with a cheap `outline` read through the connector, and note its `rev`.
   - If `~/.bnaas/backlog.rev` holds that `rev` and `~/.bnaas/backlog.md` exists, copy that file to `<tmp>/backlog.md`.
   - Else `export` the tab with `format` markdown, decode its `bytes_b64` and write it to `<tmp>/backlog.md`. If the export is not possible, read the tab and write its table yourself as Markdown, with the same columns, to that file: this is the one retype, and it happens only when the tab changed. Then copy the file to `~/.bnaas/backlog.md` and write the `rev` to `~/.bnaas/backlog.rev`.

   The tab may have a last column, Status: "done" there marks a task done with no ticket, such as the work that came before the Jira epic. Then:

   ```
   python3 -I <base>/scripts/render_tickets.py --backlog <tmp>/backlog.md --jira <tmp>/jira.json --prs <tmp>/prs.json --sprint "<current sprint>" --json > <tmp>/render.json
   ```

   It matches each ticket to its task by the "row <ID>" line of its description, else by the task id after "BNaaS D1: " in its summary. Each area has `id`, `title` and `rows`. For each area with a claimed or done task it gives rows with `id`, `task` (the title), `repos`, `estimate_days`, `owner` (the Jira assignee), `tickets`, `prs`, `status` (the Jira status), `depends_on` (each dependency marked done, open or unclaimed), and `dependencies`: ready, blocked or unknown. A task that nobody claimed has the owner "ready, nobody yet" or "dependencies unknown, nobody yet"; a blocked one is left out; one marked done in the Backlog tab has the owner "nobody, no ticket". A task with a dependency text that the backlog could not resolve is never ready. The `done` list holds the ids of the done tasks, and the `out_of_sync` list the mechanical mismatches, among them a ticket still open whose PR is merged or closed. `skipped` lists the PRs into a development branch whose ticket is not a D1 ticket: they are not D1 work, so leave them out. `progress` and `scope` feed the charts ([references/renders.md](references/renders.md)). A ticket for a step, such as row 3.1, counts for its task, 3. For the tasks in areas with no claimed task, run `python3 -I <setup>/.claude/bnaas/scripts/backlog.py --backlog <tmp>/backlog.md ready --done <the done ids, comma-separated>` (`--done ''` when there is none), and `unknown` with the same `--done` for the tasks with unknown dependencies.
6. The local work: `python3 -I <base>/scripts/local_work.py --json --workspace <the folder that holds the D1 clones> > <tmp>/local.json`. For each D1 clone and its worktrees, it lists the branches with commits that no remote branch holds: repository, worktree, branch, commits ahead and last subject. In the one-repository layout, pass the folder above the backbeat clone.

## 3. Rank the next actions, then ask

Take owners, statuses and dependencies from the JSON files, never from the cells of the tab. First render the three views, with no question, in this order: the "Tickets / Tasks" tab, the journal doc, then the two charts, because the scope chart counts the Pivots table that the journal doc render has just updated ([references/renders.md](references/renders.md)). They hold the full state, so the terminal does not. Then rank:

```
python3 -I <base>/scripts/next_actions.py --render <tmp>/render.json --local <tmp>/local.json --prs <tmp>/prs.json --backlog <tmp>/backlog.md --login <login> --pins <tmp>/pins.json > <tmp>/next.json
```

Pass `--pins` only when read 4 succeeded; an empty or broken pins file only leaves out the pins row.

It ranks by critical path, for this developer: (1) unblock the other developer, the PRs that wait for their review; (2) finish what they started, local commits to push and open as a PR, their PRs with changes requested or approved, their claimed tasks with nothing pushed; (3) keep the pipeline flowing, the Federation pins behind and the Jira mismatches; (4) start the next task, the ready task with no owner in their repositories that unblocks the most work downstream (transitive dependents, then their estimate days), admin and nice-to-have tasks last, and a piggyback when a task waits only on open PRs. Each action has `kind`, `title` (the plain name), `ref` (the task number, the PR or the branch), `does`, `why`, `unblocks`, `links` and `recommended`.

Write for a terminal, in this shape and nothing more:

1. One line with the two doc links: the tech spec (its "Tickets / Tasks" tab; the connector gives no link to a single tab) and the journal doc.
2. At most 12 lines of context, as one Markdown table, What | State | Link: what moved since the last run (read 0, with each correction), the PRs that wait on you, your local unpushed work, and one line for what the other developer is on. When read 0 holds a note "PR <link> merged without the other developer's review: read it" (or "mergeable without") for a PR that the developer did not write, add one line What "Review", State "<repo>: <title>, merged without your review: read it after the merge", with the link: it is a read, not a blocker. When `other_local` of `next.json` is not empty, add one line "Other local branches" that names them: they carry no D1 ticket, so they are never an action. Never show a bare task number: write "Arsenal: Add the bucketnotif IAM actions (task 25)". Name a PR by repository and title, never only "#7261". Leave the full list of ready tasks and the out-of-sync lines to the Tickets tab.
3. The two doc links again, then the question.

Ask one question with the AskUserQuestion tool. Its options are the top two to four actions of `next.json`, the first labelled "<title> (<ref>) (Recommended)", the others "<title> (<ref>)". Each description says in plain words what the action does and what it leads to, then why, then what it unblocks, as if the reader has forgotten the context: use `does`, `why` and `unblocks`. The question text names the first action by title and repository, and ends with the tech spec link and the sentence "The Tickets tab has the details." The developer can always type their own answer (the tool adds Other). From the second question on, keep at most three actions and add the option "Stop here" (the tool takes two to four options). If you cannot ask, for example in a test run, stop after the table and change nothing. A test run (the prompt says so) writes nothing live: it writes each render to `<tmp>` (the Tickets tab Markdown, the chart payloads, the journal doc tables) instead of the doc, and runs no `journal mark-read` and no In Review auto-fix, so that the developer's next real run still shows what moved.

## 4. Run the chosen action, then ask again

Run the action that the developer picked, then re-run reads 2 and 6 and `next_actions.py`, and ask the next question, until they pick "Stop here". After each action, say what changed and where, with the links (Rule 7).

- **review** a PR: open it with `sh <base>/scripts/open_url.sh <url>`. Run `bnaas-reviewer` in a subagent on its diff (`gh pr diff <url>`), and give its findings as a five-line brief. Then ask what to start while you read it.
- **push-pr**, push a local branch and open its PR: check the commit story first (a plan commit when there is a plan, then tests, then code; each subject `<ticket>: ...` and at most 55 characters; a ticket key in the branch name), and propose the fixes before any push. Push with `git -C <worktree> push -u origin <branch>`, open a draft PR into the epic or development branch with the five sections of Rule 7 and no reviewers (they are requested when the developer marks it ready, Rule 4), and open its link. **push** adds the commits to its open PR the same way, except when review fixes were folded: if `git merge-base --is-ancestor origin/<branch> <branch>` fails, say so and push with `--force-with-lease` (Rule 4 forbids force pushes only to epic branches). Then hand the PR to `/bnaas-shepherd-prs`, which runs the Claude Code review loop until the PR is ready for the developer's check.
- **fix-pr**: open the PR and list its review comments: the inline ones (`gh api repos/<repo>/pulls/<n>/comments`) and the review bodies (`gh pr view <url> --comments`). **merge**: into an epic, merge it with `gh pr merge <url> --merge` once its checks pass. Into a `development/*` branch (Arsenal, cloudserverclient), bert-e merges and runs the waterfall: never run `gh pr merge` there; post the `/approve` comment as the author (`gh pr comment <url> --body /approve`), and bert-e merges once its checks pass.
- **start** a task, or **continue** a claimed one: run the claim below. Then look for a worktree or branch of the ticket that already exists (`git -C <clone> worktree list --porcelain`, `git -C <clone> branch --list 'improvement/<key>-*'`) and reuse it: a claimed task with no commit yet, or a branch pushed with no PR, already has one. Only when there is none, `git -C <clone> worktree add -b improvement/<key>-<message> <clone>/.claude/worktrees/<key> <epic or development ref>`. Then enter plan mode (Rule 5).
- **piggyback**: the claim below, then the same worktree cut from the branch of the open PR that the task waits on, not from the epic. Across repositories (CloudServer on an unreleased Arsenal change), pin that PR's commit by git in `package.json`, and replace the pin by the released version before the PR leaves draft. Say both caveats in the option: a rebase when that PR changes, and the pin to replace.
- **pins**: run `/bnaas-pin-federation`. **jira-fix**: propose each fix of `out_of_sync`, one question per write.

### The claim

The Jira assignee of a task's ticket owns it and decides about its work. So a task that already has a ticket never gets another one, and a task that someone else owns never gets one from this skill, whoever runs it. The one exception is **next-step**: when every step ticket of a task is Done and the developer owns one of them, the skill asks whether to open the next step ticket (row `<task>.<n+1>`, same fields) or to mark the task done in the Backlog tab, because step tickets alone never complete a task.

- The ticket is assigned to someone else: say who, and change nothing. That person decides. A handover is a change of assignee in Jira, by the owner; the next run shows it.
- The ticket has no assignee: offer to assign it to the developer with `editJiraIssue`.
- The ticket is assigned to the developer: check that it is in the current sprint and in the status Ongoing, because Rule 3 moves a ticket to In Progress when its work starts. Offer the missing changes.
- The task has no ticket: prepare the ticket of [references/ticket-fields.md](references/ticket-fields.md), in the current sprint that it finds. Refuse to prepare one for a task with an open or unclaimed dependency unless the developer owns every open one or chose the piggyback, and say why. A ticket that matches no task (in `out_of_sync`) may be an earlier claim with a broken "row" line: read its description with `getJiraIssue`, and if it is for this task, offer to fix its description, and create nothing.

Every Jira write of the claim takes one question of its own with AskUserQuestion. It lists every write: the ticket with its project, summary, assignee and sprint, the move to In Progress, and any Jira fix. The answers are "Do it" and "Nothing now". After the yes:

1. Check the claim again: search the task's tickets in Jira as in read 1, limited to this task. If someone else claimed it since section 2, stop before any write, and show the developer the new state.
2. Create the ticket with `createJiraIssue`. Read its transitions, and use the one named "In Progress" (the id can differ between projects). The ticket then has the status "Ongoing". Make any other approved Jira fix.
3. Re-run reads 1 and 5, and render the Tickets tab and the charts again.

**Auto-fix, no question** (never in a test run). When a ticket assigned to the developer has the status Ongoing and a PR of it in read 5 is open and not a draft, move the ticket to "In Review": read its transitions with `getTransitionsForJiraIssue`, and use the one whose target is In Review. Then render the Tickets tab and the charts again. Report each move.

## If a source does not answer

- Jira: an Atlassian connector can time out. If the session has a second Atlassian connector, try it. Without Jira, rank from the doc and GitHub, say that the Jira statuses are missing, and create nothing. `next_actions.py` then offers only the reviews of epic PRs, the local work and the pins: with an empty export it cannot tell D1 work from the rest, nor claimed tasks from free ones.
- The journal: if `journal tail` fails, leave what moved out of the table, say why in one line, and do not run `journal mark-read`.
- The doc: rank from the backlog, Jira and GitHub. A ticket can still be created, because Jira holds the claim; say that the tab stays stale until the next run renders it.
- The Backlog tab: rank without the start actions, say that the Backlog tab could not be read, and mark every task "dependencies unknown". Leave the charts as they are.
- The Federation pins: if read 4 fails (no Federation clone, or `pins.py` not on the epic yet), run `next_actions.py` without `--pins`, and say in one line that the pins were not checked.
- GitHub: `prs.py --json` exits 1 when it could not read a repository or a PR link, and lists each one in the `errors` field of its output. Read that field, and name those repositories in the table, because their PRs are missing.

## Self-improvement

This skill is a living document. At the end of every run, before you finish, ask two questions. Did any command, path or assumption in this skill or its scripts turn out wrong or incomplete? Did the run need a step or a check that this skill does not mention? If so, fix the section of this SKILL.md, or the script, where it belongs, so that the next run benefits. Keep it lean. Put each correction into its existing section, not into a changelog or a list of workarounds, and delete guidance that proved useless.

Never edit the files of the shared setup checkout in place. That checkout is what every session loads, and an uncommitted edit there blocks the next setup update. The fix goes to the epic through a draft PR that you make yourself, without asking first: that push and that draft PR are the one exception of Rule 4 to asking before a push, because a draft cannot merge without two approvals, so the reviewers still decide (Rule 8). In a test run, say what you would change, and change nothing.

1. Find the standing self-improvement ticket in Jira with the search `parent = <epic key> AND summary ~ "self-improvements of the D1 skills"`. The key stays out of this file (Rule 7). If there is none, stop before any edit, and tell the developer what you would change and that the ticket is missing.
2. Find the setup checkout, the git checkout that `.claude/plans` really lives in (`cd "$CLAUDE_PROJECT_DIR/.claude/plans" && git rev-parse --show-toplevel`), and the epic ref, as `git -C <checkout> branch -r --list 'origin/epic/*-bnaas-d1'` prints it. Run `git -C <checkout> fetch origin` and `git -C <checkout> worktree add -b improvement/<ticket>-<topic> <checkout>/.claude/worktrees/<ticket>-<topic> <epic ref>`.
3. Make the fix in that worktree, run `bnaas-reviewer` on the diff, and commit it (Rules 5 and 7).
4. Push with `git -C <worktree> push -u origin improvement/<ticket>-<topic>`, and open a draft PR into the epic branch. The title is `<ticket>: <what the fix does>`. The body has the five sections of Rule 7. Request no reviewers: they are requested when the developer marks it ready (Rule 4). Hand it to `/bnaas-shepherd-prs`, which runs the Claude Code review loop (Rule 5).
5. Remove the worktree, and give the developer the PR link. The fix reaches their setup when the PR merges and they update the setup (README, "Update the setup").
