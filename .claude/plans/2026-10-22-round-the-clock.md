# Two developers, two time zones, one backlog

## Context

D1 is built by Anurag (Paris) and Taylor (San Francisco), each with Claude Code. Their days barely overlap, so the work must be able to move around the clock without a chat or a meeting. On 2026-10-22 the team dropped epic releases: Vault, CloudServer and backbeat work on epic branches that Federation deploys by commit sha, and Arsenal and cloudserverclient work on their development branches. That removes the release chain, the epic tags and the push gate from the setup, and leaves one question: how do two people and their agents coordinate with as little manual work as possible?

## The model

Shared state in four places, each the owner of one thing:

| Question | Where | Who writes it |
|---|---|---|
| What is the work and what depends on what? | The Backlog tab of the tech spec doc, the breakdown as a graph | An edit of the Backlog tab |
| Who does what, and is it done? | Jira: one ticket per task, assigned, Done when done | `/bnaas-whats-next`, when a developer claims a task |
| What is the code and who must review it? | GitHub PRs | The developer's session |
| What moved, and what did the other session decide or hit? | The D1 journal: the Log section of the journal doc | Hooks, as work happens, and `journal note` for decisions, blockers and surprises |

The Tickets tab stays as the human view of the claimed tasks. Claude keeps it current. It is not the dependency source: dependencies in a doc table are hard for an agent to read and easy for a person to break, and the breakdown already has them.

One skill carries a session through that state. `/bnaas-whats-next` reads everything and gives what moved since the last run, the review queue, the ready tasks and the other developer's work, then claims one task after one question. Nobody writes an end-of-day note: hooks log commits, pushes, PRs, test results and ticket changes to the journal as they happen, and a session adds a one-sentence note when a decision, a blocker or a surprise happens. Everything else is the normal engineering flow: plan in the PR, `tdd`, `bnaas-reviewer`, draft PR, the Claude Code review on the PR, two human approvals.

Federation follows the epics by commit. `/bnaas-pin-federation` reads the three epic heads, checks that CI built their images, edits `group_vars/all` and opens or updates one draft PR. What's next runs it in dry run, so a developer who starts their day sees "pins are behind" and can open the PR in one step. The same script runs in Federation CI on `workflow_dispatch` from the epic.

## What changes in the setup

- Rules rewritten: 8 rules, shorter. Gone: the epic tags, the release chain, the local review gate and its hooks, the `bnaas-release` skill. New: Rule 1 (shared state), Rule 2 (what's next and the journal), Rule 3 (claim before you build), Rule 6 (Federation by commit).
- The Backlog tab, seeded by `bnaas/scripts/import_breakdown.py`, and `bnaas/scripts/backlog.py`, which reads an export of it, with `ready`, `blocked`, `deps` and `check`.
- `bnaas-whats-next`: dependencies from the backlog, the journal since the last run first, the pins line, no push gate in its self-improvement.
- `bnaas-pin-federation`: new.
- `bnaas-plan-pr`: removed. A plan PR is only for a change to the shared contract beyond the approved tech spec, and the shepherd (`bnaas-shepherd-prs`, next PR) opens it.
- `settings.json`: the journal hooks only, which never block a tool call.

## Decisions for Anurag and Taylor

1. Dependencies in the Backlog tab, not in a Depends on column of the Tickets tab, which stays a rendered view. The agents read an export of the Backlog tab.
2. Plan in the PR by default; a plan PR only when a change alters the shared contract beyond the approved tech spec. A value the spec leaves open is proposed in the plan commit as provisional and written into the tech spec before the PR leaves draft. This replaces "humans review the plan before any code", which costs a day across the time zones.
3. No local review before a push. The review stage is the Claude Code review on the PR, plus `bnaas-reviewer` on the diff before each commit. The scality-skills `/review-pr` run before each push is dropped: it found nothing the PR review missed, and it took 5 to 15 minutes a push.
4. scality-kms stays pinned to a released version in the Federation epic (`og_docker_image_version` of the scality-kms entry), because no scality-kms image exists per commit. D1 does not change scality-kms.
5. The pin PR is opened by what's next, by a person, or by CI: each service epic gets a `notify-federation` job (after its image build, only on a push to the epic) that runs `gh workflow run bnaas-pins.yaml --repo scality/Federation --ref epic/... -f setup_ref=<backbeat epic head>` with the GitHub App token. The bot works on its own branch, `improvement/<ticket>-epic-image-pins-bot`, and developers on `improvement/` branches, so a force push never hits someone else's work. The job has `continue-on-error`, because one thing is unproven: GitHub may resolve a workflow file for dispatch only when it also exists on the default branch. If the first dispatch fails, a stub `bnaas-pins.yaml` with only `workflow_dispatch` goes to Federation `development/10` through a normal PR; the run then uses the epic's copy. The App must be installed on Federation with Actions write, and `ACTIONS_APP_ID` and `ACTIONS_APP_PRIVATE_KEY` must reach backbeat (no backbeat workflow uses them today).
6. The journal is written by hooks as work happens, not by hand at the end of the day, which relies on people remembering it. It lives in the journal doc only: a shell hook cannot write a Claude Doc, so the hooks buffer the facts locally and the Stop hook asks the session to append them to the Log section.

## To apply outside this PR

- Jira: the setup tickets of rows 3.1, 3.2, 3.3, 3.5 and 22c of the Tickets tab describe the old release path (the per-repository releases, the release skill and the gate) and need one closing comment each, or are dropped.
- The tech spec doc: the Remove before merge rows about tags, retags and the scality-kms CI job go; the Releases tab becomes the pin state; the "Without the release workflow" tab is now history.
- Federation: the `bnaas-pins` workflow file goes into the Federation epic through the pin PR. That PR replaces Federation#7261, whose `-bnaas.1` tags do not exist on ghcr.io, and it pins scality-kms to `9.4.4-federation` once.
- Vault, CloudServer and backbeat: a `notify-federation` job after the image build, on a push to the epic only, with `continue-on-error`, that starts the Federation `bnaas-pins` workflow. It needs the GitHub App with Actions write on Federation, and `ACTIONS_APP_ID` and `ACTIONS_APP_PRIVATE_KEY` in each of the three repositories. Until it lands, a person or what's next starts the pin run after an epic moves.
- The pin workflow needs the GitHub App installed on Federation with contents and pull-requests write, and read on Vault (private), so that the bot can read the Vault epic head and its PR triggers CI.
- Federation needs the repository variable `BNAAS_PINS_TICKET`, the Federation D1 ticket of the pin PR and of the bot branch `improvement/<ticket>-epic-image-pins-bot`, so that no key sits in the workflow. The run stops with one line until it is set.
- The workspace symlinks: add the `bnaas` link to the linked folders (README); `hooks` was linked already.
- The Federation `bnaas-pins` workflow mints two app tokens: one with contents and pull-requests write on Federation only, for the pin branch and its PR, and one read-only on Vault, cloudserver and backbeat, for the epic heads.

## Verification

- `backlog.py check` reports every D1 task, no cycle, and the unresolved dependency texts.
- One read-only what's next run marks at least one task ready and one blocked from the backlog, and shows what moved since the last run.
- `pins.py --dry-run` prints the three epic heads and the state of their images against the current Federation pins.
- A `bnaas-reviewer` pass on the new tree finds no stale reference to tags, releases, the review hooks or the push gate.
- `.claude/hooks/test-journal.sh` passes: the hooks log the facts of sample tool calls, skip other owners, and keep the buffer when `gh` fails.

## Backlog and progress live in the doc

Since the first draft of this plan, the backlog moved from a YAML file into the Backlog tab of the tech spec doc, seeded from the breakdown by `import_breakdown.py`. People fix a dependency or add new scope there, with no PR, and the scripts read an export of the tab. A Progress tab next to it charts the progress by area and the scope and pivots per week; what's next updates both charts after each render of the Tickets tab, and rewrites the Pivots table of the journal doc from the decision notes of the journal. Re-seeding the tab from the breakdown stays a deliberate step, and the corrections it must keep stay in the importer.
