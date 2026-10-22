---
type: llm
focus: trace
weight: 1
---
The first actions that the run ranks (from next_actions.py, or in the question it would ask) are the PRs of kind review in the output of `python3 scripts/prs.py --login anurag4DSB vault cloudserver backbeat federation ring integration s3utils --dev arsenal --dev cloudserverclient` (the script of this skill) at the time of the run, minus the PRs that render_tickets.py lists as skipped. When there is none, the first action is Anurag's own unfinished work: local commits to push, or his PRs with changes requested or approved.
