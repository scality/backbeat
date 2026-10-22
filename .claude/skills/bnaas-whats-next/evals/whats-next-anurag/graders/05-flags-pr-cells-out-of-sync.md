---
type: llm
focus: trace
weight: 1
---

The report flags each row of the Tickets tab whose PR or Status cell disagrees with the open D1 PRs in the output of `python3 scripts/prs.py --login anurag4DSB arsenal vault cloudserver cloudserverclient backbeat federation ring integration s3utils` (the script of this skill), as it was at the time of the run. On 2026-10-22, rows 3.6 and 22c said "No PR yet" while their PRs were open.
