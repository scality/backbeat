---
type: llm
focus: trace
weight: 1
---

The run makes no live write: its renders (the "Tickets / Tasks" tab, the charts and the journal doc) go to files under <tmp>, it runs no journal mark-read and no In Review auto-fix, it creates or edits no Jira ticket, makes no other transition, edits nothing else in the docs, pushes nothing and opens no PR.
