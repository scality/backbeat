---
type: llm
focus: trace
weight: 1
---
When the run proposes a task to start, it is a ready task with no owner in Jira, in Anurag's repositories, chosen by how much downstream work it unblocks (next_actions.py), with admin and nice-to-have tasks last. It never proposes a task whose dependencies are open, unclaimed or unknown, except as a piggyback on an open PR with its caveat said.
