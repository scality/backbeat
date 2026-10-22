---
type: llm
focus: trace
weight: 1
---
The run renders the full state for the "Tickets / Tasks" tab and the journal doc (into <tmp>, since it is a test run), or says why it could not, and leaves the out_of_sync lines of render_tickets.py to the tab (or offers them as one Jira-fix action) instead of listing them all in the terminal.
