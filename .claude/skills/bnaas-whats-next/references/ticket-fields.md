# The D1 ticket and the current sprint

The fields of a new D1 ticket, for the "start task" action of `SKILL.md`. `<base>` is the folder of the skill.

## The fields

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
| Description | What the row asks. Then "Done when:" and a numbered list of results that a reviewer can check. Then the line "Tracked in the Tickets tab of the BNaaS D1 tech spec, row <ID>: <doc link>". `render_tickets.py` finds the task by this line, so never leave it out. If the scope is not clear from the row and the tech spec, ask the developer. |

## The current sprint

Find the current sprint in Jira, because the sprint calendar can move:

1. Search `parent = <epic key> AND sprint in openSprints()`, with `maxResults` 10 and only the field `customfield_10004`. Put the values of that field together into one JSON array of sprints, and pass it on stdin, through a quoted heredoc, because a sprint name or goal can hold an apostrophe:

   ```
   python3 -I <base>/scripts/sprint.py --jira - <<'EOF'
   <the JSON array>
   EOF
   ```

   The script keeps only the active sprints of the team board, because `openSprints()` also matches future sprints and a ticket can also be in a sprint of another board. It says whether the name matches the dates.
2. If the script finds no active sprint, for example because the D1 tickets are only in a future sprint, search `sprint = "<the name that sprint.py prints>"` in the same way.
3. If Jira still gives no active sprint, ask the developer. Do not create a sprint.

When `matches_dates` is false, because the name or the start or end date of the Jira sprint differs from the date math, Jira wins. Use its sprint, and tell the developer, so that the next setup PR moves the anchor date or the sprint length in `sprint.py`.
