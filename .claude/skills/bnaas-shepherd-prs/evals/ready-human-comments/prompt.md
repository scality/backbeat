I marked scality/backbeat PR 1 ready yesterday, shepherd it

This is a test run, and it is read-only: do not push, do not run any gh command that writes (no pr edit, no comment, no GraphQL mutation), do not resolve a thread, and do not write the journal or Jira. Do not ask me a question: write the question you would ask instead. Do not run pr_state.py against GitHub: this is its output for this PR, and I am anurag4DSB:

```json
{
 "repo": "scality/backbeat",
 "number": 1,
 "url": "https://github.com/scality/backbeat/pull/1",
 "author": "anurag4DSB",
 "state": "OPEN",
 "draft": false,
 "head": "improvement/ZZ-1-x1",
 "base": "epic/ZZ-0-bnaas-d1",
 "mergeable": "MERGEABLE",
 "merge_state": "BLOCKED",
 "review_check": {
  "state": "success",
  "url": "https://github.com/scality/backbeat/actions/runs/1",
  "run_url": null
 },
 "failed_checks": [],
 "pending_checks": [],
 "threads": {
  "claude": [],
  "human": [
   {
    "id": "T2",
    "author": "tmacro",
    "path": "SKILL.md",
    "url": "https://github.com/scality/backbeat/pull/1#discussion_r2",
    "created_at": "2026-10-22T09:15:00Z",
    "body": "Why poll every 60 seconds and not 30?"
   },
   {
    "id": "T5",
    "author": "nicolas2bert",
    "path": "SKILL.md",
    "url": "https://github.com/scality/backbeat/pull/1#discussion_r5",
    "created_at": "2026-10-22T14:00:00Z",
    "body": "typo: 'recieve' in SKILL.md line 12"
   }
  ],
  "other": [
   {
    "id": "T3",
    "author": "anurag4dsb",
    "path": "SKILL.md",
    "url": "https://github.com/scality/backbeat/pull/1#discussion_r3",
    "created_at": "2026-10-22T10:15:00Z",
    "body": "Note to self: rename after the merge."
   }
  ]
 },
 "approvals": [
  "hopocalypse-member",
  "nicolas2bert"
 ],
 "requested": [],
 "reviewed": [
  "hopocalypse-member",
  "nicolas2bert",
  "scality/hopocalypse"
 ],
 "changes_requested": [],
 "other_dev": "tmacro",
 "other_approved": false,
 "stacked": [],
 "draft_goal": true,
 "merge_goal": false
}
```
 Say what the shepherd would do at this pass, in order, and stop.
