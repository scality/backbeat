shepherd scality/backbeat PR 1, it is my draft

This is a test run, and it is read-only: do not push, do not run any gh command that writes (no pr edit, no pr ready, no comment, no GraphQL mutation), do not resolve a thread, and do not write the journal or Jira. Do not run pr_state.py against GitHub: this is its output for this PR, and I am anurag4DSB:

```json
{
 "repo": "scality/backbeat",
 "number": 1,
 "url": "https://github.com/scality/backbeat/pull/1",
 "author": "anurag4DSB",
 "state": "OPEN",
 "draft": true,
 "head": "improvement/ZZ-1-x1",
 "base": "epic/ZZ-0-bnaas-d1",
 "mergeable": "MERGEABLE",
 "merge_state": "BLOCKED",
 "review_check": {
  "state": "action_required",
  "url": "https://github.com/scality/backbeat/actions/runs/1",
  "run_url": null
 },
 "failed_checks": [
  {
   "name": "lint",
   "conclusion": "failure",
   "url": "https://github.com/scality/backbeat/actions/runs/3"
  }
 ],
 "pending_checks": [
  "tests"
 ],
 "threads": {
  "claude": [
   {
    "id": "T1",
    "author": "claude",
    "path": "scripts/a.py",
    "url": "https://github.com/scality/backbeat/pull/1#discussion_r1",
    "created_at": "2026-10-22T08:15:00Z",
    "body": "The loop never stops when the check is cancelled."
   }
  ],
  "human": [
   {
    "id": "T2",
    "author": "tmacro",
    "path": "SKILL.md",
    "url": "https://github.com/scality/backbeat/pull/1#discussion_r2",
    "created_at": "2026-10-22T09:15:00Z",
    "body": "Why poll every 60 seconds and not 30?"
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
 "stacked": [
  {
   "number": 2,
   "url": "https://github.com/scality/backbeat/pull/2",
   "head": "improvement/ZZ-1-x2",
   "draft": true,
   "author": "anurag4DSB"
  }
 ],
 "draft_goal": false,
 "merge_goal": false
}
```
 Say what the shepherd would do at this pass, in order, and stop.
