---
tags: [skill-creator, practice-tab]
max_turns: 40
timeout_seconds: 900
allowed_tools: [Read, Glob, Grep, Skill, Bash, Agent, ToolSearch, mcp__claude_ai_Claude_Docs__guide, mcp__claude_ai_Claude_Docs__read, mcp__claude_ai_Claude_Docs__update]
---

Note that we decided to keep the journal in the tech spec, because one place is easier for both of us.

In this test run, put `BNAAS_JOURNAL_DIR="$PWD/journal" BNAAS_JOURNAL_CONFIG="$PWD/journal-practice.json"` in front of every `journal` command, yours and the agent's: they point the journal at the "Journal practice" tab of the tech spec. Write to no other tab. Do not ask me a question.

Input files: journal/, journal-practice.json, .claude/
