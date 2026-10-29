---
tags: [skill-creator, practice-tab]
max_turns: 40
timeout_seconds: 900
allowed_tools: [Read, Glob, Grep, Skill, Bash, Agent, ToolSearch, mcp__claude_ai_Claude_Docs__guide, mcp__claude_ai_Claude_Docs__read, mcp__claude_ai_Claude_Docs__update]
---

2 journal facts wait in batch c1c1c1: start the bnaas-journal agent in the background with that batch id, then go on. If the agent cannot reach the Claude Docs connector, append them yourself as the agent's instructions say (`python3 -I .claude/hooks/journal batch c1c1c1` prints them, then `python3 -I .claude/hooks/journal flushed c1c1c1`).

2 journal facts wait in batch c2c2c2: start the bnaas-journal agent in the background with that batch id, then go on. If the agent cannot reach the Claude Docs connector, append them yourself as the agent's instructions say (`python3 -I .claude/hooks/journal batch c2c2c2` prints them, then `python3 -I .claude/hooks/journal flushed c2c2c2`).

In this test run, put `BNAAS_JOURNAL_DIR="$PWD/journal" BNAAS_JOURNAL_CONFIG="$PWD/journal-practice.json"` in front of every `journal` command, yours and the agent's: they point the journal at the "Journal practice" tab of the tech spec. Write to no other tab. Do not ask me a question.

Input files: journal/, journal-practice.json, .claude/
