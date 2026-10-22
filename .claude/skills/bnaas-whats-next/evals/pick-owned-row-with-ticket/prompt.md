---
tags: [skill-creator]
max_turns: 60
timeout_seconds: 1200
allowed_tools: [Read, Glob, Grep, Skill, Bash, ToolSearch, mcp__claude_ai_Claude_Docs__read, mcp__claude_ai_Claude_Docs__guide, mcp__plugin_design_atlassian__getAccessibleAtlassianResources, mcp__plugin_design_atlassian__atlassianUserInfo, mcp__plugin_design_atlassian__lookupJiraAccountId, mcp__plugin_design_atlassian__searchJiraIssuesUsingJql, mcp__plugin_design_atlassian__getJiraIssue, mcp__claude_ai_Atlassian_Rovo__getAccessibleAtlassianResources, mcp__claude_ai_Atlassian_Rovo__atlassianUserInfo, mcp__claude_ai_Atlassian_Rovo__lookupJiraAccountId, mcp__claude_ai_Atlassian_Rovo__searchJiraIssuesUsingJql, mcp__claude_ai_Atlassian_Rovo__getJiraIssue]
---

I'll take row 3.5 next. Create the Jira ticket for me and put me on the row.

Answer as Taylor: GitHub login tmacro, Jira user Taylor McKinnon. The gh and Atlassian tools here are logged in as Anurag, so use Taylor's identity instead of the one they return.

This is a test run: do not write to Jira, do not edit the tech spec doc, do not push, and do not open a PR. Reading Jira, the doc and GitHub is fine.
