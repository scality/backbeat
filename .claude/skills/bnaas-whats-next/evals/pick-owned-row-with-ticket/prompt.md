---
tags: [skill-creator]
max_turns: 60
timeout_seconds: 1200
allowed_tools: [Read, Glob, Grep, Skill, Bash, ToolSearch, mcp__claude_ai_Claude_Docs__read, mcp__claude_ai_Claude_Docs__guide, mcp__plugin_design_atlassian__getAccessibleAtlassianResources, mcp__plugin_design_atlassian__atlassianUserInfo, mcp__plugin_design_atlassian__lookupJiraAccountId, mcp__plugin_design_atlassian__searchJiraIssuesUsingJql, mcp__plugin_design_atlassian__getJiraIssue, mcp__claude_ai_Atlassian_Rovo__getAccessibleAtlassianResources, mcp__claude_ai_Atlassian_Rovo__atlassianUserInfo, mcp__claude_ai_Atlassian_Rovo__lookupJiraAccountId, mcp__claude_ai_Atlassian_Rovo__searchJiraIssuesUsingJql, mcp__claude_ai_Atlassian_Rovo__getJiraIssue, mcp__plugin_design_atlassian__getTransitionsForJiraIssue, mcp__plugin_design_atlassian__transitionJiraIssue, mcp__claude_ai_Atlassian_Rovo__getTransitionsForJiraIssue, mcp__claude_ai_Atlassian_Rovo__transitionJiraIssue, mcp__claude_ai_Claude_Docs__update]
---

I'll take row 3.5 next. Create the Jira ticket for me.

Answer as Taylor: GitHub login tmacro, Jira user Taylor McKinnon. The gh and Atlassian tools here are logged in as Anurag, so use Taylor's identity instead of the one they return.

This is a test run: do not create or edit a Jira ticket, do not push, and do not open a PR. The writes allowed are the skill's renders (the Tickets tab, the charts and the journal doc) and its In Review auto-fix: moving an Ongoing ticket of the developer whose PR is open and not a draft to In Review. Reading Jira, the doc and GitHub is fine.
