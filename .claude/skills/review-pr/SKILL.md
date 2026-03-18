<!-- markdownlint-disable -->

---
name: review-pr
description: Review a PR on backbeat (Node.js async queue and job manager for S3C and Artesca)
argument-hint: <pr-number-or-url>
disable-model-invocation: true
allowed-tools: Read, Bash(gh repo view *), Bash(gh pr view *), Bash(gh pr diff *), Bash(gh pr comment *), Bash(gh api *), Bash(git diff *), Bash(git log *), Bash(git show *)
---

# Review GitHub PR

You are an expert code reviewer. Review this PR: $ARGUMENTS

## Determine PR target

Parse `$ARGUMENTS` to extract the repo and PR number:

- If arguments contain `REPO:` and `PR_NUMBER:` (CI mode), use those values directly.
- If the argument is a GitHub URL (starts with `https://github.com/`), extract `owner/repo` and the PR number from it.
- If the argument is just a number, use the current repo from `gh repo view --json nameWithOwner -q .nameWithOwner`.

## Output mode

- **CI mode** (arguments contain `REPO:` and `PR_NUMBER:`): post inline comments and summary to GitHub.
- **Local mode** (all other cases): output the review as text directly. Do NOT post anything to GitHub.

## Steps

1. **Fetch PR details:**

```bash
gh pr view <number> --repo <owner/repo> --json title,body,headRefOid,author,files
gh pr diff <number> --repo <owner/repo>
```

1. **Read changed files** to understand the full context around each change (not just the diff hunks).

1. **Analyze the changes** against these criteria:

| Area | What to check |
|------|---------------|
| Async error handling | Uncaught promise rejections, missing error callbacks, swallowed errors in streams. Double callbacks in try/catch blocks (callback called in try then again in catch) |
| Async/await usage | New or modified code should use async/await instead of callbacks when possible. When code is migrated from callbacks to async/await, verify: no leftover callback or next params, no mixed callback + promise patterns, proper try/catch around awaited calls, errors are re-thrown or handled (not silently swallowed). Watch for the anti-pattern: `try { cb(); } catch(err) { cb(err); }` where an exception after the first `cb()` triggers a second call |
| Kafka consumer/producer | Correct topic configuration, proper offset commits, consumer group handling, message serialization. Verify `onEntryCommittable` is always reachable. Check circuit breaker thresholds when adding new downstream topics |
| Stream handling | Backpressure, proper cleanup on error, no leaked file descriptors, correct pipe chains |
| Dependency pinning | Git-based deps (arsenal, vaultclient, bucketclient, werelogs, breakbeat, httpagent) must pin to a tag, not a branch |
| Logging | Proper use of werelogs, no `console.log` in production code, log levels match severity. Include enough context (bucket, object key, version, offset) for production troubleshooting |
| Prometheus metrics | New metrics follow existing naming conventions (`s3_backbeat_*`), correct metric types (counter vs gauge vs histogram), bounded label cardinality — avoid per-connector or per-bucket labels that explode with scale |
| Config changes | Backward compatibility, Joi schema updates match new fields, environment variable naming, default values. Env var overrides in `lib/Config.js` must stay consistent with the config file schema |
| MongoDB / Redis resilience | Reconnection handling, proper timeouts on external calls, no indefinite waits. Network errors to MongoDB must not cause stuck tasks or silent data loss |
| Extension architecture | Changes respect the pluggable extension pattern, no cross-extension coupling |
| Security | Command injection, prototype pollution, unsafe deserialization, credential exposure in config/env vars, OWASP-relevant issues for Node.js |
| Breaking changes | Anything that changes public APIs, Kafka message formats, inter-service contracts, or oplog/change stream projections |

1. **Deliver your review:**

### If CI mode: post to GitHub

#### Part A: Inline file comments

For each specific issue, post a comment on the exact file and line:

```bash
gh api -X POST -H "Accept: application/vnd.github+json" "repos/<owner/repo>/pulls/<number>/comments" -f body="Your comment<br><br>— Claude Code" -f path="path/to/file" -F line=<line_number> -f side="RIGHT" -f commit_id="<headRefOid>"
```

**The command must stay on a single bash line.** Never use newlines in bash commands — use `<br>` for line breaks in comment bodies. Never put `<br>` inside code blocks or suggestion blocks.

Each inline comment must:

- Be short and direct — say what's wrong, why it's wrong, and how to fix it in 1-3 sentences
- No filler, no complex words, no long explanations
- When the fix is a concrete line change (not architectural), include a GitHub suggestion block so the author can apply it in one click:

  ````text
  ```suggestion
  corrected-line-here
  ```
  ````

  Only suggest when you can show the exact replacement. For architectural or design issues, just describe the problem.
  Example with a suggestion block:

  ```bash
  gh api ... -f body=$'Missing the shared-guidelines update command.<br><br>\n```suggestion\n/plugin update shared-guidelines@scality-agent-hub\n/plugin update scality-skills@scality-agent-hub\n```\n<br><br>— Claude Code' ...
  ```

- When the comment contains a suggestion block, use `$'...'` quoting with `\n` for code fence boundaries. Escape single quotes as `\'` (e.g., `don\'t`)
- End with: `— Claude Code`

Use the line number from the **new version** of the file (the line number you'd see after the PR is merged), which corresponds to the `line` parameter in the GitHub API.

#### Part B: Summary comment

```bash
gh pr comment <number> --repo <owner/repo> --body "LGTM<br><br>Review by Claude Code"
```

**The command must stay on a single bash line.** Never use newlines in bash commands — use `<br>` for line breaks in comment bodies.

Do not describe or summarize the PR. For each issue, state the problem on one line, then list one or more suggestions below it:

```text
- <issue>
  - <suggestion>
  - <suggestion>
```

If no issues: just say "LGTM". End with: `Review by Claude Code`

### If local mode: output the review as text

Do NOT post anything to GitHub. Instead, output the review directly as text.

For each issue found, output:

```text
**<file_path>:<line_number>** — <what's wrong and how to fix it>
```

When the fix is a concrete line change, include a fenced code block showing the suggested replacement.

At the end, output a summary section listing all issues. If no issues: just say "LGTM".

End with: `Review by Claude Code`

## What NOT to do

- Do not comment on markdown formatting preferences
- Do not suggest refactors unrelated to the PR's purpose
- Do not praise code — only flag problems or stay silent
- If no issues are found, post only a summary saying "LGTM"
- Do not flag style issues already covered by the project's linter (eslint with eslint-config-scality)
