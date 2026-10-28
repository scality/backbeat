#!/bin/sh
# Test the D1 journal hooks: feed them sample hook input and check the
# buffer, the Stop request and the reading of a doc export. Nothing is
# written to the doc. Run: .claude/hooks/test-journal.sh
HOOKS=$(cd "$(dirname "$0")" && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
export BNAAS_JOURNAL_DIR="$TMP/journal"
BUF="$BNAAS_JOURNAL_DIR/buffer.jsonl"
FAIL=0

check() { # <description> <test command...>
  desc=$1; shift
  if "$@"; then echo "ok: $desc"; else echo "FAIL: $desc"; FAIL=1; fi
}
lines() { [ -f "$BUF" ] && wc -l < "$BUF" | tr -d ' ' || echo 0; }
has() { grep -q -- "$1" "$BUF"; }
capture() { "$HOOKS/journal-capture.sh"; }
bash_ok() { # <cwd> <command> [stdout]
  python3 -c 'import json,sys; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"Bash","cwd":sys.argv[1],"tool_input":{"command":sys.argv[2]},"tool_response":{"stdout":sys.argv[3],"stderr":"","interrupted":False}}))' "$1" "$2" "${3:-}" | capture
}
commit() { # <subject>: a real commit, as the command under test made it
  git -C "$REPO" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m "$1"
}
bash_fail() { # <cwd> <command> <error>
  python3 -c 'import json,sys; print(json.dumps({"hook_event_name":"PostToolUseFailure","tool_name":"Bash","cwd":sys.argv[1],"tool_input":{"command":sys.argv[2]},"error":sys.argv[3]}))' "$1" "$2" "$3" | capture
}

# A scality clone with a branch and a commit, and a clone of another owner.
REPO="$TMP/backbeat"
git init -q -b improvement/topic-journal "$REPO"
git -C "$REPO" remote add origin git@github.com:scality/backbeat.git
git -C "$REPO" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m "Start"
OTHER="$TMP/personal"
git init -q -b main "$OTHER"
git -C "$OTHER" remote add origin git@github.com:someone/personal.git
git -C "$OTHER" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m "Personal"

# A branch seen for the first time logs no commit: HEAD is only recorded.
bash_fail "$REPO" "git add -A && git commit -m x && git push" "Exit code 1"
check "a failed first commit on a branch not seen before logs nothing" test "$(lines)" = 0
commit "Add the journal sample"
bash_ok "$TMP" "cd $REPO && git commit -m 'secret=hunter2; more'"
check "a commit after cd logs the repo, branch, sha and subject" \
  has '"repo": "backbeat", "branch": "improvement/topic-journal", "kind": "commit", "text": "commit [0-9a-f]* Add the journal sample"'
check "the command text is never logged" sh -c "! grep -q hunter2 '$BUF'"
n=$(lines)
git -C "$REPO" switch -q -c improvement/topic-new
bash_ok "$REPO" "git switch -c improvement/topic-new"
bash_fail "$REPO" "git commit -m x && git push" "Exit code 1"
check "a failed first commit on a new branch logs nothing" test "$(lines)" = "$n"
commit "First on the new branch"
bash_ok "$REPO" "git commit -m x"
check "the first real commit on a new branch is logged" has 'commit [0-9a-f]* First on the new branch'
git -C "$REPO" switch -q improvement/topic-journal
bash_ok "$REPO" "git switch improvement/topic-journal"
git -C "$REPO" worktree add -q -b improvement/topic-wt "$TMP/wt"
bash_ok "$REPO" "git worktree add -b improvement/topic-wt $TMP/wt"
git -C "$TMP/wt" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m "First in the worktree"
bash_ok "$TMP/wt" "git commit -m x"
check "a worktree add records the new branch, so its first commit is logged" \
  has '"branch": "improvement/topic-wt", "kind": "commit", "text": "commit [0-9a-f]* First in the worktree"'
# A pull that moves HEAD before a commit that fails: no commit fact.
git clone -q "$REPO" "$TMP/up"
git -C "$TMP/up" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m "Pulled from upstream"
n=$(lines)
git -C "$REPO" pull -q --ff-only "$TMP/up" improvement/topic-journal
bash_fail "$REPO" "git pull && git commit -m x && git push" "Exit code 1"
check "a pull then a failed commit logs no commit" sh -c "! grep -q 'commit [0-9a-f]* Pulled from upstream' '$BUF'"
check "the pull logs that it moved HEAD" has '"kind": "head", "text": "pull moved HEAD to [0-9a-f]* Pulled from upstream"'
# A rebase that moves HEAD before a no-op commit: no commit fact.
commit "To rebase away"
bash_ok "$REPO" "git status"
git -C "$REPO" rebase -q --onto HEAD~1 HEAD
bash_ok "$REPO" "git rebase -q --onto HEAD~1 HEAD; git commit -m x"
check "a rebase then a no-op commit logs no commit" sh -c "! tail -1 '$BUF' | grep -q '\"kind\": \"commit\"'"
check "the rebase logs that it moved HEAD" has '"kind": "head", "text": "rebase moved HEAD to'
# A push that happened leaves the remote-tracking branch at HEAD.
pushed() { git -C "$REPO" update-ref -m "update by push" "refs/remotes/origin/$(git -C "$REPO" branch --show-current)" HEAD; }
pushed
git -C "$REPO" branch -q -u "origin/improvement/topic-journal"
bash_ok "$TMP" "git -C $REPO push -u origin HEAD"
check "a push with git -C logs the push" has '"kind": "push"'
bash_ok "$TMP" "git -C $REPO push -u origin HEAD"
check "a real push logs once, and an Everything up-to-date re-push logs nothing" \
  test "$(grep -c '"kind": "push"' "$BUF")" = 1
# A stack pushed from one worktree names a branch that is not checked out.
git -C "$REPO" branch -q improvement/topic-stacked
git -C "$REPO" update-ref -m "update by push" refs/remotes/origin/improvement/topic-stacked improvement/topic-stacked
bash_ok "$REPO" "git push --force-with-lease=improvement/topic-stacked:abc origin improvement/topic-stacked"
check "a push of another branch than the checked-out one is logged under it" \
  has '"branch": "improvement/topic-stacked", "kind": "push"'
sed -i.bak '/topic-stacked", "kind": "push"/d' "$BUF" && rm -f "$BUF.bak"
# A stack push where one lease is rejected still pushed the others.
git -C "$REPO" branch -q improvement/topic-stacked-2
git -C "$REPO" branch -q improvement/topic-stacked-3
git -C "$REPO" update-ref -m "update by push" refs/remotes/origin/improvement/topic-stacked-2 improvement/topic-stacked-2
bash_fail "$REPO" "git push --force-with-lease origin improvement/topic-stacked-2 improvement/topic-stacked-3" "Exit code 1"
check "a stack push with one rejected lease logs the branches it pushed" \
  sh -c "grep -q 'topic-stacked-2\", \"kind\": \"push' '$BUF' && ! grep -q 'topic-stacked-3\", \"kind\": \"push' '$BUF'"
sed -i.bak '/topic-stacked-2", "kind": "push"/d' "$BUF" && rm -f "$BUF.bak"
# A pull moved the remote-tracking branch, then a push that sent nothing.
commit "Pulled from another clone"
git -C "$REPO" update-ref -m "fetch: fast-forward" refs/remotes/origin/improvement/topic-journal HEAD
n=$(grep -c '"kind": "push"' "$BUF")
bash_ok "$REPO" "git pull && git push"
check "a push after a fetch moved the remote-tracking branch, with nothing sent, logs nothing" \
  test "$(grep -c '"kind": "push"' "$BUF")" = "$n"
commit "Pushed with a redirection"
pushed
bash_ok "$REPO" "git push origin HEAD 2>&1 | tail -3"
check "a push with 2>&1 piped into tail is logged" test "$(grep -c '"kind": "push"' "$BUF")" = 2
# Two pushes in one second: the reflog keeps whole seconds only.
NOW="$(date +%s) +0000"
commit "Pushed in the same second, first"
GIT_COMMITTER_DATE="$NOW" pushed
bash_ok "$REPO" "git push"
commit "Pushed in the same second, second"
GIT_COMMITTER_DATE="$NOW" pushed
bash_ok "$REPO" "git push"
check "two pushes in the same second log two facts" test "$(grep -c '"kind": "push"' "$BUF")" = 4
n=$(lines)
commit "Not pushed yet"
bash_fail "$REPO" "git push --force-with-lease && gh pr create --draft" "Exit code 1"
bash_ok "$REPO" "git push 2>&1 | tail -3"
check "a rejected lease or a failed piped push logs nothing" test "$(lines)" = "$n"
bash_ok "$REPO" "gh pr create --draft --title t --body-file b" "https://github.com/scality/backbeat/pull/42"
check "gh pr create logs the PR link from the output" has 'PR create https://github.com/scality/backbeat/pull/42'
check "a create in the PR's own clone takes the clone's branch" \
  has '"repo": "backbeat", "branch": "improvement/topic-journal", "kind": "pr", "text": "PR create https://github.com/scality/backbeat/pull/42"'
bash_fail "$REPO" "yarn ft_test:replication" "Exit code 1"
bash_ok "$REPO" "npm run bh_test && yarn typecheck"
check "functional, behavior and typecheck scripts are logged by prefix" \
  sh -c "grep -q 'yarn ft_test:replication exit 1' '$BUF' && grep -q 'npm bh_test exit unknown' '$BUF' && grep -q 'yarn typecheck exit 0' '$BUF'"
bash_ok "$TMP" "gh pr create --repo scality/backbeat --title t" "https://github.com/scality/backbeat/pull/43"
check "a PR from a folder that is not a clone takes its repository from the PR" \
  has '"repo": "backbeat", "branch": "-", "kind": "pr", "text": "PR create https://github.com/scality/backbeat/pull/43"'
n=$(lines)
bash_fail "$TMP" "gh pr create --repo scality/backbeat --title t && gh pr view --web" "Exit code 1"
check "a failed gh pr create logs nothing" test "$(lines)" = "$n"
bash_fail "$TMP" "gh pr create --repo scality/backbeat --title t && gh pr view --web" "Exit code 1
https://github.com/scality/backbeat/pull/60
could not open a browser"
check "a create that worked before a later step failed logs its PR" \
  has 'PR create https://github.com/scality/backbeat/pull/60'
n=$(lines)
bash_fail "$TMP" "gh pr create --repo scality/backbeat --title t" "a pull request for branch \"b\" into branch \"e\" already exists:
https://github.com/scality/backbeat/pull/61"
check "a create that found an existing PR logs nothing" test "$(lines)" = "$n"
bash_ok "$TMP" "gh pr create --repo scality/backbeat --title a && gh pr create --repo scality/backbeat --title b" "https://github.com/scality/backbeat/pull/50
https://github.com/scality/backbeat/pull/51"
check "two creates in one call log each its own PR" \
  sh -c "grep -q 'PR create https://github.com/scality/backbeat/pull/50' '$BUF' && grep -q 'PR create https://github.com/scality/backbeat/pull/51' '$BUF'"
# The hook's cwd is already the folder the command moved to.
X="$REPO/.claude/worktrees/x"
git -C "$REPO" worktree add -q -b improvement/topic-x "$X"
bash_ok "$X" "git status"
git -C "$X" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m "Committed after a relative cd"
bash_ok "$X" "cd .claude/worktrees/x && git commit -m y"
check "a relative cd that the hook's cwd already ends with keeps that cwd" \
  has '"branch": "improvement/topic-x", "kind": "commit", "text": "commit [0-9a-f]* Committed after a relative cd"'
n=$(lines)
bash_fail "$REPO" "gh pr merge 12 --squash && git checkout epic && git pull" "Exit code 1"
bash_ok "$REPO" "gh pr ready 12 --undo"
bash_ok "$REPO" "gh pr merge 12 --auto"
check "a failed merge, ready --undo and merge --auto log no PR fact" test "$(lines)" = "$n"
bash_ok "$REPO" "gh pr close 2903 > /dev/null"
check "a number before a spaced redirection stays an argument" has '"text": "PR close 2903"'
git -C "$REPO" switch -q -c improvement/topic-oneliner
git -C "$REPO" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m "On a branch made in the same call"
bash_ok "$REPO" "git checkout -b improvement/topic-oneliner && git add -A && git commit -m y"
check "commits on a branch made in the same call are logged" \
  has '"branch": "improvement/topic-oneliner", "kind": "commit", "text": "commit [0-9a-f]* On a branch made in the same call"'
git -C "$REPO" switch -q -c improvement/topic-pushed
git -C "$REPO" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m "Pushed from a new branch"
git -C "$REPO" update-ref -m "update by push" refs/remotes/origin/improvement/topic-pushed HEAD
n=$(grep -c '"kind": "push"' "$BUF")
bash_ok "$REPO" "git checkout -b improvement/topic-pushed && git commit -m y && git push -u origin improvement/topic-pushed"
check "a push of a branch made in the same call is logged" \
  test "$(grep -c '"kind": "push"' "$BUF")" = "$((n + 1))"
git -C "$REPO" switch -q improvement/topic-journal
bash_ok "$REPO" "git switch improvement/topic-journal"
# A stale recorded sha (a commit made where the hook could not see it):
# the walk stops where the branch was entered, and the commits of the
# branch before it stay out.
git -C "$REPO" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m "Unseen"
git -C "$REPO" switch -q -c improvement/topic-other
git -C "$REPO" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m "On the other branch"
git -C "$REPO" switch -q improvement/topic-journal
git -C "$REPO" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m "Back on the journal branch"
bash_ok "$REPO" "git commit -m x"
check "a stale recorded sha logs no commit of the branch left behind" \
  sh -c "! grep -q 'On the other branch' '$BUF' && grep -q 'Back on the journal branch' '$BUF'"
bash_ok "$REPO" "gh pr ready"
check "gh pr ready with no number takes the clone's branch" \
  has '"repo": "backbeat", "branch": "improvement/topic-journal", "kind": "pr", "text": "PR ready ?"'
bash_ok "$REPO" "gh pr ready improvement/other"
check "gh pr ready <branch> logs the PR under the branch it names" \
  has '"repo": "backbeat", "branch": "improvement/other", "kind": "pr", "text": "PR ready improvement/other"'
bash_ok "$TMP" "gh pr ready improvement/third --repo scality/backbeat"
check "gh pr ready <branch> --repo logs the PR under the branch it names" \
  has '"repo": "backbeat", "branch": "improvement/third", "kind": "pr", "text": "PR ready improvement/third"'
n=$(lines)
bash_ok "$REPO" "gh pr merge 2902 \\
  --auto --squash"
check "a backslash continuation keeps the flags on the same command" test "$(lines)" = "$n"
bash_ok "$REPO" "gh pr ready improvement/\\
split"
check "a backslash continuation joins a word with nothing between, as bash does" \
  has '"branch": "improvement/split", "kind": "pr", "text": "PR ready improvement/split"'
bash_ok "$REPO" "gh pr ready 12 \\
  --repo scality/cloudserver"
check "a continued --repo names the repository" has '"repo": "cloudserver", "branch": "-", "kind": "pr", "text": "PR ready 12"'
commit "Committed without a pager"
bash_ok "$REPO" "git --no-pager commit -m x"
check "git --no-pager commit is logged" has 'Committed without a pager'
# A worktree made outside Bash: its first commit is the first Bash call.
git -C "$REPO" worktree add -q -b improvement/topic-unseen "$TMP/unseen"
git -C "$TMP/unseen" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m "First in an unseen worktree"
bash_ok "$TMP/unseen" "git commit -m x"
check "the first commit of a worktree made outside Bash is logged" \
  has 'commit [0-9a-f]* First in an unseen worktree'
bash_ok "$REPO" "gh pr ready 2902"
check "gh pr ready by number in a clone takes the clone's repository" \
  has '"repo": "backbeat", "branch": "-", "kind": "pr", "text": "PR ready 2902"'
n=$(lines)
bash_ok "$REPO" "gh pr merge 5 --repo someone/personal"
check "a --repo outside scality logs nothing, even inside a clone" test "$(lines)" = "$n"
bash_ok "$REPO" "gh pr ready 7 --repo=scality/cloudserver"
check "--repo=scality/x names the repository" has '"repo": "cloudserver", "branch": "-", "kind": "pr", "text": "PR ready 7"'
n=$(lines)
bash_ok "$REPO" "gh pr merge https://github.com/someone/personal/pull/9"
check "a PR URL outside scality, run in a scality clone, logs nothing" test "$(lines)" = "$n"
bash_ok "$TMP" "gh pr merge 2902 --squash"
check "gh pr merge by number outside a clone logs nothing" test "$(lines)" = "$n"
bash_fail "$REPO" "yarn test 2>&1" "Exit code 1
lots of output"
check "a failing test logs its exit code" has 'yarn test exit 1'
bash_ok "$REPO" "yarn lint 2>&1 | tail -5"
check "a check piped into another command logs exit unknown" has 'yarn lint exit unknown'
bash_ok "$REPO" "S3BACKEND=mem yarn mocha tests/unit/x.js --exit"
check "a passing yarn mocha run logs exit 0" has "mocha exit 0"
bash_ok "$REPO" "yarn test || true"
check "a check before another segment logs exit unknown" has 'yarn test exit unknown'
n=$(lines)
bash_fail "$REPO" "git add -A && git commit -m x && git push" "Exit code 1"
bash_ok "$REPO" "git commit -m x 2>&1 | tail -3"
check "a commit that left HEAD as it was logs nothing" test "$(lines)" = "$n"
commit "Amend the sample"
bash_fail "$REPO" "git commit --amend --no-edit && yarn jest" "Exit code 1"
check "a commit before a failing check is still logged, and the check may not have run" \
  sh -c "tail -2 '$BUF' | grep -q '\"kind\": \"commit\"' && tail -1 '$BUF' | grep -q 'jest may not have run'"
bash_fail "$REPO" "yarn test" "Command timed out"
check "a check with no exit code in its error (a timeout) logs exit unknown" \
  sh -c "tail -1 '$BUF' | grep -q 'yarn test exit unknown'"
bash_fail "$REPO" "cd $REPO && yarn jest" "Exit code 1"
check "after only cd steps, a failed check keeps its exit code" sh -c "tail -1 '$BUF' | grep -q 'jest exit 1'"
bash_fail "$REPO" "source ~/.nvm/nvm.sh && nvm use 22 && yarn mocha tests/x.js" "Exit code 3"
check "after the nvm setup, a failed check keeps its exit code" sh -c "tail -1 '$BUF' | grep -q 'mocha exit 3'"
n=$(lines)
commit "It is done"
bash_ok "$TMP" "git -c core.pager=cat -C $REPO commit -m 'it's done' && git -C $REPO commit --amend --no-edit"
check "-C after -c, an unquoted apostrophe, and one commit fact per HEAD" test "$(lines)" = "$((n + 1))"
bash_ok "$TMP" "git -C \$WT commit -F - <<'EOF'
yarn test
git push
EOF"
check "an unexpanded folder logs nothing, and a heredoc body is not run" \
  test "$(lines)" = "$((n + 1))"
n=$(lines)
bash_ok "$REPO" "grep -q x <<< x
yarn lint"
check "a here-string opens no heredoc, so the next line is run" has 'yarn lint exit 0'
bash_ok "$TMP" "cd \$WT && git push"
bash_fail "$TMP" "cd \"\$WT\" && git commit -m x" "Exit code 1"
check "a push or a failed commit from an unexpanded folder logs nothing" test "$(lines)" = "$((n + 1))"
bash_ok "$TMP" "cd \$WT && yarn test"
check "a test run from an unexpanded folder is logged under unknown" \
  sh -c "tail -1 '$BUF' | grep -q '\"repo\": \"unknown\", \"branch\": \"-\", \"kind\": \"check\", \"text\": \"yarn test exit 0\"'"
n=$(lines)
commit "First of two"
commit "Second of two"
bash_ok "$REPO" "git add a && git commit -m a && git add b && git commit -m b"
check "two commits in one call log two facts, oldest first" \
  sh -c "test \$(wc -l < '$BUF') -eq $((n + 2)) && tail -2 '$BUF' | head -1 | grep -q 'First of two' && tail -1 '$BUF' | grep -q 'Second of two'"
n=$(lines)
bash_ok "$REPO" "git push --dry-run origin HEAD"
bash_ok "$OTHER" "git commit -m x"
bash_ok "$REPO" "ls -la && cat README.md"
bash_ok "$TMP" "git commit -m x"
check "other owners, other commands and non-repos log nothing" test "$(lines)" = "$n"

python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__plugin_design_atlassian__createJiraIssue","tool_input":{"projectKey":"ZZ","summary":"s"},"tool_response":[{"type":"text","text":"{\"key\":\"ZZ-12\",\"id\":\"1\"}"}]}))' | capture
check "a Jira ticket creation logs its key" has '"text": "ZZ-12 created"'
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__plugin_design_atlassian__createJiraIssue","tool_input":{"projectKey":"ZZ","summary":"s"},"tool_response":[{"type":"text","text":"{\"fields\":{\"parent\":{\"key\":\"ZZ-1\"}},\"key\":\"ZZ-13\"}"}]}))' | capture
check "a created ticket is its top-level key, not its parent" has '"text": "ZZ-13 created"'
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Atlassian_Rovo__transitionJiraIssue","tool_input":{"issueIdOrKey":"ZZ-12","transition":{"id":"31"}},"tool_response":"ok"}))' | capture
check "a Jira transition logs the key and transition" has 'ZZ-12 transitioned (transition 31)'
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"bfe390f9-d819-4a93-9050-ffd5eee1b80a"},"ref":{"object":"node","id":"c9598c30-9a9c"},"payload":{}},"tool_response":[{"type":"text","text":"{\"verdict\":\"allow\",\"rev\":5}"}]}))' | capture
check "a write to a D1 doc logs the doc and node" has 'tech spec write (node c9598c30-9a9c)'
n=$(lines)
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"bfe390f9-d819-4a93-9050-ffd5eee1b80a"},"ref":{"object":"node","id":"2eb208d1-69b7"},"payload":{}},"tool_response":[{"type":"text","text":"{\"verdict\":\"allow\",\"rev\":6}"}]}))' | capture
check "a render of the Tickets tab or a chart is not logged" test "$(lines)" = "$n"
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"another-doc"},"ref":{"object":"node","id":"x"}},"tool_response":"{\"verdict\":\"allow\"}"}))' | capture
echo 'not json' | capture
check "another doc and bad input log nothing" test "$(lines)" = "$n"
check "bad input went to errors.log" grep -q capture "$BNAAS_JOURNAL_DIR/errors.log"

(cd "$REPO" && python3 -I "$HOOKS/journal" note --decision --tasks 22b "Pivots move to the journal doc")
check "journal note --decision logs a decision" has '"kind": "decision", "text": "Pivots move to the journal doc | tasks: 22b"'

stop() { # <json> : run the Stop hook, output in $TMP/out
  echo "$1" | "$HOOKS/journal-flush.sh" > "$TMP/out"
}
blocks() { reason > /dev/null; }
reason() { python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); assert d["decision"]=="block"; print(d["reason"])' "$TMP/out"; }

stop '{"hook_event_name":"Stop","session_id":"s1","stop_hook_active":true}'
check "stop_hook_active skips the flush" test ! -s "$TMP/out" -a -s "$BUF"
stop '{"hook_event_name":"SubagentStop","session_id":"s1"}'
check "a subagent stop asks nothing" test ! -s "$TMP/out" -a -s "$BUF"

# A broken line is dropped, and the pending facts of a quiet session are taken over.
echo 'not json' >> "$BUF"
echo '{"repo": "backbeat", "branch": "b", "kind": "push", "text": "push from a quiet session"}' > "$BNAAS_JOURNAL_DIR/pending.old.jsonl"
touch -t 202001010000 "$BNAAS_JOURNAL_DIR/pending.old.jsonl"
echo '{"repo": "backbeat", "branch": "b", "kind": "push", "text": "push from a live session"}' > "$BNAAS_JOURNAL_DIR/pending.live.jsonl"
n=$(($(lines) + 1))

stop '{"hook_event_name":"Stop","session_id":"s1","stop_hook_active":false}'
check "the Stop hook blocks once with a reason" blocks
reason > "$TMP/reason" 2>/dev/null
check "the reason names the journal doc and the connector" \
  grep -q 'Log section of the BNaaS D1 journal doc (https://claude.ai/artifact/Bgoru7knkQrfMGVAbDoGEM) through the Claude Docs connector' "$TMP/reason"
check "the reason puts each fact under its UTC day heading" grep -q 'its UTC day heading' "$TMP/reason"
check "the reason gives the flushed command of the session" grep -q "journal flushed s1\`" "$TMP/reason"
check "a dated heading, then one bullet per fact, repo and branch first" \
  sh -c "grep -q '^### 20[0-9-]*\$' '$TMP/reason' && grep -q '^- backbeat improvement/topic-journal: commit ' '$TMP/reason'"
check "every fact is shown, minus the broken line" test "$(grep -c '^- ' "$TMP/reason")" = "$((n - 1))"
check "the facts of a quiet session are taken over" grep -q 'push from a quiet session' "$TMP/reason"
check "the facts of a live session are left to it" sh -c "! grep -q 'push from a live session' '$TMP/reason'"
check "the buffer is empty and the facts wait as pending" test ! -s "$BUF" -a -s "$BNAAS_JOURNAL_DIR/pending.s1.jsonl"

# The write failed: the next Stop shows the same facts again, plus new ones.
pushed
bash_ok "$REPO" "git push origin HEAD"
stop '{"hook_event_name":"Stop","session_id":"s1"}'
reason > "$TMP/reason" 2>/dev/null
check "a failed write is retried at the next Stop" test "$(grep -c '^- ' "$TMP/reason")" = "$n"
python3 -I "$HOOKS/journal" flushed s1
check "journal flushed drops the pending facts" test ! -e "$BNAAS_JOURNAL_DIR/pending.s1.jsonl"
python3 -I "$HOOKS/journal" note "a manual flush test"
python3 -I "$HOOKS/journal" flush > "$TMP/manual"
check "a manual flush has a pending file of its own" \
  sh -c "grep -q 'flushed manual-[0-9]*\`' '$TMP/manual' && ls '$BNAAS_JOURNAL_DIR' | grep -q '^pending.manual-[0-9]*.jsonl$'"
python3 -I "$HOOKS/journal" flushed "$(grep -o 'manual-[0-9]*' "$TMP/manual" | head -1)"
stop '{"hook_event_name":"Stop","session_id":"s1"}'
check "nothing waits, so the Stop asks nothing" test ! -s "$TMP/out"

python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"Bgoru7knkQrfMGVAbDoGEM"},"ref":{"object":"node","id":"x"}},"tool_response":"{\"verdict\":\"allow\"}"}))' | capture
stop '{"hook_event_name":"Stop","session_id":"s1"}'
check "the journal doc's own writes never start another Stop" test ! -s "$TMP/out"

# A long buffer is shown 200 facts at a time.
python3 -c 'import json; [print(json.dumps({"repo":"backbeat","branch":"b","kind":"note","text":"x"*290})) for _ in range(300)]' > "$BUF"
stop '{"hook_event_name":"Stop","session_id":"s2"}'
reason > "$TMP/reason" 2>/dev/null
check "a long buffer is cut at 200 facts" test "$(grep -c '^- backbeat b: note: ' "$TMP/reason")" = 200
check "the rest stays in the buffer" test "$(lines)" = 100

# journal tail reads the Log section of a doc export.
cat > "$TMP/doc.md" <<'EOF'
# BNaaS D1 journal

## Current state

- not a log bullet

## Log

### 2026-10-23

- backbeat b: commit 1234567 Add the journal
- doc -: tech spec write

### 2026-10-22

- vault b: push
EOF
python3 -I "$HOOKS/journal" tail --new --doc "$TMP/doc.md" > "$TMP/out"
check "tail --new prints every Log bullet on a first run" test "$(grep -c '^- ' "$TMP/out")" = 3
check "tail skips the other sections" sh -c "! grep -q 'not a log bullet' '$TMP/out'"
python3 -I "$HOOKS/journal" mark-read > /dev/null
printf '%s\n' '- backbeat b: test exit 0' >> "$TMP/doc.md"
python3 -I "$HOOKS/journal" tail --new --doc "$TMP/doc.md" > "$TMP/out"
check "after mark-read, tail --new prints only the new bullets" \
  sh -c "test \$(grep -c '^- ' '$TMP/out') = 1 && grep -q 'test exit 0' '$TMP/out'"
python3 -I "$HOOKS/journal" mark-read > /dev/null
printf '%s\n' '- backbeat b: test exit 0' >> "$TMP/doc.md"
python3 -I "$HOOKS/journal" tail --new --doc "$TMP/doc.md" > "$TMP/out"
check "a fact repeated word for word on one day is new again" \
  sh -c "test \$(grep -c '^- ' '$TMP/out') = 1 && grep -q 'test exit 0' '$TMP/out'"
python3 -I "$HOOKS/journal" mark-read > /dev/null
check "mark-read keeps one digest per Log bullet, not a growing list" \
  test "$(wc -l < "$BNAAS_JOURNAL_DIR/last-read" | tr -d ' ')" = 5
check "every hook exits 0 on garbage" sh -c "echo '{' | '$HOOKS/journal-flush.sh' && echo '{' | '$HOOKS/journal-capture.sh'"

[ $FAIL = 0 ] && echo "all journal tests passed" || echo "some journal tests failed"
exit $FAIL
