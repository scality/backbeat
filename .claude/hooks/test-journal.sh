#!/bin/sh
# Test the D1 journal hooks: feed them sample hook input and check the
# buffer, the Stop request and the reading of a doc export. Nothing is
# written to the doc. Run: .claude/hooks/test-journal.sh
HOOKS=$(cd "$(dirname "$0")" && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
export BNAAS_HOME="$TMP"
BUF="$BNAAS_HOME/journal/buffer.jsonl"
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
SHA=$(git -C "$REPO" rev-parse HEAD)
# sha1sum on Linux, where the Perl shasum may be off PATH; shasum on macOS.
if command -v sha1sum > /dev/null; then SHA1=sha1sum; else SHA1=shasum; fi
ID=$(printf 'commit\nbackbeat\n%s' "$SHA" | $SHA1 | cut -c1-10)
check "a commit's id comes from its repository and sha" has "\"id\": \"$ID\""
check "a commit's time is its commit time" \
  has "\"t\": \"$(TZ=UTC git -C "$REPO" log -1 --date=format-local:%Y-%m-%dT%H:%M:%S --format=%cd)+00:00\""
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
bash_ok "$TMP" "gh pr ready https://github.com/scality/backbeat/pull/77"
sleep 1
bash_ok "$TMP" "gh pr ready https://github.com/scality/backbeat/pull/77"
check "a repeated action on the same PR is a second fact" test "$(lines)" = "$((n + 2))"
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
bash_fail "$REPO" 'PATH="$(ls -d ~/.local/share/nvm/v22.* | sort -V | tail -n 1)/bin:$PATH" yarn test' "Exit code 2"
check "after an nvm.fish PATH prefix, a failed check keeps its exit code" sh -c "tail -1 '$BUF' | grep -q 'yarn test exit 2'"
# A check inside a shell's -c string is logged as if it ran outside it.
bash_fail "$REPO" "fish -c 'nvm use 22; and yarn test'" "Exit code 4"
check "a check inside fish -c, after the nvm setup, keeps its exit code" sh -c "tail -1 '$BUF' | grep -q 'yarn test exit 4'"
bash_fail "$TMP" "bash -lc 'cd $REPO && yarn lint'" "Exit code 5"
check "a check inside bash -lc logs in the folder that its cd moved to" \
  sh -c "tail -1 '$BUF' | grep -q '\"repo\": \"backbeat\".*yarn lint exit 5'"
bash_ok "$REPO" "sh -c 'yarn lint:md' | tail -3"
check "a piped shell -c string logs its check with exit unknown" sh -c "tail -1 '$BUF' | grep -q 'yarn lint:md exit unknown'"
bash_fail "$REPO" "fish -c 'nvm use 22; and S3BACKEND=mem yarn mocha tests/x.js'" "Exit code 6"
check "a VAR=value prefix after fish's and is dropped too" sh -c "tail -1 '$BUF' | grep -q 'mocha exit 6'"
bash_fail "$REPO" "fish -c 'nvm use 22; and bash -c \"yarn test:nested\"'" "Exit code 8"
check "a shell after fish's and is unwrapped too" sh -c "tail -1 '$BUF' | grep -q 'yarn test:nested exit 8'"
bash_ok "$REPO" "bash -c 'cat <<EOF
not a command
EOF
yarn typecheck:heredoc'"
check "a check after a heredoc inside bash -c is logged" sh -c "tail -1 '$BUF' | grep -q 'yarn typecheck:heredoc exit 0'"
bash_ok "$REPO" "bash -c < x.sh && yarn test:after"
check "a shell with no string after -c stays one step, and the rest is read" sh -c "tail -1 '$BUF' | grep -q 'yarn test:after exit 0'"
n=$(lines)
bash_ok "$REPO" "echo \"fish -c 'yarn test'\""
check "a shell -c string that is only an argument logs nothing" test "$(lines)" = "$n"
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
# The Jira key holds the capture minute: retry when the minute turned.
for _ in 1 2; do
  m=$(date -u +%M); n=$(lines)
  python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Atlassian_Rovo__transitionJiraIssue","tool_input":{"issueIdOrKey":"ZZ-12","transition":{"id":"32"}},"tool_response":"ok"}))' | capture
  python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Atlassian_Rovo__transitionJiraIssue","tool_input":{"issueIdOrKey":"ZZ-12","transition":{"id":"32"}},"tool_response":"ok"}))' | capture
  [ "$(date -u +%M)" = "$m" ] && break
done
check "the same event captured twice in one minute waits once" test "$(lines)" = "$((n + 1))"
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"bfe390f9-d819-4a93-9050-ffd5eee1b80a"},"ref":{"object":"node","id":"c9598c30-9a9c"},"payload":{}},"tool_response":[{"type":"text","text":"{\"verdict\":\"allow\",\"rev\":5}"}]}))' | capture
check "a write to a D1 doc logs the doc and node" has 'tech spec write (node c9598c30-9a9c)'
n=$(lines)
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"bfe390f9-d819-4a93-9050-ffd5eee1b80a"},"ref":{"object":"node","id":"c9598c30-9a9c"}},"tool_response":"{\"verdict\":\"allow\",\"rev\":7}"}))' | capture
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"bfe390f9-d819-4a93-9050-ffd5eee1b80a"},"ref":{"object":"node","id":"c9598c30-9a9c"}},"tool_response":"{\"verdict\":\"allow\",\"rev\":7}"}))' | capture
check "a doc write is one event per rev" test "$(lines)" = "$((n + 1))"
n=$(lines)
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"bfe390f9-d819-4a93-9050-ffd5eee1b80a"},"ref":{"object":"node","id":"2eb208d1-69b7"},"payload":{}},"tool_response":[{"type":"text","text":"{\"verdict\":\"allow\",\"rev\":6}"}]}))' | capture
check "a render of the Tickets tab or a chart is not logged" test "$(lines)" = "$n"
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"another-doc"},"ref":{"object":"node","id":"x"}},"tool_response":"{\"verdict\":\"allow\"}"}))' | capture
echo 'not json' | capture
check "another doc and bad input log nothing" test "$(lines)" = "$n"
check "bad input went to errors.log" grep -q capture "$BNAAS_HOME/journal/errors.log"

(cd "$REPO" && python3 -I "$HOOKS/journal" note --decision --tasks 22b "Pivots move to the Journal tab")
check "journal note --decision logs a decision" has '"kind": "decision", "text": "Pivots move to the Journal tab | tasks: 22b"'
# A note over the 300 characters of a fact is refused, not cut.
n=$(lines)
long=$(python3 -c 'print("x" * 301)')
check "journal note refuses a note over 300 characters" \
  sh -c "cd '$REPO' && python3 -I '$HOOKS/journal' note '$long' 2> '$TMP/err'; test \$? -eq 2"
check "the refusal names the length and the limit" sh -c "grep -q 301 '$TMP/err' && grep -q 300 '$TMP/err'"
check "a refused note leaves the buffer as it was" test "$(lines)" = "$n"
half=$(python3 -c 'print("x" * 200)')
check "the limit counts the note and its --why together" \
  sh -c "cd '$REPO' && python3 -I '$HOOKS/journal' note '$half' --why '$half' 2> /dev/null; test \$? -eq 2"
near=$(python3 -c 'print("x" * 290)')
check "the limit counts the --tasks text too" \
  sh -c "cd '$REPO' && python3 -I '$HOOKS/journal' note '$near' --tasks 22b 2> /dev/null; test \$? -eq 2"
check "a note refused for its --why or --tasks leaves the buffer as it was" test "$(lines)" = "$n"
full=$(python3 -c 'print("y" * 300)')
(cd "$REPO" && python3 -I "$HOOKS/journal" note "$full")
check "a note of 300 characters lands whole" has "\"text\": \"$full\""
spaced=$(python3 -c 'print("xx  " * 100)')
check "a note is measured with its blanks folded" \
  sh -c "cd '$REPO' && python3 -I '$HOOKS/journal' note '$spaced' 2> /dev/null"
folded=$(python3 -c 'print(" ".join(["xx"] * 100))')
check "a note whose blanks fold under 300 lands whole" has "\"text\": \"$folded\""

stop() { # <json> : run the Stop hook, output in $TMP/out
  echo "$1" | "$HOOKS/journal-flush.sh" > "$TMP/out"
}
blocks() { reason > /dev/null; }
reason() { python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); assert d["decision"]=="block"; print(d["reason"])' "$TMP/out"; }

stop '{"hook_event_name":"Stop","session_id":"s1","stop_hook_active":true}'
check "stop_hook_active skips the flush" test ! -s "$TMP/out" -a -s "$BUF"
stop '{"hook_event_name":"SubagentStop","session_id":"s1"}'
check "a subagent stop asks nothing" test ! -s "$TMP/out" -a -s "$BUF"

# A broken line is dropped, and the batch of a quiet session is taken over.
echo 'not json' >> "$BUF"
echo '{"repo": "backbeat", "branch": "b", "kind": "push", "text": "push from a quiet session"}' > "$BNAAS_HOME/journal/pending.old.aaaa.jsonl"
touch -t 202001010000 "$BNAAS_HOME/journal/pending.old.aaaa.jsonl"
echo '{"repo": "backbeat", "branch": "b", "kind": "push", "text": "push from a live session"}' > "$BNAAS_HOME/journal/pending.live.bbbb.jsonl"
n=$(($(lines) + 1))

batch_of() { sed -n 's/.* wait in batch \([0-9a-f]*\):.*/\1/p' "$TMP/reason"; }
show() { python3 -I "$HOOKS/journal" batch "$1" > "$TMP/batch"; }
stop '{"hook_event_name":"Stop","session_id":"s1","stop_hook_active":false}'
check "the Stop hook blocks once with a reason" blocks
reason > "$TMP/reason" 2>/dev/null
B1=$(batch_of)
check "the reason names the batch and the background agent" \
  sh -c "test -n '$B1' && grep -q 'start the bnaas-journal agent in the background with that batch id, then go on' '$TMP/reason'"
check "the reason says what to do without the connector" grep -q "journal batch $B1\` prints them, then \`.*journal flushed $B1\`" "$TMP/reason"
check "the batch has a file of its own" test -s "$BNAAS_HOME/journal/pending.s1.$B1.jsonl"
show "$B1"
check "the batch names the Journal tab and its nodes" \
  grep -q 'Target: the Journal tab of https://claude.ai/artifact/QhKsCqtGNzaVice35nvip9 (doc bfe390f9-d819-4a93-9050-ffd5eee1b80a, tab file b251bb0a-d91f, body node 267e707b-5e7b)' "$TMP/batch"
check "one bullet per fact: UTC time, fact id, repo and branch" \
  grep -Eq '^- 20[0-9]{2}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}Z [0-9a-f]{10} backbeat improvement/topic-journal: commit ' "$TMP/batch"
check "the bullets are oldest first" sh -c "sed '/^Shown before/,\$d' '$TMP/batch' | grep '^- ' | cut -c3-19 | sort -c"
check "every fact is in the batch, minus the broken line" test "$(grep -c '^- ' "$TMP/batch")" = "$((n - 1))"
check "the batch of a quiet session is taken over" \
  sh -c "sed -n '/^Shown before/,\$p' '$TMP/batch' | grep -q 'push from a quiet session' && test ! -e '$BNAAS_HOME/journal/pending.old.aaaa.jsonl'"
check "first-time facts are appended without a read" \
  sh -c "sed -n '/^New, to append without a read/,/^Shown before/p' '$TMP/batch' | grep -q '^- .* backbeat improvement/topic-journal: commit '"
check "the batch of a live session is left to it" \
  sh -c "! grep -q 'push from a live session' '$TMP/batch' && test -s '$BNAAS_HOME/journal/pending.live.bbbb.jsonl'"
check "the buffer is empty" test ! -s "$BUF"
grep '^- ' "$TMP/batch" | cut -d' ' -f3 | sort > "$TMP/ids1"

# A second Stop of the same session: a new batch with only the new facts,
# while the agent may still be appending the first one.
pushed
bash_ok "$REPO" "git push origin HEAD"
stop '{"hook_event_name":"Stop","session_id":"s1"}'
reason > "$TMP/reason" 2>/dev/null
B2=$(batch_of)
show "$B2"
check "two batches in one session: the second holds only the new push" \
  sh -c "test '$B2' != '$B1' && test \$(grep -c '^- ' '$TMP/batch') = 1 && grep -q '^- .*: push$' '$TMP/batch' && test -s '$BNAAS_HOME/journal/pending.s1.$B1.jsonl'"
python3 -I "$HOOKS/journal" flushed "$B2"
check "journal flushed removes exactly that batch" \
  sh -c "test ! -e '$BNAAS_HOME/journal/pending.s1.$B2.jsonl' && test -s '$BNAAS_HOME/journal/pending.s1.$B1.jsonl'"

# The first batch was never flushed: after an hour, the next batch takes
# it over, its facts marked as shown before, with the same ids.
touch -t 202001010000 "$BNAAS_HOME/journal/pending.s1.$B1.jsonl"
python3 -I "$HOOKS/journal" note "after the takeover"
stop '{"hook_event_name":"Stop","session_id":"s3"}'
reason > "$TMP/reason" 2>/dev/null
B3=$(batch_of)
show "$B3"
takeover() {
  sed -n '/^Shown before/,$p' "$TMP/batch" | grep '^- ' | cut -d' ' -f3 | sort > "$TMP/ids3" &&
    test -s "$TMP/ids1" && test -z "$(comm -23 "$TMP/ids1" "$TMP/ids3")" &&
    sed -n '/^New, to append without a read/,/^Shown before/p' "$TMP/batch" | grep -q 'after the takeover' &&
    test ! -e "$BNAAS_HOME/journal/pending.s1.$B1.jsonl"
}
check "a stale batch is taken over as shown before, with the same ids" takeover
python3 -I "$HOOKS/journal" flushed "$B3"
python3 -I "$HOOKS/journal" note "a manual flush test"
python3 -I "$HOOKS/journal" flush > "$TMP/reason"
BM=$(batch_of)
check "a manual flush takes a batch of its own" \
  sh -c "test -n '$BM' && ls '$BNAAS_HOME/journal' | grep -q '^pending.manual-[0-9]*.$BM.jsonl$'"
python3 -I "$HOOKS/journal" flushed "$BM"
python3 -I "$HOOKS/journal" note "taken by hand"
BT=$(python3 -I "$HOOKS/journal" take s4)
check "journal take prints the id of a new batch" test -s "$BNAAS_HOME/journal/pending.s4.$BT.jsonl"
python3 -I "$HOOKS/journal" flushed "$BT"
python3 -I "$HOOKS/journal" note "for the practice tab"
BP=$(python3 -I "$HOOKS/journal" take s5)
practice() {
  BNAAS_JOURNAL_CONFIG="$HOOKS/../skills/bnaas-journal/evals/fixtures/journal-practice.json" \
    python3 -I "$HOOKS/journal" batch "$BP" | grep -q 'Target: the Journal practice tab .*body node f093d6dc-8533'
}
check "BNAAS_JOURNAL_CONFIG points a batch at the practice tab" practice
python3 -I "$HOOKS/journal" flushed "$BP"
# A fact with no time takes the time of its file, once; the facts sort by
# their parsed time, not by the text of their time.
echo '{"repo": "backbeat", "branch": "b", "kind": "note", "text": "no time of its own"}' > "$BUF"
TZ=UTC touch -t 202601020304 "$BUF"
echo '{"repo": "backbeat", "branch": "b", "kind": "note", "text": "ten at plus two", "t": "2026-01-02T10:00:00+02:00"}' >> "$BUF"
echo '{"repo": "backbeat", "branch": "b", "kind": "note", "text": "nine at zero", "t": "2026-01-02T09:00:00+00:00"}' >> "$BUF"
TZ=UTC touch -t 202601020304 "$BUF"
BS=$(python3 -I "$HOOKS/journal" take s6)
show "$BS"
check "a fact with no time takes the time of its file" grep -q '^- 2026-01-02T03:04Z .*no time of its own' "$TMP/batch"
sorted_by_time() {
  test "$(grep '^- ' "$TMP/batch" | sed 's/.*: note: //' | tr '\n' '|')" = "no time of its own|ten at plus two|nine at zero|"
}
check "the facts sort by their parsed time" sorted_by_time
grep '^- ' "$TMP/batch" | cut -d' ' -f2 > "$TMP/t1"
touch "$BNAAS_HOME/journal"/pending.s6.$BS.jsonl
sleep 1
show "$BS"
check "the time of a fact does not change on a later read" sh -c "grep '^- ' '$TMP/batch' | cut -d' ' -f2 | cmp -s - '$TMP/t1'"
python3 -I "$HOOKS/journal" flushed "$BS"
# Two batches wait at once: each prints only its own facts.
echo '{"repo": "cloudserver", "branch": "z", "kind": "push", "text": "first batch fact", "t": "2026-01-02T11:00:00+00:00"}' > "$BNAAS_HOME/journal/pending.eval.c1c1c1.jsonl"
echo '{"repo": "cloudserver", "branch": "z", "kind": "push", "text": "second batch fact", "t": "2026-01-02T11:10:00+00:00"}' > "$BNAAS_HOME/journal/pending.eval.c2c2c2.jsonl"
own_facts() {
  python3 -I "$HOOKS/journal" batch c1c1c1 > "$TMP/b1" && python3 -I "$HOOKS/journal" batch c2c2c2 > "$TMP/b2" &&
    test "$(grep -c '^- ' "$TMP/b1")" = 1 && grep -q 'first batch fact' "$TMP/b1" &&
    test "$(grep -c '^- ' "$TMP/b2")" = 1 && grep -q 'second batch fact' "$TMP/b2"
}
check "two batch files: each batch prints only its own facts" own_facts
python3 -I "$HOOKS/journal" flushed c1c1c1
check "flushing one batch leaves the other" \
  sh -c "test ! -e '$BNAAS_HOME/journal/pending.eval.c1c1c1.jsonl' && test -s '$BNAAS_HOME/journal/pending.eval.c2c2c2.jsonl'"
python3 -I "$HOOKS/journal" flushed c2c2c2
(cd "$REPO" && python3 -I "$HOOKS/journal" note --decision "Options after the sentence" --why "the skill writes them so" --tasks 7)
check "journal note takes its options after the sentence too" \
  has '"kind": "decision", "text": "Options after the sentence | why: the skill writes them so | tasks: 7"'
: > "$BUF"
check "journal batch fails on an unknown batch" sh -c "! python3 -I '$HOOKS/journal' batch nope 2>/dev/null"
rm -f "$BNAAS_HOME/journal"/pending.live.*.jsonl
stop '{"hook_event_name":"Stop","session_id":"s1"}'
check "nothing waits, so the Stop asks nothing" test ! -s "$TMP/out"

python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"bfe390f9-d819-4a93-9050-ffd5eee1b80a"},"ref":{"object":"node","id":"267e707b-5e7b"}},"tool_response":"{\"verdict\":\"allow\"}"}))' | capture
stop '{"hook_event_name":"Stop","session_id":"s1"}'
check "the Journal tab's own writes never start another Stop" test ! -s "$TMP/out"
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"bfe390f9-d819-4a93-9050-ffd5eee1b80a"},"ref":{"object":"file","id":"b251bb0a-d91f"}},"tool_response":"{\"verdict\":\"allow\"}"}))' | capture
stop '{"hook_event_name":"Stop","session_id":"s1"}'
check "a write by the Journal tab file is not logged either" test ! -s "$TMP/out"
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__batch","tool_input":{"container":{"kind":"project","id":"bfe390f9-d819-4a93-9050-ffd5eee1b80a"},"batch":[{"op":"insert","content":"- 2026-10-28T09:00Z a1b2c3d4e5 backbeat b: push"}]},"tool_response":"{\"verdict\":\"allow\",\"rev\":9}"}))' | capture
stop '{"hook_event_name":"Stop","session_id":"s1"}'
check "a journal append with no node, by container alone, is not logged" test ! -s "$TMP/out"
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"bfe390f9-d819-4a93-9050-ffd5eee1b80a"},"ref":{"object":"node","id":"f093d6dc-8533"}},"tool_response":"{\"verdict\":\"allow\"}"}))' | capture
stop '{"hook_event_name":"Stop","session_id":"s1"}'
check "a write to the practice tab is not logged" test ! -s "$TMP/out"
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"Bgoru7knkQrfMGVAbDoGEM"},"ref":{"object":"node","id":"x"}},"tool_response":"{\"verdict\":\"allow\"}"}))' | capture
check "a stray write to the old journal doc is logged" grep -q 'old journal doc write (node x)' "$BUF"
: > "$BUF"

# A long buffer is shown 200 facts at a time.
python3 -c 'import json; [print(json.dumps({"repo":"backbeat","branch":"b","kind":"note","text":"x"*290})) for _ in range(300)]' > "$BUF"
stop '{"hook_event_name":"Stop","session_id":"s2"}'
reason > "$TMP/reason" 2>/dev/null
show "$(batch_of)"
check "a long buffer is cut at 200 facts" test "$(grep -Ec '^- [^ ]+ [0-9a-f]{10} backbeat b: note: ' "$TMP/batch")" = 200
check "the reason says that more facts wait" grep -q '100 more facts wait' "$TMP/reason"
check "the rest stays in the buffer" test "$(lines)" = 100

# journal tail reads the Log section of an export. The dates are
# relative to today: tail --new looks back 14 days.
D0=$(date -u +%Y-%m-%d)
utc_days_ago() { python3 -c 'import datetime, sys; print((datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=int(sys.argv[1]))).date())' "$1"; }
D1=$(utc_days_ago 1)
OLD=$(utc_days_ago 20)

# The old layout (the old journal doc): day headings, no time, no id.
cat > "$TMP/doc.md" <<EOF
# BNaaS D1 journal

## Current state

- not a log bullet

## Log

### $D0

- backbeat b: commit 1234567 Add the journal
- doc -: tech spec write

### $D1

- vault b: push
EOF
python3 -I "$HOOKS/journal" tail --new --doc "$TMP/doc.md" > "$TMP/out"
check "the old layout: tail --new prints every Log bullet on a first run" test "$(grep -c '^- ' "$TMP/out")" = 3
check "tail skips the other sections" sh -c "! grep -q 'not a log bullet' '$TMP/out'"
python3 -I "$HOOKS/journal" mark-read > /dev/null
printf '%s\n' '- backbeat b: test exit 0' >> "$TMP/doc.md"
python3 -I "$HOOKS/journal" tail --new --doc "$TMP/doc.md" > "$TMP/out"
check "the old layout: after mark-read, tail --new prints only the new bullets" \
  sh -c "test \$(grep -c '^- ' '$TMP/out') = 1 && grep -q 'test exit 0' '$TMP/out'"
python3 -I "$HOOKS/journal" mark-read > /dev/null
printf '%s\n' '- backbeat b: test exit 0' >> "$TMP/doc.md"
python3 -I "$HOOKS/journal" tail --new --doc "$TMP/doc.md" > "$TMP/out"
check "the old layout: a fact repeated word for word on one day is new again" \
  sh -c "test \$(grep -c '^- ' '$TMP/out') = 1 && grep -q 'test exit 0' '$TMP/out'"
python3 -I "$HOOKS/journal" mark-read > /dev/null
check "the read set holds each id once, with its time" \
  sh -c "test \$(wc -l < '$BNAAS_HOME/journal/last-read' | tr -d ' ') = 5 && ! grep -qv ' ' '$BNAAS_HOME/journal/last-read'"
printf '\n%s\n\n%s\n' '### Earlier' '- vault b: history under no date' >> "$TMP/doc.md"
python3 -I "$HOOKS/journal" tail --new --doc "$TMP/doc.md" > "$TMP/out"
check "the old layout: a bullet under a heading that is no date is never new" test "$(grep -c '^- ' "$TMP/out")" = 0

# The Journal tab: one append-only list, each bullet with its time and
# fact id. Sessions append at once, so the order in the tab is not the
# order in time, and a session that died before `journal flushed` leaves
# a fact twice.
cat > "$TMP/tab.md" <<EOF
# Journal

## Current state

- not a log bullet

## Pivots

## Log

Newest facts at the end.

- ${D1}T09:00Z a1b2c3d4e5 backbeat b: commit 1234567 Add the journal
- ${D1}T11:30Z e5f6a7b8c9 vault b: push
- ${OLD}T08:00Z 9999999999 vault b: an old fact
EOF
rm -f "$BNAAS_HOME/journal/last-read"
python3 -I "$HOOKS/journal" tail --new --doc "$TMP/tab.md" > "$TMP/out"
check "tail --new reads the bullets of the tab" test "$(grep -c '^- ' "$TMP/out")" = 2
check "a fact older than 14 days is not new" sh -c "! grep -q 'an old fact' '$TMP/out'"
python3 -I "$HOOKS/journal" mark-read > /dev/null
check "the read set keeps no id older than 14 days" \
  sh -c "test -s '$BNAAS_HOME/journal/last-read' && ! grep -q '^9999999999 ' '$BNAAS_HOME/journal/last-read'"
printf '%s\n' "- ${D0}T00:10Z 0f0f0f0f0f backbeat b: test exit 0" \
  "- ${D1}T10:05Z 1a1a1a1a1a cloudserver b: push" \
  "- ${D1}T11:30Z e5f6a7b8c9 vault b: push" >> "$TMP/tab.md"
python3 -I "$HOOKS/journal" tail --new --doc "$TMP/tab.md" > "$TMP/out"
check "appends in any order are new, newest first by time" \
  sh -c "test \$(grep -c '^- ' '$TMP/out') = 2 && grep '^- ' '$TMP/out' | head -1 | grep -q 'test exit 0'"
check "a repeated fact id is read once" sh -c "! grep -q 'vault b: push' '$TMP/out'"
python3 -I "$HOOKS/journal" tail 10 --doc "$TMP/tab.md" > "$TMP/out"
check "tail lists each fact once, newest first" \
  sh -c "test \$(grep -c '^- ' '$TMP/out') = 5 && grep '^- ' '$TMP/out' | tail -1 | grep -q 'an old fact'"
python3 -I "$HOOKS/journal" mark-read > /dev/null
python3 -I "$HOOKS/journal" tail --new --doc "$TMP/tab.md" > "$TMP/out"
check "after mark-read, nothing is new" test "$(grep -c '^- ' "$TMP/out")" = 0
printf '%s\n' "- ${D0}T00:20Z 2c2c2c2c2c backbeat improvement/topic\\_x: push" \
  "- ${D0}T00:20Z 2c2c2c2c2c backbeat improvement/topic\\_x: push" \
  "- a bullet with no time or id" >> "$TMP/tab.md"
python3 -I "$HOOKS/journal" tail --new --doc "$TMP/tab.md" > "$TMP/out" 2> "$TMP/err"
check "a new fact appended twice is new once" test "$(grep -c '2c2c\|topic_x' "$TMP/out")" = 1
check "tail unescapes the Markdown of the export" grep -q 'improvement/topic_x: push' "$TMP/out"
check "tail says how many Log bullets it could not read" grep -q '1 Log bullets could not be read' "$TMP/err"
python3 -I "$HOOKS/journal" tail 0 --doc "$TMP/tab.md" > "$TMP/out" 2> /dev/null
check "tail 0 prints nothing" test "$(grep -c '^- ' "$TMP/out")" = 0
printf '%s\n' "- ${D0}T00:30Z 3d3d3d3d backbeat b: an id of 8 hex" >> "$TMP/tab.md"
python3 -I "$HOOKS/journal" tail --new --doc "$TMP/tab.md" > "$TMP/out" 2> /dev/null
check "a bullet with an id of 8 hex still reads" grep -q 'an id of 8 hex' "$TMP/out"
check "every hook exits 0 on garbage" sh -c "echo '{' | '$HOOKS/journal-flush.sh' && echo '{' | '$HOOKS/journal-capture.sh'"

# The local data folder is $BNAAS_HOME, absolute, which each developer
# sets in their shell profile. Nothing else: not the folder where Claude
# Code started, and never the home folder.
bare() { env -u BNAAS_HOME HOME="$TMP/home" CLAUDE_PROJECT_DIR="$TMP/proj" "$@"; }
(cd "$REPO" && bare BNAAS_HOME="$TMP/data" python3 -I "$HOOKS/journal" note "a note under BNAAS_HOME" 2> /dev/null)
check "BNAAS_HOME holds the journal" grep -q 'under BNAAS_HOME' "$TMP/data/journal/buffer.jsonl"
(cd "$REPO" && bare BNAAS_HOME=relative python3 -I "$HOOKS/journal" note "a note with a relative home" > /dev/null 2> "$TMP/err")
check "a relative BNAAS_HOME counts as unset" test $? -ne 0
(cd "$REPO" && bare python3 -I "$HOOKS/journal" note "a note with no folder" > /dev/null 2> "$TMP/err")
check "with no folder, the journal stops" test $? -ne 0
check "with no folder, the journal says why" grep -q 'BNAAS_HOME' "$TMP/err"
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__x__createJiraIssue","tool_input":{"projectKey":"ZZ","summary":"s"},"tool_response":[{"type":"text","text":"{\"key\":\"ZZ-13\",\"id\":\"1\"}"}]}))' | (cd "$REPO" && bare BNAAS_HOME="$TMP/data5" "$HOOKS/journal-capture.sh")
check "the capture hook logs under BNAAS_HOME" grep -q 'ZZ-13 created' "$TMP/data5/journal/buffer.jsonl"
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__x__createJiraIssue","tool_input":{"projectKey":"ZZ","summary":"s"},"tool_response":[{"type":"text","text":"{\"key\":\"ZZ-14\",\"id\":\"1\"}"}]}))' | (cd "$REPO" && bare "$HOOKS/journal-capture.sh") && echo '{"hook_event_name":"Stop","session_id":"nf"}' | (cd "$REPO" && bare "$HOOKS/journal-flush.sh") > /dev/null
check "every hook exits 0 with no folder" test $? -eq 0
check "with no folder, nothing lands anywhere" sh -c "test ! -e '$REPO/.bnaas' && test ! -e '$REPO/relative' && test ! -e '$TMP/proj/.bnaas' && test ! -e '$TMP/home/.bnaas'"

# The SessionStart hook checks BNAAS_HOME and never sets it.
(cd "$TMP" && bare BNAAS_HOME="$TMP/data7" "$HOOKS/bnaas-home.sh" < /dev/null > "$TMP/out")
check "the session hook creates the folder and its .gitignore" grep -qx '\*' "$TMP/data7/.gitignore"
check "the session hook is quiet when BNAAS_HOME is set" test ! -s "$TMP/out"
(cd "$TMP" && bare "$HOOKS/bnaas-home.sh" < /dev/null > "$TMP/out")
check "the session hook exits 0 without BNAAS_HOME" test $? -eq 0
check "the session hook creates nothing without BNAAS_HOME" sh -c "test ! -e '$TMP/proj/.bnaas' && test ! -e '$TMP/home/.bnaas'"
check "the session hook warns without BNAAS_HOME" grep -q 'BNAAS_HOME is not set' "$TMP/out"
mkdir -p "$TMP/repo9" && (cd "$TMP" && bare BNAAS_HOME="$TMP/repo9" "$HOOKS/bnaas-home.sh" < /dev/null)
check "the session hook adds no .gitignore to a folder that existed" test ! -e "$TMP/repo9/.gitignore"
check "the session hook warns when the folder cannot be made" sh -c "cd '$TMP' && BNAAS_HOME=/dev/null/bnaas '$HOOKS/bnaas-home.sh' < /dev/null | grep -q 'cannot be created'"
check "the session hook warns with a relative BNAAS_HOME" sh -c "cd '$TMP' && BNAAS_HOME=relative '$HOOKS/bnaas-home.sh' < /dev/null | grep -q 'BNAAS_HOME is not set' && test ! -e '$TMP/relative'"

[ $FAIL = 0 ] && echo "all journal tests passed" || echo "some journal tests failed"
exit $FAIL
