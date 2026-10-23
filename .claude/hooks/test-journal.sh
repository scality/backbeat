#!/bin/sh
# Test the D1 journal hooks: feed them sample hook input and check the
# buffer. Nothing is posted: the flush runs in dry run, and a fake gh
# stands in for the failure case. Run: .claude/hooks/test-journal.sh
HOOKS=$(cd "$(dirname "$0")" && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
export BNAAS_JOURNAL_DIR="$TMP/journal" BNAAS_JOURNAL_DRY_RUN=1
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
bash_fail() { # <cwd> <command> <error>
  python3 -c 'import json,sys; print(json.dumps({"hook_event_name":"PostToolUseFailure","tool_name":"Bash","cwd":sys.argv[1],"tool_input":{"command":sys.argv[2]},"error":sys.argv[3]}))' "$1" "$2" "$3" | capture
}

# A scality clone with a branch and a commit, and a clone of another owner.
REPO="$TMP/backbeat"
git init -q -b improvement/topic-journal "$REPO"
git -C "$REPO" remote add origin git@github.com:scality/backbeat.git
git -C "$REPO" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m "Add the journal sample"
OTHER="$TMP/personal"
git init -q -b main "$OTHER"
git -C "$OTHER" remote add origin git@github.com:someone/personal.git
git -C "$OTHER" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m "Personal"

bash_ok "$TMP" "cd $REPO && git commit -m 'secret=hunter2; more'"
check "a commit after cd logs the repo, branch, sha and subject" \
  has '"repo": "backbeat", "branch": "improvement/topic-journal", "kind": "commit", "text": "commit [0-9a-f]* Add the journal sample"'
check "the command text is never logged" sh -c "! grep -q hunter2 '$BUF'"
bash_ok "$TMP" "git -C $REPO push -u origin HEAD"
check "a push with git -C logs the push" has '"kind": "push"'
bash_ok "$REPO" "gh pr create --draft --title t --body-file b" "https://github.com/scality/backbeat/pull/42"
check "gh pr create logs the PR link from the output" has 'PR create https://github.com/scality/backbeat/pull/42'
bash_fail "$REPO" "yarn test 2>&1" "Exit code 1
lots of output"
check "a failing test logs its exit code" has 'yarn test exit 1'
bash_ok "$REPO" "yarn lint 2>&1 | tail -5"
check "a check piped into another command logs exit unknown" has 'yarn lint exit unknown'
bash_ok "$REPO" "S3BACKEND=mem yarn mocha tests/unit/x.js --exit"
check "a passing yarn mocha run logs exit 0" has "mocha exit 0"
n=$(lines)
bash_ok "$TMP" "git -c core.pager=cat -C $REPO commit -m 'it's done' && git -C $REPO commit --amend --no-edit"
check "-C after -c, an unquoted apostrophe, and one commit fact per HEAD" test "$(lines)" = "$((n + 1))"
bash_ok "$TMP" "git -C \$WT commit -F - <<'EOF'
yarn test
git push
EOF"
check "an unexpanded folder logs the kind only, and a heredoc body is not run" \
  sh -c "tail -1 '$BUF' | grep -q '\"repo\": \"unknown\", \"branch\": \"-\", \"kind\": \"commit\", \"text\": \"commit\"' && test \$(wc -l < '$BUF') -eq $((n + 2))"
n=$(lines)
bash_ok "$REPO" "git push --dry-run origin HEAD"
bash_ok "$OTHER" "git commit -m x"
bash_ok "$REPO" "ls -la && cat README.md"
bash_ok "$TMP" "git commit -m x"
check "other owners, other commands and non-repos log nothing" test "$(lines)" = "$n"

python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__plugin_design_atlassian__createJiraIssue","tool_input":{"projectKey":"ZZ","summary":"s"},"tool_response":[{"type":"text","text":"{\"key\":\"ZZ-12\",\"id\":\"1\"}"}]}))' | capture
check "a Jira ticket creation logs its key" has '"text": "ZZ-12 created"'
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Atlassian_Rovo__transitionJiraIssue","tool_input":{"issueIdOrKey":"ZZ-12","transition":{"id":"31"}},"tool_response":"ok"}))' | capture
check "a Jira transition logs the key and transition" has 'ZZ-12 transitioned (transition 31)'
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"568a45b7-4c63-45e7-bc63-13fc847dfc92"},"ref":{"object":"node","id":"c9598c30-9a9c"},"payload":{}},"tool_response":[{"type":"text","text":"{\"verdict\":\"allow\",\"rev\":5}"}]}))' | capture
check "a write to a D1 doc logs the doc and node" has 'journal doc write (node c9598c30-9a9c)'
n=$(lines)
python3 -c 'import json; print(json.dumps({"hook_event_name":"PostToolUse","tool_name":"mcp__claude_ai_Claude_Docs__update","tool_input":{"container":{"kind":"project","id":"another-doc"},"ref":{"object":"node","id":"x"}},"tool_response":"{\"verdict\":\"allow\"}"}))' | capture
echo 'not json' | capture
check "another doc and bad input log nothing" test "$(lines)" = "$n"
check "bad input went to errors.log" grep -q capture "$BNAAS_JOURNAL_DIR/errors.log"

(cd "$REPO" && python3 -I "$HOOKS/journal" note --decision --tasks 22b "Pivots move to the journal doc")
check "journal note --decision logs a decision" has '"kind": "decision", "text": "Pivots move to the journal doc | tasks: 22b"'

echo '{"hook_event_name":"Stop","stop_hook_active":true}' | "$HOOKS/journal-flush.sh" > "$TMP/out"
check "stop_hook_active skips the flush" test ! -s "$TMP/out" -a -s "$BUF"

# A broken line is dropped, and the buffer of a dead flush is taken back.
echo 'not json' >> "$BUF"
echo '{"repo": "backbeat", "branch": "b", "kind": "push", "text": "push from a dead flush"}' > "$BUF.999999.flushing"
n=$(($(lines) + 1))

# gh fails: the buffer stays.
mkdir "$TMP/bin" && printf '#!/bin/sh\nexit 1\n' > "$TMP/bin/gh" && chmod +x "$TMP/bin/gh"
echo '{"hook_event_name":"Stop"}' | BNAAS_JOURNAL_DRY_RUN=0 PATH="$TMP/bin:$PATH" "$HOOKS/journal-flush.sh"
check "a failed gh keeps every fact, minus the broken line" test "$(lines)" = "$((n - 1))"
check "the leftover of a dead flush is merged" has 'push from a dead flush'
check "no flushing file is left" sh -c "! ls '$BUF'.*.flushing 2>/dev/null"

echo '{"hook_event_name":"SubagentStop","stop_hook_active":false}' | "$HOOKS/journal-flush.sh" > "$TMP/out"
check "the dry-run flush prints a dated comment" grep -q '^### 20[0-9-]* [0-9:]* UTC$' "$TMP/out"
check "one bullet per fact, repo and branch first" \
  grep -q '^- backbeat improvement/topic-journal: commit ' "$TMP/out"
check "the flush empties the buffer" test ! -s "$BUF"

# A long buffer goes out in several comments, each under 60000 characters.
python3 -c 'import json; [print(json.dumps({"repo":"backbeat","branch":"b","kind":"note","text":"x"*290})) for _ in range(300)]' > "$BUF"
echo '{}' | "$HOOKS/journal-flush.sh" > "$TMP/out"
check "a long buffer is cut into several comments" test "$(grep -c '^### ' "$TMP/out")" -ge 2
check "every fact is posted once" test "$(grep -c '^- backbeat b: note: ' "$TMP/out")" = 300
check "every hook exits 0 on garbage" sh -c "echo '{' | '$HOOKS/journal-flush.sh' && echo '{' | '$HOOKS/journal-capture.sh'"

[ $FAIL = 0 ] && echo "all journal tests passed" || echo "some journal tests failed"
exit $FAIL
