#!/usr/bin/env bash
# The local review gate: run the review-pr skill of scality-skills, the one
# that the Code Review workflow runs on each PR, on the commits between
# <base> and <head> of the repository in the current folder. It posts
# nothing to GitHub and prints the reviews; "LGTM" means no issue.
#
#   local-review.sh <name> <base> <head> [--only <sha>...]
#   local-review.sh --self-test
#
# One run is one sample: a review of the whole diff skims what a review of
# one commit catches, and the other way round. So the gate runs, in
# parallel, one review of the whole range and, when the range has more
# than one commit, one review per commit. --only limits the per-commit
# runs to the commits named, for the next round after a fold; the whole
# range always runs. Each run costs about 1.5 to 5 USD.
#
# <name> labels the output (a branch or a PR). The skill's local mode
# reviews the commits of the current branch that no other local branch
# has, so each run has its own scratch clone that holds exactly two local
# branches: base at its base, and review at its head, checked out.
# It exits non-zero when a run could not run, not when a run finds issues.
# BNAAS_REVIEW_CLAUDE replaces the claude binary (the self-test uses it).
set -euo pipefail

# The scratch folder and the background runs, global so that the traps
# still see them.
tmp=
t=
pids=()
CLAUDE=${BNAAS_REVIEW_CLAUDE:-claude}
MODEL=claude-opus-5-5
ALLOWED='Read,Bash(git diff *),Bash(git log *),Bash(git show *),Bash(git rev-list *),Bash(git rev-parse *),Bash(git branch --show-current),Bash(gh repo view *),Bash(gh pr view *),Bash(gh pr diff *)'
DENIED='Bash(*post-inline-comment.sh*),Bash(*publish-summary.sh*),Bash(gh api *),Bash(gh pr review *),Bash(gh pr comment *)'

# Kill a process and everything it started.
kill_tree() {
  local c
  for c in $(pgrep -P "$1" 2>/dev/null); do kill_tree "$c"; done
  kill "$1" 2>/dev/null || true
}

# On exit or Ctrl-C: stop the runs still going, then remove the clones.
cleanup() {
  local p
  for p in ${pids[@]+"${pids[@]}"}; do kill_tree "$p"; done
  wait 2>/dev/null || true
  [ -n "$tmp" ] && rm -rf "$tmp"
}

usage() {
  echo "usage: local-review.sh <name> <base> <head> [--only <sha>...] | --self-test" >&2
  exit 2
}

# Make the scratch clone of the current repository in $1, with the two
# branches of the review, and point origin at the GitHub remote for gh.
# It runs on the left of `||`, where set -e is off: each step returns.
make_clone() {
  local dir=$1 base=$2 head=$3 src url
  src=$(git rev-parse --show-toplevel) || return 1
  git clone -q --shared --no-checkout "$src" "$dir" || return 1
  url=$(git -C "$src" remote get-url origin 2>/dev/null || true)
  if [ -n "$url" ]; then git -C "$dir" remote set-url origin "$url" || return 1; fi
  git -C "$dir" checkout -q -B review "$head" || return 1
  git -C "$dir" branch -f base "$base" || return 1
  git -C "$dir" for-each-ref --format='%(refname:short)' refs/heads |
    { grep -vxE 'base|review' || true; } |
    while read -r b; do git -C "$dir" branch -q -D "$b" || exit 1; done || return 1
}

# One review, in its own clone: claude's JSON goes to out.<i>, its exit
# code to rc.<i>. A clone that is not set up as planned fails the run, so
# that it never reviews the wrong range.
run_one() {
  local i=$1 base=$2 head=$3
  make_clone "$tmp/clone.$i" "$base" "$head" || return 1
  [ "$(git -C "$tmp/clone.$i" rev-list --count review --not --exclude=review --branches)" = \
    "$(git rev-list --count "$base..$head")" ] || {
    echo "local-review: the clone does not hold ${base:0:8}..${head:0:8}" >&2; return 1; }
  # --setting-sources user: the project settings would fire the journal
  # hooks inside the review and write to the journal.
  (cd "$tmp/clone.$i" && "$CLAUDE" -p "/review-pr" --model "$MODEL" \
    --output-format json --setting-sources user \
    --allowedTools "$ALLOWED" --disallowedTools "$DENIED" </dev/null)
}

review() {
  local name=$1 from=$2 to=$3 base head only=() runs=() c i
  base=$(git rev-parse --verify -q "$2^{commit}") || {
    echo "local-review: unknown base $2" >&2; exit 2; }
  head=$(git rev-parse --verify -q "$3^{commit}") || {
    echo "local-review: unknown head $3" >&2; exit 2; }
  # A base that moved on (an epic that took new merges) is no ancestor of
  # the branch any more: review from where the branch left it.
  base=$(git merge-base "$base" "$head") || {
    echo "local-review: $2 and $3 share no history" >&2; exit 2; }
  shift 3
  if [ $# -gt 0 ]; then
    [ "$1" = --only ] && [ $# -gt 1 ] || usage
    shift
    for c in "$@"; do
      c=$(git rev-parse --verify -q "$c^{commit}") || {
        echo "local-review: unknown commit $c" >&2; exit 2; }
      git merge-base --is-ancestor "$base" "$c" && git merge-base --is-ancestor "$c" "$head" &&
        [ "$c" != "$base" ] || {
        echo "local-review: ${c:0:8} is not between $from and $to" >&2; exit 2; }
      only+=("$c")
    done
  fi
  # The skill counts the commits and diffs HEAD~<count>, which only holds
  # for a straight line of commits.
  local commits
  commits=$(git rev-list --reverse "$base..$head")
  [ -n "$commits" ] || {
    echo "local-review: no commit between $from and $to" >&2; exit 1; }
  [ -z "$(git rev-list --merges "$base..$head")" ] || {
    echo "local-review: a merge commit between $from and $to; rebase first" >&2; exit 1; }

  tmp=$(mktemp -d "${TMPDIR:-/tmp}/local-review.XXXXXX")
  trap cleanup EXIT
  trap 'exit 130' INT TERM
  # base|head|label: the label goes last, so a | in a subject stays in it.
  runs=("$base|$head|whole diff")
  if [ "$(printf '%s\n' "$commits" | wc -l)" -gt 1 ]; then
    for c in $commits; do
      if [ ${#only[@]} -gt 0 ] && ! printf '%s\n' "${only[@]}" | grep -qx "$c"; then
        continue
      fi
      runs+=("$c^|$c|commit ${c:0:8} $(git log -1 --format=%s "$c")")
    done
  fi
  echo "== review of $name, ${base:0:8}..${head:0:8}: ${#runs[@]} runs in parallel"
  i=0
  for r in "${runs[@]}"; do
    IFS='|' read -r rbase rhead label <<<"$r"
    printf '%s\n' "$label" > "$tmp/label.$i"
    # set -e would end the group at a failed run before its code is
    # written: catch the code instead.
    { rc=0; (run_one "$i" "$(git rev-parse "$rbase")" "$rhead") > "$tmp/out.$i" 2> "$tmp/err.$i" || rc=$?
      echo "$rc" > "$tmp/rc.$i"; } &
    pids+=($!)
    i=$((i + 1))
  done
  wait
  pids=()
  python3 -I - "$tmp" "${#runs[@]}" <<'EOF'
import json, re, sys
tmp, n = sys.argv[1], int(sys.argv[2])
cost, lgtm, failed = 0.0, True, []
for i in range(n):
    read = lambda name: open("%s/%s.%d" % (tmp, name, i), encoding="utf-8").read()
    label = read("label").strip()
    print("\n== %s" % label)
    try:
        if read("rc").strip() != "0":
            raise ValueError("claude exit %s: %s" % (read("rc").strip(), read("err").strip()[:300]))
        r = json.loads(read("out"))
        if r.get("is_error"):
            raise ValueError("claude error: %s" % str(r.get("result"))[:300])
    except (OSError, ValueError) as e:
        print("could not run: %s" % e)
        failed.append(label)
        continue
    result = str(r.get("result") or "")
    print(result)
    print("-- turns %s, cost $%.2f" % (r.get("num_turns"), r.get("total_cost_usd") or 0))
    cost += r.get("total_cost_usd") or 0
    # LGTM when a line of the review starts with it (the skill puts it in
    # its closing summary) and no line lists an issue at a path:line.
    lines = [l.strip() for l in result.splitlines() if l.strip()]
    says = any(re.sub(r"^[*_#>\s]+", "", l).startswith("LGTM") for l in lines)
    listed = any(re.match(r"^[-*`\s\d.]*[\w./-]+:\d+", l) for l in lines)
    lgtm = lgtm and says and not listed
verdict = "could not run: %s" % ", ".join(failed) if failed else "LGTM" if lgtm else "issues to read"
print("\n== %d runs, cost $%.2f, %s" % (n, cost, verdict))
sys.exit(1 if failed else 0)
EOF
}

self_test() {
  local fails=0 out
  t=$(mktemp -d "${TMPDIR:-/tmp}/local-review-test.XXXXXX")
  trap 'rm -rf "$t"' EXIT
  check() { if eval "$2"; then echo "ok: $1"; else echo "FAIL: $1"; fails=$((fails + 1)); fi; }
  commit() { echo "$1" > "$t/src/$1"; git -C "$t/src" add "$1"
    git -C "$t/src" -c user.name=t -c user.email=t@t commit -q -m "$1"; }

  git init -q -b main "$t/src"
  git -C "$t/src" remote add origin https://github.com/scality/example.git
  for c in c0 c1 c2 c3; do commit "$c"; done
  git -C "$t/src" -c user.name=t -c user.email=t@t commit -q --amend -m "c3 | with a bar"
  git -C "$t/src" branch other HEAD~1

  # A fake claude: each run records, in a file of its own, its arguments,
  # its branches, the commits the skill would review, its origin, its
  # folder and its stdin. FAKE_FAIL and FAKE_ISSUE name a head subject.
  cat > "$t/claude" <<EOF
#!/bin/sh
r="$t/run.\$\$"
printf '%s\n' "\$@" > "\$r.argv"
git branch --list --format='%(refname:short)' | tr '\n' ' ' > "\$r.branches"
git rev-list --count HEAD --not --exclude=review --branches > "\$r.count"
git remote get-url origin > "\$r.origin"
pwd > "\$r.cwd"
cat > "\$r.stdin"
subject=\$(git log -1 --format=%s)
echo \$\$ > "\$r.pid"
[ -n "\$FAKE_SLEEP" ] && sleep "\$FAKE_SLEEP"
if [ "\$subject" = "\$FAKE_FAIL" ]; then echo "fake failure on stderr" >&2; exit 3; fi
if [ "\$subject" = "\$FAKE_LATE_LGTM" ]; then
  printf '%s\n' '{"result": "Reviewed the commits.\\n\\nLGTM", "num_turns": 4, "total_cost_usd": 0.5, "is_error": false}'
elif [ "\$subject" = "\$FAKE_LISTED" ]; then
  printf '%s\n' '{"result": "**a.py:3** a bug.\\n\\nLGTM otherwise", "num_turns": 4, "total_cost_usd": 0.5, "is_error": false}'
elif [ "\$subject" = "\$FAKE_ISSUE" ]; then
  echo '{"result": "1. a real issue, LGTM otherwise", "num_turns": 4, "total_cost_usd": 0.5, "is_error": false}'
else
  echo '{"result": "LGTM", "num_turns": 4, "total_cost_usd": 0.5, "is_error": false}'
fi
EOF
  chmod +x "$t/claude"
  export BNAAS_REVIEW_CLAUDE="$t/claude"
  local self
  self=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")
  gate() { rm -f "$t"/run.*; (cd "$t/src" && echo data | "$BASH" "$self" "$@" 2>&1); }
  runs() { ls "$t"/run.*.argv 2>/dev/null | wc -l | tr -d ' '; }
  counts() { cat "$t"/run.*.count | sort | tr '\n' ' '; }

  check "no argument is a usage error" '! (cd "$t/src" && "$BASH" "$self") 2>/dev/null'
  check "two arguments are a usage error" '! (cd "$t/src" && "$BASH" "$self" x HEAD~2) 2>/dev/null'
  check "an unknown base fails" '! (cd "$t/src" && "$BASH" "$self" x nope HEAD) 2>/dev/null'
  check "an empty range fails" '! (cd "$t/src" && "$BASH" "$self" x HEAD HEAD) >/dev/null 2>&1'
  check "an empty range says so" '[[ $( (cd "$t/src" && "$BASH" "$self" x HEAD HEAD) 2>&1) == *"no commit between HEAD and HEAD"* ]]'

  out=$(gate one HEAD~1 HEAD) || true
  check "one commit: one run only" '[ "$(runs)" = 1 ] && [[ $out == *"== whole diff"* ]]'
  check "the result, turns and cost are printed" '[[ $out == *LGTM* && $out == *"turns 4, cost \$0.50"* ]]'
  check "the clone has only base and review" '[ "$(cat "$t"/run.*.branches)" = "base review " ]'
  check "origin points at GitHub" '[ "$(cat "$t"/run.*.origin)" = https://github.com/scality/example.git ]'
  check "claude runs with the CI model" 'grep -qx claude-opus-5-5 "$t"/run.*.argv'
  check "claude runs with user settings only" 'grep -A1 -x -- --setting-sources "$t"/run.*.argv | grep -qx user'
  check "the post scripts are denied" 'grep -q "post-inline-comment.sh" "$t"/run.*.argv'
  check "claude reads no stdin" '! [ -s "$t"/run.*.stdin ]'

  git -C "$t/src" checkout -q -b moved HEAD~3
  commit side
  git -C "$t/src" checkout -q main
  out=$(gate moved moved HEAD --only HEAD) && rc=0 || rc=$?
  check "a base that moved on reviews from the merge-base" \
    '[ $rc = 0 ] && [ "$(runs)" = 2 ] && [[ $out == *"== whole diff"* && $out == *" c3 | with a bar"* ]]'
  git -C "$t/src" branch -q -D moved

  out=$(gate three HEAD~3 HEAD) && rc=0 || rc=$?
  check "three commits: four runs, the whole range and each commit" \
    '[ "$(runs)" = 4 ] && [ "$(counts)" = "1 1 1 3 " ]'
  check "each section is labelled, a | in a subject included" \
    '[[ $out == *"== whole diff"* && $out == *"== commit "*" c1"* && $out == *" c2"* && $out == *" c3 | with a bar"* ]]'
  check "the last line gives the runs, the cost and LGTM" '[ "$(printf "%s\n" "$out" | tail -1)" = "== 4 runs, cost \$2.00, LGTM" ] && [ $rc = 0 ]'
  check "every scratch clone is removed" '[ -z "$(cat "$t"/run.*.cwd | while read -r d; do [ -e "$d" ] && echo "$d"; done)" ]'

  c2=$(git -C "$t/src" rev-parse HEAD~1)
  out=$(gate again HEAD~3 HEAD --only "$c2") || true
  check "--only: the whole range and the commits named" \
    '[ "$(runs)" = 2 ] && [[ $out == *" c2"* && $out != *" c1"* && $out != *" c3"* ]]'
  check "--only with a commit outside the range fails" \
    '! (cd "$t/src" && "$BASH" "$self" x HEAD~2 HEAD --only HEAD~3) >/dev/null 2>&1'
  check "--only with no commit is a usage error" '! (cd "$t/src" && "$BASH" "$self" x HEAD~2 HEAD --only) 2>/dev/null'

  out=$(FAKE_ISSUE=c2 gate issues HEAD~3 HEAD) && rc=0 || rc=$?
  check "an issue is not LGTM, and not a failure" '[[ $out == *"a real issue"* ]] && [[ $(printf "%s\n" "$out" | tail -1) == *"issues to read" ]] && [ $rc = 0 ]'
  out=$(FAKE_LATE_LGTM=c2 gate late HEAD~3 HEAD) || true
  check "LGTM in the closing summary is LGTM" '[ "$(printf "%s\n" "$out" | tail -1)" = "== 4 runs, cost \$2.00, LGTM" ]'
  out=$(FAKE_LISTED=c2 gate listed HEAD~3 HEAD) || true
  check "a review that lists an issue at a path:line is not LGTM" '[[ $(printf "%s\n" "$out" | tail -1) == *"issues to read" ]]'
  # A clone whose setup fails is a failed run, never a review.
  mkdir -p "$t/bin"
  real_git=$(command -v git)
  printf '#!/bin/sh\ncase "$*" in *"branch -f base"*) exit 1;; esac\nexec "%s" "$@"\n' "$real_git" > "$t/bin/git"
  chmod +x "$t/bin/git"
  out=$(PATH="$t/bin:$PATH" gate broken HEAD~1 HEAD) && rc=0 || rc=$?
  check "a clone that fails to set up fails its run" '[ $rc != 0 ] && [ "$(runs)" = 0 ] && [[ $out == *"could not run"* ]]'
  out=$(FAKE_FAIL=c2 gate failing HEAD~3 HEAD) && rc=0 || rc=$?
  check "a run that fails fails the gate, and the others still report" \
    '[ $rc != 0 ] && [[ $(printf "%s\n" "$out" | tail -1) == *"could not run: commit "*" c2" ]] && [[ $out == *LGTM* ]]'
  check "the failed run gives its exit code and its stderr" '[[ $out == *"claude exit 3: fake failure on stderr"* ]]'

  # Stopped (Ctrl-C, or TERM here: a background job of a script ignores
  # INT): the runs still going are killed, and the clones removed.
  rm -f "$t"/run.*
  (cd "$t/src" && FAKE_SLEEP=30 exec "$BASH" "$self" stop HEAD~3 HEAD > /dev/null 2>&1) &
  local gpid=$! n=0
  while [ "$(ls "$t"/run.*.pid 2>/dev/null | wc -l | tr -d ' ')" -lt 4 ] && [ $n -lt 100 ]; do sleep 0.1; n=$((n + 1)); done
  kill -TERM "$gpid" 2>/dev/null || true
  # The fakes sleep 30 s: give the gate 5 s to stop them, not more.
  n=0
  while kill -0 "$gpid" 2>/dev/null && [ $n -lt 50 ]; do sleep 0.1; n=$((n + 1)); done
  stopped() {
    local f
    for f in "$t"/run.*.pid; do ! kill -0 "$(cat "$f")" 2>/dev/null || return 1; done
    [ -z "$(cat "$t"/run.*.cwd | while read -r d; do [ -e "$d" ] && echo "$d"; done)" ]
  }
  check "a stopped gate kills its runs and removes the clones" stopped
  for f in "$t"/run.*.pid; do kill_tree "$(cat "$f")"; done

  git -C "$t/src" switch -q -c side HEAD~1
  commit s
  git -C "$t/src" switch -q main
  git -C "$t/src" -c user.name=t -c user.email=t@t merge -q --no-edit side
  check "a merge commit in the range fails" '! (cd "$t/src" && "$BASH" "$self" x HEAD~2 HEAD) >/dev/null 2>&1'

  # The gate runs from a linked worktree too.
  git -C "$t/src" worktree add -q --detach "$t/wt" side
  rm -f "$t"/run.*
  out=$(cd "$t/wt" && "$BASH" "$self" wt HEAD~1 HEAD 2>&1) || true
  check "a linked worktree works" '[[ $out == *LGTM* ]] && [ "$(counts)" = "1 " ]'

  [ "$fails" -eq 0 ] && echo "all local-review tests passed" || { echo "$fails failed"; exit 1; }
}

case "${1:-}" in
  --self-test) [ $# -eq 1 ] || usage; self_test ;;
  *) [ $# -ge 3 ] || usage; review "$@" ;;
esac
