#!/usr/bin/env bash
# Test set for review-gate.sh. Usage: test-review-gate.sh
# It builds two scratch repositories in a temporary folder, feeds the gate
# PreToolUse payloads, and checks each decision. It changes nothing else.

G="$(cd "$(dirname "$0")" && pwd)/review-gate.sh"
S=$(mktemp -d)
trap 'rm -rf "$S"' EXIT
mkdir -p "$S/ws/notgit"
for r in a b; do
  git init -q -b main "$S/ws/$r"
  git -C "$S/ws/$r" commit -q --allow-empty -m init
  git -C "$S/ws/$r" switch -q -c feat
done
git -C "$S/ws/a" branch other
git -C "$S/ws/a" commit -q --allow-empty -m reviewed
mkdir -p "$S/ws/a/sub" "$S/ws/a/.git/bnaas-review"
echo review >"$S/ws/a/.git/bnaas-review/$(git -C "$S/ws/a" rev-parse HEAD).md"
git -C "$S/ws/a" switch -q other
git -C "$S/ws/a" commit -q --allow-empty -m unreviewed
git -C "$S/ws/a" switch -q feat
git -C "$S/ws/a" tag -m test 8.6.1-bnaas.1
git -C "$S/ws/a" tag -m test 8.6.1
W=$S/ws
A=$S/ws/a
SHA=$(git -C "$A" rev-parse HEAD)
P="git push"
pass=0
fail=0

t() {
  local want=$1 name=$2 cwd=$3 cmd=$4 out got
  out=$(python3 -I -c 'import json,sys; print(json.dumps({"tool_name":"Bash","cwd":sys.argv[1],"tool_input":{"command":sys.argv[2]}}))' "$cwd" "$cmd" | "$G")
  if [ -z "$out" ]; then got=ALLOW; else got=DENY; fi
  if [ "$got" = "$want" ]; then pass=$((pass + 1)); else fail=$((fail + 1)); echo "FAIL $name: want $want got $got: $cmd"; fi
}

# Branch pushes and the review record.
t ALLOW a1 "$A" "git status 2>&1"
t ALLOW a2 "$A" 'git commit -m "push later"'
t ALLOW a3 "$A" "$P -u origin feat"
t ALLOW a4 "$A" "$P -u origin feat 2>&1 | tail -3"
t ALLOW a5 "$W" "git -C a push -u origin feat"
t ALLOW a6 "$W" "cd a && $P origin feat"
t ALLOW a7 "$W" "git -C a fetch -q origin && git -C a push origin feat"
t ALLOW a8 "$A" "$P origin feat # ship it"
t ALLOW a9 "$A" "$P origin --delete other"
t ALLOW a10 "$A" "cat > /tmp/m.txt <<'EOF'
It refuses a $P. Do not skip it.
EOF"
t ALLOW a11 "$A" "timeout 120 $P origin feat"
t ALLOW a12 "$W" "bash -c 'git -C a push origin feat'"
t ALLOW a13 "$A" "grep -n '$P' README.md"
t ALLOW a14 "$A" "$P --force-with-lease origin feat"
t ALLOW a15 "$W" "ls"
t ALLOW a16 "$A" "git add plans/review-before-push.md && git commit -q -m x && git log -1"
t DENY d1 "$A" "# push it
$P origin other"
t DENY d2 "$A" "git log -1 --format=%h#%s && $P origin other"
t DENY d3 "$A" "timeout 120 $P origin other"
t DENY d4 "$W" "env X=1 git -C a push origin other"
t DENY d5 "$W" "bash -c 'git -C a push origin other'"
t DENY d6 "$W" "GIT_DIR=a/.git $P origin feat"
t DENY d7 "$W" "git --git-dir a/.git push origin feat"
t DENY d8 "$W" "pushd a && $P origin other"
t DENY d9 "$A" "(cd /tmp && git status) && $P origin other"
t DENY d10 "$A" "/usr/bin/git push origin other"
t DENY d11 "$A" "$P --all origin"
t DENY d12 "$A" "$P --tags origin"
t DENY d13 "$A" "git branch -f feat other && $P origin feat"
t DENY d14 "$A" "yarn version --new-version 1.2.3 && $P"
t DENY d15 "$W" 'for r in a b; do git -C $r push; done'
t DENY d16 "$W" "echo a | xargs -I{} git -C {} push origin other"
t DENY d17 "$A" "echo don't; $P origin other"
t DENY d18 "$A" "$P origin other:refs/heads/feat"
t DENY d19 "$A/sub" "cd - && $P origin other"
t DENY d20 "$A" "cd -P sub && $P origin other"
t DENY d21 "$W/notgit" "$P origin feat"
t DENY d22 "$W" "git -C a \\
push origin other"
t DENY d23 "$A" "$P \\
origin other"

# Epic tags pass with no review; any other tag is refused (Rule 6).
t ALLOW tag1 "$A" "$P origin 8.6.1-bnaas.1"
t ALLOW tag2 "$A" "$P origin refs/tags/8.6.1-bnaas.1"
t ALLOW tag3 "$W" "git -C a push origin refs/tags/8.6.1-bnaas.1 2>&1 | tail -2"
t DENY tag4 "$A" "$P origin 8.6.1"
t DENY tag5 "$A" "$P origin refs/tags/9.9.9"
t DENY tag6 "$A" "$P origin --delete 8.6.0"
t DENY tag7 "$A" "$P origin :refs/tags/8.6.0"
t ALLOW tag8 "$A" "$P origin --delete old-branch"
t DENY tag9 "$A" "$P --follow-tags origin feat"
t DENY tag10 "$A" "$P origin $SHA:refs/tags/7.0.0"

# Malformed payloads.
out=$(printf '{"tool_name":"Bash","tool_input":"%s origin other","cwd":"%s"}' "$P" "$A" | "$G")
if [ -n "$out" ]; then pass=$((pass + 1)); else fail=$((fail + 1)); echo "FAIL malformed payload"; fi
out=$(printf 'not json at all' | "$G")
if [ -z "$out" ]; then pass=$((pass + 1)); else fail=$((fail + 1)); echo "FAIL junk without push"; fi

echo "pass=$pass fail=$fail"
[ "$fail" -eq 0 ]
