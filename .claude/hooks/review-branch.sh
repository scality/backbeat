#!/usr/bin/env bash
# Local run of the CI review on the current branch of a repository (Rule 7).
# It runs the review-pr skill of scality-skills@scality-agent-hub, the one
# the "Code Review" workflow runs on each PR, in its local mode: it reviews
# the commits that no other local branch has and posts nothing to GitHub.
# The review is saved as <git common dir>/bnaas-review/<HEAD sha>.md. The
# review-gate.sh hook lets a git push through only when that file exists.
# Usage: review-branch.sh [repository directory]

set -euo pipefail

repo="${1:-.}"
cd "$repo"
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Not a git repository: $repo" >&2
  exit 1
fi

if ! claude plugin list --json 2>/dev/null | python3 -I -c 'import json,sys
ok = any(p.get("id") == "scality-skills@scality-agent-hub" and p.get("enabled")
         for p in json.load(sys.stdin))
sys.exit(0 if ok else 1)'; then
  cat >&2 <<'EOF'
The review-pr skill is not installed. Install it once, at user scope:
  claude plugin marketplace add scality/agent-hub
  claude plugin install scality-skills@scality-agent-hub
EOF
  exit 1
fi

branch=$(git branch --show-current)
case "$branch" in
  '' | epic/* | development/* | hotfix/*)
    echo "Check out the ticket branch first (got '${branch:-detached HEAD}')." >&2
    exit 1
    ;;
esac

# The skill reviews the commits that no other local branch has, so each
# remote epic branch needs an up-to-date local copy. The fetch creates it
# or fast-forwards it, and changes nothing else.
for epic in $(git for-each-ref --format='%(refname:lstrip=3)' 'refs/remotes/origin/epic/'); do
  git fetch -q origin "$epic:$epic" 2>/dev/null || true
done
count=$(git rev-list --count HEAD --not --exclude="$branch" --branches)
if [ "$count" -eq 0 ]; then
  echo "No commit of $branch is missing from the other local branches." >&2
  exit 1
fi

sha=$(git rev-parse HEAD)
dir="$(cd "$(git rev-parse --git-common-dir)" && pwd)/bnaas-review"
mkdir -p "$dir"
out=$(mktemp "$dir/.review.XXXXXX")
trap 'rm -f "$out"' EXIT

echo "Reviewing $count commit(s) of $branch at ${sha:0:8} with review-pr." \
  "This takes 5 to 15 minutes." >&2
claude -p "/scality-skills:review-pr" --model opus \
  --allowedTools Read Grep Glob "Bash(git rev-list:*)" "Bash(git log:*)" \
  "Bash(git diff:*)" "Bash(git show:*)" "Bash(git rev-parse:*)" \
  "Bash(git branch --show-current)" >"$out"

if [ ! -s "$out" ]; then
  echo "The review printed nothing, so nothing is recorded." >&2
  exit 1
fi
# A real review says LGTM or lists items as **<file>:<line>**.
if ! grep -q -E 'LGTM|\*\*[^*]+:[0-9]+\*\*' "$out"; then
  echo "This output is not a review, so nothing is recorded:" >&2
  cat "$out" >&2
  exit 1
fi
if [ "$(git rev-parse HEAD)" != "$sha" ]; then
  echo "HEAD moved during the review, so nothing is recorded. Run it again." >&2
  exit 1
fi

mv "$out" "$dir/$sha.md"
trap - EXIT
echo "Review saved: $dir/$sha.md"
echo
cat "$dir/$sha.md"
