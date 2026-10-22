#!/bin/sh
# Render the Tickets tab from the sample Jira export, a captured prs.py run
# and a Markdown export of the Backlog tab (the docs connector `export`).
# Refresh the capture with:
#   python3 .claude/skills/bnaas-whats-next/scripts/prs.py --json --login anurag4DSB \
#     vault cloudserver backbeat federation integration ring s3utils \
#     --dev arsenal --dev cloudserverclient \
#     > .claude/skills/bnaas-whats-next/evals/fixtures/prs-live.json
cd "$(dirname "$0")/../../../../.." || exit 1
FIX=.claude/skills/bnaas-whats-next/evals/fixtures
python3 -I .claude/skills/bnaas-whats-next/scripts/render_tickets.py \
  --backlog "$FIX/backlog.md" --jira "$FIX/jira-sample.json" \
  --prs "$FIX/prs-live.json" --sprint "S3C Sprint 42" "$@"
