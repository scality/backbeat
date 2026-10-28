#!/usr/bin/env bash
# Lay out what the run needs in its folder: the journal command, the
# agent, the skill, the practice config and the waiting facts.
set -eu
case_dir=$(cd "$(dirname "$0")" && pwd)
setup=$(cd "$case_dir/../../../.." && pwd)
mkdir -p .claude/hooks .claude/agents .claude/skills/bnaas-journal
cp "$setup/hooks/journal" .claude/hooks/
cp "$setup/agents/bnaas-journal.md" .claude/agents/
cp "$setup/skills/bnaas-journal/SKILL.md" .claude/skills/bnaas-journal/
cp -R "$case_dir/files/." .
cp "$case_dir/../fixtures/journal-practice.json" .
