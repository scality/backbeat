#!/bin/sh
# The D1 SessionStart hook: checks BNAAS_HOME, the folder of all local D1
# data, which each developer sets in their shell profile. Set, absolute and
# writable: it creates the folder and a .gitignore that keeps git off it,
# quietly. Otherwise it tells the session that the D1 hooks and skills are
# off. It never blocks the session: it exits 0.
off="Tell the developer now: the D1 journal, the dog and the D1 skills are off in this session"
case $BNAAS_HOME in
  /*) ;;
  *) echo "$off, because BNAAS_HOME is not set to an absolute path. Add export BNAAS_HOME=\"$CLAUDE_PROJECT_DIR/.bnaas\" to the shell profile (~/.zshenv, ~/.bashrc or ~/.config/fish/config.fish), open a new terminal and start Claude Code again in the workspace folder."
     exit 0 ;;
esac
new=; [ -d "$BNAAS_HOME" ] || new=1
if ! mkdir -p "$BNAAS_HOME" 2>/dev/null || [ ! -w "$BNAAS_HOME" ]; then
  echo "$off, because BNAAS_HOME=$BNAAS_HOME cannot be created or written. Fix the path in the shell profile and start Claude Code again."
  exit 0
fi
# Only a folder this hook made gets the .gitignore, so that a BNAAS_HOME
# pointed at a repository by mistake never hides its files from git.
[ -z "$new" ] || printf '*\n' > "$BNAAS_HOME/.gitignore" 2>/dev/null
exit 0
