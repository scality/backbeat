#!/bin/sh
# Open a URL in the default browser, on macOS or Linux. It never fails the
# caller: without a browser it prints the URL instead.
[ -n "$1" ] || { echo "usage: open_url.sh <url>" >&2; exit 2; }
if command -v open >/dev/null 2>&1 && [ "$(uname)" = Darwin ]; then
  open "$1"
elif command -v xdg-open >/dev/null 2>&1; then
  xdg-open "$1" >/dev/null 2>&1
else
  echo "Open: $1"
fi
exit 0
