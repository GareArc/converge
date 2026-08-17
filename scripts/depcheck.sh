#!/usr/bin/env bash
# Core-module import gate: stdlib + this module + the cron parser, nothing else.
set -euo pipefail
cd "$(dirname "$0")/.."
allow='^(github\.com/GareArc/converge(/|$)|github\.com/robfig/cron/v3(/|$))'
bad=$(GOWORK=off go list -deps ./... | awk -F/ '$1 ~ /\./' | grep -Ev "$allow" || true)
if [ -n "$bad" ]; then
  echo "disallowed core dependencies:" >&2
  echo "$bad" >&2
  exit 1
fi
