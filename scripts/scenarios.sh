#!/usr/bin/env bash
# Acceptance gate: every scenario under examples/scenarios must run to completion
# and exit 0. Compiling them is not enough — they are the acceptance criteria and
# the source of the documentation's tagged snippets, so they must still work.
set -uo pipefail
cd "$(dirname "$0")/../examples"

log=$(mktemp -d)
trap 'rm -rf "$log"' EXIT

names=()
for dir in scenarios/*/; do
  name=$(basename "$dir")
  names+=("$name")
  ( go run "./scenarios/$name" >"$log/$name.out" 2>&1; echo $? >"$log/$name.code" ) &
done
wait

status=0
for name in "${names[@]}"; do
  if [ "$(cat "$log/$name.code")" != "0" ]; then
    echo "scenario $name failed:" >&2
    cat "$log/$name.out" >&2
    status=1
  fi
done
exit $status
