#!/usr/bin/env bash

set -e
set -x

if [ "$#" -gt 0 ] && [[ "$1" != -* ]]; then
  COMPETITION="$1"
  shift
else
  COMPETITION="${COMPETITION:-mwahaha}"
fi
export COMPETITION

# Dumping the intermediate files (the votes one by one, and the full results) requires reading every vote, so it's opt-in
# with `DUMP_INTERMEDIATE=1`. Without it, the database groups the votes instead, which is much faster.
DUMP_ARGS=()
if [ "${DUMP_INTERMEDIATE:-0}" = "1" ]; then
  DUMP_ARGS=(--dump-intermediate scoring)
fi

exec ./scripts/compute_scores.py --competition "$COMPETITION" "${DUMP_ARGS[@]}" "$@"
