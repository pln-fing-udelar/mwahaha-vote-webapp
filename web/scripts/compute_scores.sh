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

# Dumping the intermediate files (the votes one by one, and the full results) requires reading every vote.
# With `DUMP_INTERMEDIATE=0` it's skipped, and it's much faster: the database groups the votes instead.
DUMP_ARGS=(--dump-intermediate scoring)
if [ "${DUMP_INTERMEDIATE:-1}" = "0" ]; then
  DUMP_ARGS=()
fi

exec ./scripts/compute_scores.py --competition "$COMPETITION" "${DUMP_ARGS[@]}" "$@"
