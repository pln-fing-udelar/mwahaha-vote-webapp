#!/usr/bin/env bash

set -e
set -x

COMPETITION="${1:-${COMPETITION:-mwahaha}}"
export COMPETITION
[ "$#" -gt 0 ] && shift

exec ./scripts/compute_scores.py --competition "$COMPETITION" --dump-intermediate scoring "$@"
