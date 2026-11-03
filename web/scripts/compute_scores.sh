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

exec ./scripts/compute_scores.py --competition "$COMPETITION" --dump-intermediate scoring "$@"
