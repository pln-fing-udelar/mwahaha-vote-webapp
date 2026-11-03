#!/usr/bin/env bash

set -e
set -x

COMPETITION="${1:-${COMPETITION:-mwahaha}}"
export COMPETITION
[ "$#" -gt 0 ] && shift

./scripts/prepare_data_for_scoring.py --competition "$COMPETITION" "$@"

TASKS=$(uv run python -c "from mwahahavote.config import get_competition; print(' '.join(get_competition('$COMPETITION').task_ids))")

for task in $TASKS; do
  json_file="scoring/votes-$task.json"
  if (( $(wc -c < "$json_file") <= 2 )); then
    echo "Skipping the unvoted task $task."
    rm -f "scoring/elo_results_$task.pkl"
  else
    ./scripts/elo_analysis.py --clean-battle-file "$json_file" --output "scoring/elo_results_$task.pkl" --num-cpu 1
  fi
done

./scripts/postprocess_scores.py --competition "$COMPETITION"
