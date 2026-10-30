#!/usr/bin/env bash

set -e
set -x

COMPETITION="${1:-${COMPETITION:-mwahaha}}"
export COMPETITION

./scripts/prepare_data_for_scoring.py --competition "$COMPETITION"

TASKS=$(uv run python -c "from mwahahavote.config import get_competition; print(' '.join(get_competition('$COMPETITION').task_ids))")

for task in $TASKS; do
  json_file="scoring/votes-$task.json"
  if (( $(wc -c < "$json_file") <= 2 )); then
    echo "Skipping the unvoted task $task."
    rm -f "scoring/elo_results_$task.pkl"
  else
    uv run --extra scripts -m fastchat.serve.monitor.elo_analysis --clean-battle-file "$json_file" --num-cpu 1
    mv elo_results_*.pkl "scoring/elo_results_$task.pkl"
  fi
done

./scripts/postprocess_scores.py --competition "$COMPETITION"
