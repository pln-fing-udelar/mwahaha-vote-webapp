#!/usr/bin/env bash

set -x

./scripts/prepare_data_for_scoring.py

TASKS=$(uv run python -c "from mwahahavote.config import ACTIVE_COMPETITION; print(' '.join(ACTIVE_COMPETITION.task_ids))")

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

./scripts/postprocess_scores.py
