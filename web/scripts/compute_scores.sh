#!/usr/bin/env bash

set -x

uv run --env-file ../.env scripts/prepare_data_for_scoring.py

for task in a-en a-es a-zh b1 b2; do
  json_file=scoring/votes-$task.json
  if (( $(wc -c < "$json_file") <= 2 )); then
    echo "Skipping the unvoted task $task."
  else
    uv run -m fastchat.serve.monitor.elo_analysis --clean-battle-file "$json_file" --num-cpu 1
    mv elo_results_*.pkl "scoring/elo_results_$task.pkl"
  fi
done
