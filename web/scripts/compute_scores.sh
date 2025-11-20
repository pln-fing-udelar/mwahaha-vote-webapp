#!/usr/bin/env bash

set -x

uv run --env-file ../.env scripts/prepare_data_for_scoring.py

for task in a-en a-es a-zh b1 b2; do
  uv run -m fastchat.serve.monitor.elo_analysis --clean-battle-file scoring/votes-${task}.json
  mv elo_results_*.pkl scoring/elo_results_${task}.pkl
done
