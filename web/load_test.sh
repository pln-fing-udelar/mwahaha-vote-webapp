#!/usr/bin/env bash

url=localhost:5000

for _i in {1..50}; do
  curl -sS "${url}/battles" >/dev/null &
done

wait
