#!/usr/bin/env bash

url=localhost:5000

start_time=$(date +%s.%N)

for _i in {1..50}; do
  curl -sS "${url}/battles" >/dev/null &
done

wait

end_time=$(date +%s.%N)
elapsed=$(echo "$end_time - $start_time" | bc)

echo "Total time taken: ${elapsed} seconds"
