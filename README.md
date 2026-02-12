# MWAHAHA Vote Web App

Website to crowd-annotate computer-generated jokes from
[the MWAHAHA competition](https://pln-fing-udelar.github.io/semeval-2026-humor-gen/).

To use this repo, follow the instructions in the
[pgHumor-clasificahumor repository](https://github.com/pln-fing-udelar/pghumor-clasificahumor) and replace the values
appropriately.
Some differences:

* The database name is `mwahaha` instead of `pghumor`.
* The Docker container names are prefixed with `mwahaha-vote-webapp-` instead of `clasificahumor-`.

## Setup

```bash
cd web/
DB_HOST=$(docker container inspect mwahaha-vote-webapp-database-1 | uv run jq -r '.[0].NetworkSettings.Networks."mwahaha-vote-webapp_net".IPAddress')
```

## Add the prompts to the database

First, place the prompt files under the directory `web/prompts/`.
Then, run:

```bash
./scripts/ingest_prompts.py
```

## Ingest submissions

Save your CodaBench session ID in the env var `CODABENCH_SESSION_ID`.
You can obtain by looking at the cookie named `sessionid` in your browser's request's `Cookie` header when logged in to
CodaBench.

TODO: explain how to add submissions manually

Then, run:

```bash
./scripts/ingest_submissions.py
```

## Ingest the baseline

Place the `baseline.zip` file under the directory `web/baselines`.
Then, run:

```bash
./scripts/ingest_baseline.py
```

## TODO: explain:

`screen`

In production:

```bash
while true; do
  DB_HOST=$(docker container inspect clasificahumor-database-1 | jq -r '.[0].NetworkSettings.Networks."clasificahumor_net".IPAddress') ./scripts/compute_scores.sh
  sleep 3600
done

# May need to empty `submissions/` first.
export CODABENCH_SESSION_ID=...
while true; do
  DB_HOST=$(docker container inspect clasificahumor-database-1 | jq -r '.[0].NetworkSettings.Networks."clasificahumor_net".IPAddress') ./scripts/ingest_submissions.py
  sleep 3600
done
```
