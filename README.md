# MWAHAHA Vote Web App

Website to crowd-annotate computer-generated jokes from
[the MWAHAHA competition](https://pln-fing-udelar.github.io/semeval-2026-humor-gen/).
It was also used to annotate for the [HAHA 2026 competition](https://www.fing.edu.uy/inco/grupos/pln/haha/).

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

To run some of the Python scripts and command in this README file,
you may benefit from prefixing `uv run --env-file ../.env` to them.

## Add the prompts to the database

First, place the prompt files under the directory `web/prompts/`.
Then, run:

```bash
./scripts/ingest_prompts.py
```

## Ingest submissions

Save your CodaBench session ID in the env var `CODABENCH_SESSION_ID`.
You can obtain it by looking at the cookie named `sessionid` in your browser's request's `Cookie` header when logged in
to CodaBench.

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

## Compute the scores

Run:

```bash
./scripts/compute_scores.sh
```

## Load test

To conduct a test simulating high annotation load (especially for production), run:

```bash
./load_test.sh
```

## Running it in production

When we run the server in production to collect annotations, we run some commands under an SSH session using `screen`
so it survives session disconnections. You can do similarly with other programs such as `tmux`.

Note that the database docker container in production is called `clasificahumor-database-1` because it's shared with
clasificahumor.
And its Docker Compose network is called `clasificahumor_net`.

When making changes on the submissions or scores, it's recommended to first test it locally,
then apply the same change on production.

To ingest new submissions and recompute the scores based on the latest annotations every hour,
you can run the following command:

```bash
# You may need to empty the `submissions/` subdirectory first.
export CODABENCH_SESSION_ID=...
while true; do
  export DB_HOST=$(docker container inspect clasificahumor-database-1 | uv run jq -r '.[0].NetworkSettings.Networks."clasificahumor_net".IPAddress')
  ./scripts/ingest_submissions.py
  ./scripts/compute_scores.sh
  sleep 3600
done
```

To inspect and run queries against the database in production:

```bash
uv run mycli -h "$(docker container inspect clasificahumor-database-1 | jq -r '.[0].NetworkSettings.Networks."clasificahumor_net".IPAddress')" -u root mwahaha
```

To save a database dump:

```bash
docker exec clasificahumor-database-1 mysqldump -u root -p mwahaha > /opt/dump-mwahaha-DATE.sql
```

To inspect the web logs:

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml logs -f web
```
