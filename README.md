# MWAHAHA Vote Web App

Website to crowd-annotate computer-generated jokes from
[the MWAHAHA competition](https://pln-fing-udelar.github.io/semeval-2026-humor-gen/).

To use this repo, follow the instructions in the
[pgHumor-clasificahumor repository](https://github.com/pln-fing-udelar/pghumor-clasificahumor) and replace the values
appropriately.
Some differences:

* The database name is `mwahaha` instead of `pghumor`.
* The Docker container names are prefixed with `mwahaha-vote-webapp-` instead of `clasificahumor-`.

## Add the prompts to the database

First, place the prompt files under the directory `web/prompts/`.
Then, run:

```bash
cd web/
uv run --env-file ../.env ./scripts/ingest_prompts.py
```

## Ingest submissions from CodaBench

Save your CodaBench session ID in the env var `CODABENCH_SESSION_ID`.
You can obtain by looking at the cookie named `sessionid` in your browser's request's `Cookie` header when logged in to
CodaBench.

Then, run:

```bash
cd web/
uv run --env-file ../.env ./scripts/ingest_all_submissions_from_codabench.py
```

## Ingest the baseline

Place the `baseline.zip` file under the directory `web/baselines`.
Then, run:

```bash
cd web/
uv run --env-file ../.env ./scripts/ingest_baseline.py
```
