[![ACL Anthology](https://img.shields.io/badge/ACL_Anthology-2026.semeval--1.454-ed1c24.svg)](https://aclanthology.org/2026.semeval-1.454/)
[![Actions Status](https://github.com/pln-fing-udelar/mwahaha-vote-webapp/workflows/CI/badge.svg)](https://github.com/pln-fing-udelar/mwahaha-vote-webapp/actions)
[![Coverage](https://codecov.io/gh/pln-fing-udelar/mwahaha-vote-webapp/branch/main/graph/badge.svg)](https://codecov.io/gh/pln-fing-udelar/mwahaha-vote-webapp)

# MWAHAHA Vote Web App

Website to crowd-annotate computer-generated jokes from
[the MWAHAHA competition](https://pln-fing-udelar.github.io/semeval-2026-humor-gen/) (SemEval 2026 Task 1).
It was also used to annotate for the [HAHA 2026 task 3](https://www.fing.edu.uy/inco/grupos/pln/haha/).

## Quick links

* [MWAHAHA 2026 @ SemEval 2026 Task 1](https://pln-fing-udelar.github.io/semeval-2026-humor-gen/)
* [HAHA 2026 @ IberLEF 2026](https://www.fing.edu.uy/inco/grupos/pln/haha/)
* [MWAHAHA 2026 data (prompts + systems' outputs + annotations)](https://github.com/pln-fing-udelar/mwahaha-vote-webapp/releases/tag/v2026.02.22)
  (The annotations were collected using the web app in this repo.)

## About using this annotation web application

To use this repo (e.g., to collect data in a similar fashion or to reproduce the results),
follow the instructions in the
[pgHumor-clasificahumor repository](https://github.com/pln-fing-udelar/pghumor-clasificahumor) and replace the values
appropriately.
Some differences:

* The database name is `mwahaha` instead of `pghumor`.
* The Docker container names are prefixed with `mwahaha-vote-webapp-` instead of `clasificahumor-`.

## Software Stack

* **Backend & API**: [FastAPI](https://fastapi.tiangolo.com/) running on [Uvicorn](https://www.uvicorn.org/) with [uvloop](https://github.com/MagicStack/uvloop) and [httptools](https://github.com/MagicStack/httptools) for async request handling; [Pydantic](https://docs.pydantic.dev/) for data validation; [Jinja2](https://jinja.palletsprojects.com/) for server-side HTML rendering.
* **Database Layer**: [MySQL](https://www.mysql.com/) 26.10 with [SQLAlchemy](https://www.sqlalchemy.org/) 2.0 (using async drivers: `asyncmy` in production and `aiosqlite` for in-memory unit tests).
* **Scoring Engine**: [FastChat](https://github.com/lm-sys/FastChat)'s Bradley-Terry / Elo ranking model with bootstrap confidence intervals; custom least-voted unseen battle matching algorithm (`random_least_voted_unseen_battles`).
* **Security & Anti-Bot**: [Cloudflare Turnstile](https://www.cloudflare.com/products/turnstile/) CAPTCHA with async server verification ([`httpx2`](https://pypi.org/project/httpx2/)); Fernet symmetric encryption ([`cryptography`](https://cryptography.io/)) for tamper-proof battle token signing.
* **Frontend**: Vanilla modern JavaScript (ES6+), HTML5, and CSS3 without heavy external framework dependencies.
* **Tooling & Environment**: Python `>=3.15` managed via [`uv`](https://docs.astral.sh/uv/); containerized with [Docker Compose](https://docs.docker.com/compose/); tested and linted with [`ruff`](https://docs.astral.sh/ruff/), [`ty`](https://github.com/astral-sh/ty), and [`pytest`](https://docs.pytest.org/).

## Setup

```bash
cd web/
DB_HOST=$(docker container inspect mwahaha-vote-webapp-database-1 | uv run jq -r '.[0].NetworkSettings.Networks."mwahaha-vote-webapp_net".IPAddress')
```

To run some of the Python scripts and commands in this README file,
you may benefit from prefixing `uv run --env-file ../.env` to them.

## Loading a database dump

Optionally, you can populate the database from a pre-existing dump, such as
[the database dump containing the annotations from MWAHAHA 2026](https://github.com/pln-fing-udelar/mwahaha-vote-webapp/releases/download/v2026.02.22/dump-mwahaha-sanitized.sql.gz).
This restores all `prompts`, `systems`, `outputs`, and `votes` at once, bypassing the need to ingest prompts and CodaBench submissions manually.

You can stream, decompress, and pipe the dump directly into the database container without saving it to disk:

```bash
curl -sSL https://github.com/pln-fing-udelar/mwahaha-vote-webapp/releases/download/v2026.02.22/dump-mwahaha-sanitized.sql.gz | \
  gunzip -c | \
  docker compose exec -T database mysql -uroot -p mwahaha
```

If you do this, you can skip the steps involving adding the prompts and ingesting the submissions and baselines.

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

To add submissions manually, place `<system_id>.tsv` files (with `id` and `text` tab-separated columns matching the
task's reference prompt IDs) under `web/submissions/`.

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

## Development & Testing

Run the linter, formatter, type-checker, and test suite from `web/`:

```bash
uv run ruff check --fix
uv run ruff format
uv run ty check
uv run pytest
```

You can run the linter, formatter, and type checker automatically with `pre-commit`.

## Load test

To conduct a test simulating high annotation load (especially for production), run:

```bash
./load_test.sh
```

## Running it in production

When we run the server in production to collect annotations, we run some commands under an SSH session using `screen`
so it survives session disconnections. You can do the same with other programs, such as `tmux`.

Note that the database Docker container in production is called `clasificahumor-database-1` because it's shared with
clasificahumor.
And its Docker Compose network is called `clasificahumor_net`.

When making changes to the submissions or scores, it's recommended to first test it locally,
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

To load a database dump:

```bash
docker exec -i clasificahumor-database-1 mysql -u root -p mwahaha < /opt/dump-mwahaha-DATE.sql
```

In development, you can load a dump using Docker Compose:

```bash
docker compose exec -T database mysql -u root -p mwahaha < /path/to/dump.sql
```

To inspect the web logs:

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml logs -f web
```

After multiple deploys in production, if there's not enough storage available, you can run:

```bash
docker system prune
```

## Citing

If you use this software or the MWAHAHA evaluation data in your research, please cite:

```bibtex
@inproceedings{castro-etal-2026-semeval,
    title = "{S}em{E}val-2026 Task 1: {MWAHAHA}, Models Write Automatic Humor And Humans Annotate",
    author = "Castro, Santiago  and
      Chiruzzo, Luis  and
      G{\'o}ngora, Santiago  and
      Deng, Naihao  and
      Rahili, Salar  and
      Sastre, Ignacio  and
      Ros{\'a}, Aiala  and
      Amoroso, Victoria  and
      Rey, Guillermo  and
      Moncecchi, Guillermo  and
      Meaney, J. A.  and
      Prada, Juan Jos{\'e}  and
      Mihalcea, Rada",
    editor = "Kochmar, Ekaterina  and
      Ghosh, Debanjan  and
      North, Kai  and
      Komachi, Mamoru  and
      Zampieri, Marcos",
    booktitle = "Proceedings of the 20th {I}nternational {W}orkshop on {S}emantic {E}valuation (2026)",
    month = jul,
    year = "2026",
    address = "San Diego, California, USA",
    publisher = "Association for Computational Linguistics",
    url = "https://aclanthology.org/2026.semeval-1.454/",
    doi = "10.18653/v1/2026.semeval-1.454",
    pages = "3797--3822",
    ISBN = "979-8-89176-414-9"
}
```
