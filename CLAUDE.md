# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A crowd-annotation web app for the MWAHAHA competition (SemEval 2026 humor generation).
Users perform pairwise comparisons of computer-generated jokes,
and the system uses FastChat's Elo ranking to score submissions.

## Architecture

### Core Components

**Database Layer** (`web/src/mwahahavote/database.py`):

- MySQL backend with SQLAlchemy
- Four main tables: `prompts`, `systems`, `outputs`, `votes`
- Complex SQL query in `STATEMENT_RANDOM_LEAST_VOTED_UNSEEN_BATTLES` implements the battle selection algorithm:
  prioritizes systems/prompts with fewer votes, ensures users don't see duplicates
- Task types: `a-en`, `a-es`, `a-zh` (text-based), `b1`, `b2` (image-based)
- Vote types: `a` (system A wins), `b` (system B wins), `n` (skip), `t` (tie)

**FastAPI App** (`web/src/mwahahavote/__main__.py`):

- Main routes: `/battles` (get comparison pairs), `/vote` (submit vote), `/l` (leaderboard)
- Phase ID constant: `PHASE_ID = 15785` (CodaBench evaluation phase)
- Session management via cookies

### CORS Configuration

**Credentials:** Disabled (`allow_credentials=False`)

- Session cookies will NOT be sent in cross-origin requests
- Affects: `/session-vote-count`, `/battles`, `/vote` session tracking
- Current architecture serves frontend from the same origin (no CORS needed)

**Allowed Origins:**

- Development: `http://localhost:5000` (hardcoded)
- Production: Derived from `VIRTUAL_HOST` environment variable (adds `https://` prefix)

**Allowed Methods:** GET, POST only

**Ingestion Pipeline** (`web/scripts/`):

- `ingest_prompts.py`: Load prompts from TSV files in `web/prompts/`
- `ingest_submissions.py`: Fetch from CodaBench API (requires `CODABENCH_SESSION_ID` env var)
  or manual files in `web/submissions/`
- `ingest_baseline.py`: Load baseline from `web/baselines/baseline.zip`

**Scoring Pipeline** (`web/scripts/compute_scores.sh`):

1. `prepare_data_for_scoring.py`: Convert votes to FastChat format
2. Run FastChat Elo analysis per task
3. `postprocess_scores.py`: Generate final leaderboard JSON

### Key Data Flow

Prompts (TSV) → DB → Submissions (CodaBench/manual) → Outputs → User votes → Elo scoring → Leaderboard

## Development Commands

### Setup and Run

```bash
# Start services (uses docker-compose.override.yml for dev settings)
docker compose up

# Get database host IP for scripts
cd web/
DB_HOST=$(docker container inspect mwahaha-vote-webapp-database-1 | uv run jq -r '.[0].NetworkSettings.Networks."mwahaha-vote-webapp_net".IPAddress')
```

### Data Ingestion

```bash
# Place TSV files in web/prompts/ first
./scripts/ingest_prompts.py

# Requires CODABENCH_SESSION_ID env var (from browser cookie)
./scripts/ingest_submissions.py

# Place baseline.zip in web/baselines/ first
./scripts/ingest_baseline.py
```

### Scoring

```bash
./scripts/compute_scores.sh

# Optionally exclude specific session IDs from scoring
EXCLUDED_SESSION_IDS=session1,session2,session3 ./scripts/compute_scores.sh
```

Runs for all tasks: `a-en`, `a-es`, `a-zh`, `b1`, `b2`. Skips tasks with no votes.

**Session Exclusion**: Set `EXCLUDED_SESSION_IDS` (comma-separated) to exclude votes from specific sessions in score
calculations.

### Code Quality

```bash
cd web/
uv run ruff check
uv run ruff format
uv run ty check
```

Configuration in `pyproject.toml`: line length 120, ignores N803/N806/N812 (uppercase variable names).

## Important Details

- **Database name**: `mwahaha` (not `pghumor` from parent project)
- **Container prefix**: `mwahaha-vote-webapp-` (not `clasificahumor-`)
- **Python tool**: `uv` for dependency management
- **All scripts** use shebang: `#!/usr/bin/env -S uv run --script --env-file ../.env`
- **Related repo**: Based on [pgHumor-clasificahumor](https://github.com/pln-fing-udelar/pghumor-clasificahumor)

## Production Setup

See README.md for continuous loops that:

1. Re-compute scores hourly
2. Ingest new submissions hourly

Uses `docker-compose.prod.yml` with nginx reverse proxy configuration.
