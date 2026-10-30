#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
lazy import asyncio
lazy import datetime

lazy import mwahahavote.database
lazy from ingestion.codabench import Submission
lazy from ingestion.submission import ingest_submission
lazy from mwahahavote.config import ACTIVE_COMPETITION


async def async_main() -> None:
    async with mwahahavote.database.create_engine() as engine:
        await ingest_submission(
            engine,
            ACTIVE_COMPETITION.phase_id,
            Submission(
                user="baseline",
                id=1,
                date=datetime.datetime.now(),
                tasks=list(ACTIVE_COMPETITION.task_choices),
                tests_passed=[True] * len(ACTIVE_COMPETITION.task_choices),
                path_or_url="baselines/baseline.zip",
            ),
            system_exists_ok=True,
        )


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
