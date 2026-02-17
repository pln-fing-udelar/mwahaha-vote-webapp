#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
import asyncio
import datetime

import mwahahavote.database
from ingestion.codabench import EVALUATION_PHASE_ID, Submission
from ingestion.submission import ingest_submission
from mwahahavote.database import TASK_CHOICES


async def async_main() -> None:
    async with mwahahavote.database.create_engine() as engine:
        await ingest_submission(
            engine,
            EVALUATION_PHASE_ID,
            Submission(
                user="baseline",
                id=1,
                date=datetime.datetime.now(),
                tasks=list(TASK_CHOICES),
                tests_passed=[True] * len(TASK_CHOICES),
                path_or_url="baselines/baseline.zip",
            ),
            system_exists_ok=True,
        )


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
