#!/usr/bin/env -S uv run --script --env-file ../.env
import datetime

from ingestion.codabench import EVALUATION_PHASE_ID, Submission
from ingestion.submission import ingest_submission
from mwahahavote.database import TASK_CHOICES


def main() -> None:
    ingest_submission(
        Submission(
            user="baseline",
            id=1,
            date=datetime.datetime.now(),
            tasks=list(TASK_CHOICES),
            tests_passed=[True, True, True, True, True],
        ),
        file="baselines/baseline.zip",
        phase_id=EVALUATION_PHASE_ID,
        system_exists_ok=True,
    )


if __name__ == "__main__":
    main()
