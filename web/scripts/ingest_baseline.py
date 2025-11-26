#!/usr/bin/env python
import datetime

from ingestion.codabench import Submission
from ingestion.submission import ingest_submission


def main() -> None:
    ingest_submission(
        Submission(
            user="baseline",
            id=1,
            date=datetime.datetime.now(),
            tasks=["a-en", "a-zh", "b2"],
            tests_passed=[True, True, True],
        ),
        file="baselines/baseline.zip",
    )


if __name__ == "__main__":
    main()
