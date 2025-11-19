#!/usr/bin/env python
import datetime

from ingestion.codabench import Submission
from ingestion.submission import ingest_submission


def main() -> None:
    with open("baselines/baseline.zip", "rb") as file:
        ingest_submission(
            Submission(
                user="baseline", id=1, date=datetime.datetime.now(), tasks=["a-en", "b2"], tests_passed=[True, True]
            ),
            file,
        )


if __name__ == "__main__":
    main()
