#!/usr/bin/env -S uv run --script --env-file ../.env
"""A script that ingests all submissions from CodaBench and any submission manually set under `submissions/`.

The submissions that were already ingested are skipped.
"""

import dataclasses
import datetime
import logging
import os
import tempfile
import zipfile
from collections.abc import Iterator

import pandas as pd
from tqdm.auto import tqdm

from ingestion.codabench import EVALUATION_PHASE_ID, Submission, list_submissions
from ingestion.submission import ingest_submission, list_ingested_system_ids, print_stats
from mwahahavote.database import TASK_CHOICES, Task


def available_tasks_in_file(path: str) -> Iterator[Task]:
    with tempfile.TemporaryDirectory() as dir_, zipfile.ZipFile(path) as zip_file:
        zip_file.extractall(dir_)

        for task in sorted(TASK_CHOICES):
            task_file_path = os.path.join(dir_, f"task-{task}.tsv")
            if os.path.exists(task_file_path) and os.path.isfile(task_file_path):
                yield task


def main() -> None:  # noqa: C901
    # We sort them so they are ingested in order
    # so that the latest submission per user per task is the one that remains.

    print("Obtaining the list of all CodaBench submissions so far… ", end="")
    all_submissions = sorted(list_submissions())
    print("✅")

    print_stats(all_submissions)

    # Keep only the valid submissions:
    valid_submissions = [
        dataclasses.replace(
            submission,
            tasks=[
                task for task, test_passed in zip(submission.tasks, submission.tests_passed, strict=True) if test_passed
            ],
            tests_passed=[True] * sum(submission.tests_passed),
        )
        for submission in all_submissions
        if not submission.is_deleted and any(submission.tests_passed)
    ]

    # Now process any submission that was set manually placed under `submissions/`.
    # Note we leave the manual submissions at the end to override any previous one.

    manual_submissions = [
        Submission(
            id=i,
            user=os.path.splitext(filename)[0],
            date=datetime.datetime.now(datetime.UTC),
            tasks=(tasks := list(available_tasks_in_file(path))),  # type: ignore[invalid-argument-type]
            tests_passed=[True] * len(tasks),
            is_deleted=False,
            path_or_url=path,
        )
        for i, filename in enumerate(os.listdir("submissions"))
        if filename.endswith(".zip") and os.path.isfile(path := os.path.join("submissions", filename))
    ]
    valid_submissions.extend(manual_submissions)
    print(f"Obtained {len(manual_submissions)} manual submissions placed under `submissions/`.")
    print()

    # We only leave the last submission from each user:

    submissions_to_ingest: list[Submission] = []
    users: set[str] = set()
    for submission in reversed(valid_submissions):
        if submission.user not in users:
            users.add(submission.user)
            submissions_to_ingest.append(submission)

    print(
        "Left only the latest submission from each user,"
        f" resulting in {len(submissions_to_ingest)} submissions to ingest."
    )
    print()

    already_ingested_system_ids = frozenset(list_ingested_system_ids())

    affected_rows = 0
    successful: set[Submission] = set()
    skipped: set[Submission] = set()

    for submission in tqdm(submissions_to_ingest, desc="Ingesting submissions", unit="submission"):
        if submission.system_id in already_ingested_system_ids:
            skipped.add(submission)
        else:
            # noinspection PyBroadException
            try:
                affected_rows += ingest_submission(EVALUATION_PHASE_ID, submission)
                successful.add(submission)
            except Exception:
                logging.exception(f"Failed to ingest the submission '{submission}'. See below.")

    print()

    if skipped:
        print(
            f"{len(skipped)}/{len(submissions_to_ingest)} submissions were skipped because there were already ingested"
            " before."
        )
        print()

    attempted = set(submissions_to_ingest) - skipped

    if successful:
        print(f"The following {len(successful)}/{len(attempted)} new submissions ingested successfully:")
        for submission in sorted(successful):
            print(f"- {submission}")

        print()
        print(f"{affected_rows} affected rows.")
        print()

    if failed := attempted - successful:
        print(f"The following {len(failed)}/{len(attempted)} new submissions failed to be ingested:")
        for submission in sorted(failed):
            print(f"- {submission}")
        print()

    print("Final stats after ingestion:")

    print_stats(submissions_to_ingest)

    print(
        pd.DataFrame(
            {"user": submission.user, **{task: (task in submission.tasks) for task in sorted(TASK_CHOICES)}}
            for submission in sorted(submissions_to_ingest, key=lambda submission: submission.user)
        )
    )


if __name__ == "__main__":
    main()
