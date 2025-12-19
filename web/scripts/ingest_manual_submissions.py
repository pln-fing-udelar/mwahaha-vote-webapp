#!/usr/bin/env -S uv run --script --env-file ../.env
import dataclasses
import logging
import os
import tempfile
import zipfile
from collections.abc import Iterator

from tqdm.auto import tqdm

from ingestion.codabench import Submission, list_submissions
from ingestion.submission import ingest_submission, list_ingested_system_ids, print_stats
from mwahahavote.database import Task


def available_tasks_in_file(path: str) -> Iterator[Task]:
    with tempfile.TemporaryDirectory() as dir_, zipfile.ZipFile(path) as zip_file:
        zip_file.extractall(dir_)

        for task in ["a-en", "a-es", "a-zh", "b1", "b2"]:
            task_file_path = os.path.join(dir_, f"task-{task}.tsv")
            if os.path.exists(task_file_path) and os.path.isfile(task_file_path):
                yield task


def main() -> None:
    # We sort them so they are ingested in order
    # so that the latest submission per user per task is the one that remains.

    print("Obtaining the list of all submissions so far… ", end="")
    submissions = sorted(list_submissions())
    print("✅")

    print_stats(submissions)

    # Keep only the submissions that were *deleted* or did *not pass* the test:
    submissions = [
        submission for submission in submissions if submission.is_deleted or not any(submission.tests_passed)
    ]

    already_ingested_system_ids = frozenset(list_ingested_system_ids())

    affected_rows = 0
    successful: set[Submission] = set()
    skipped: set[Submission] = set()

    for submission in tqdm(submissions, desc="Ingesting submissions", unit="submission"):
        if submission.system_id in already_ingested_system_ids:
            skipped.add(submission)
        else:
            if os.path.exists(path := f"submissions/{submission.system_id}.zip"):
                # Note we don't check the list of tasks the person submitted for but the actual files.
                submission = dataclasses.replace(submission, tasks=list(available_tasks_in_file(path)))

                try:
                    affected_rows += ingest_submission(submission, path)
                    successful.add(submission)
                except Exception:
                    logging.exception(f"Failed to ingest the submission {submission}'. See below.")

    if skipped:
        print()
        print(f"{len(skipped)}/{len(submissions)} submissions were skipped because there were already ingested before.")

    attempted = set(submissions) - skipped

    if successful:
        print()
        print(f"The following {len(successful)}/{len(attempted)} new submissions ingested successfully:")
        for submission in sorted(successful):
            print(f"- {submission}")

        print()
        print(f"{affected_rows} affected rows.")
        print()

    if failed := attempted - successful:
        print()
        print(f"The following {len(failed)}/{len(attempted)} new submissions failed to be ingested:")
        for submission in sorted(failed):
            print(f"- {submission}")


if __name__ == "__main__":
    main()
