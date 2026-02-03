#!/usr/bin/env -S uv run --script --env-file ../.env
"""A script that ingests all submissions from CodaBench and any submissions manually set under `submissions/`.

The submissions that were already ingested are skipped.
"""

import dataclasses
import logging
import os
import tempfile
import zipfile
from collections.abc import Iterator

import fsspec
from tqdm.auto import tqdm

from ingestion.codabench import EVALUATION_PHASE_ID, Submission, get_submission_url, list_submissions
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

    print("Obtaining the list of all submissions so far… ", end="")
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

    manual_submissions = sorted(
        submission for submission in all_submissions if os.path.exists(f"submissions/{submission.system_id}.zip")
    )
    valid_submissions.extend(manual_submissions)
    print(f"Added {len(manual_submissions)} manual submissions placed under `submissions/`.")

    # We only leave the last submission from each user:

    submissions_to_ingest: list[Submission] = []
    users: set[str] = set()
    for submission in reversed(valid_submissions):
        if submission.user not in users:
            users.add(submission.user)
            submissions_to_ingest.append(submission)

    already_ingested_system_ids = frozenset(list_ingested_system_ids())

    affected_rows = 0
    successful: set[Submission] = set()
    skipped: set[Submission] = set()

    for submission in tqdm(submissions_to_ingest, desc="Ingesting submissions", unit="submission"):
        if submission.system_id in already_ingested_system_ids:
            skipped.add(submission)
        else:
            context_manager = None
            # noinspection PyBroadException
            try:
                # Note we don't check the list of tasks the person submitted for but the actual files.
                if os.path.exists(path := f"submissions/{submission.system_id}.zip"):
                    submission = dataclasses.replace(submission, tasks=list(available_tasks_in_file(path)))
                    path_or_file = path
                else:
                    # Note we don't check the list of tasks the person submitted for but the actual files.
                    url = get_submission_url(submission.id)
                    context_manager = fsspec.open(url)
                    path_or_file = context_manager.__enter__()

                affected_rows += ingest_submission(submission, path_or_file, phase_id=EVALUATION_PHASE_ID)
                successful.add(submission)
            except Exception:
                logging.exception(f"Failed to ingest the submission '{submission}'. See below.")
            finally:
                if context_manager is not None:
                    context_manager.__exit__(None, None, None)

    if skipped:
        print()
        print(
            f"{len(skipped)}/{len(submissions_to_ingest)} submissions were skipped because there were already ingested"
            " before."
        )

    attempted = set(submissions_to_ingest) - skipped

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
