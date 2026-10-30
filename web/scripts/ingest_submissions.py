#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
"""A script that ingests all submissions from CodaBench and any submission manually set under `submissions/`.

The submissions that were already ingested are skipped.
"""

lazy import argparse
lazy import asyncio
lazy import dataclasses
lazy import datetime
lazy import logging
lazy import os
lazy import tempfile
lazy import zipfile
lazy from collections.abc import Iterator

lazy import pandas as pd
lazy from tqdm.auto import tqdm

lazy import mwahahavote.database
lazy from ingestion.codabench import Submission, list_submissions
lazy from ingestion.submission import ingest_submission, list_ingested_system_ids, print_stats
lazy from mwahahavote.config import ACTIVE_COMPETITION, is_only_latest_submission_per_user
lazy from mwahahavote.database import Task


def available_tasks_in_file(path: str) -> Iterator[Task]:
    with tempfile.TemporaryDirectory() as dir_, zipfile.ZipFile(path) as zip_file:
        zip_file.extractall(dir_)

        for task in sorted(ACTIVE_COMPETITION.task_choices):
            task: Task
            task_file_path = os.path.join(dir_, f"task-{task}.tsv")
            alt_task_file_path = os.path.join(dir_, f"{task}.tsv")
            if (os.path.exists(task_file_path) and os.path.isfile(task_file_path)) or (
                os.path.exists(alt_task_file_path) and os.path.isfile(alt_task_file_path)
            ):
                yield task


def _collect_codabench_submissions() -> list[Submission]:
    print("Obtaining the list of all CodaBench submissions so far… ", end="")
    all_submissions = sorted(list_submissions())
    print("✅")

    print_stats(all_submissions)

    return [
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


def _collect_manual_submissions() -> list[Submission]:
    if not (os.path.exists("submissions") and os.path.isdir("submissions")):
        return []

    manual_submissions: list[Submission] = []
    for filename in sorted(os.listdir("submissions")):
        path = os.path.join("submissions", filename)
        if not os.path.isfile(path):
            continue
        if filename.endswith(".zip"):
            tasks = list(available_tasks_in_file(path))
            if tasks:
                manual_submissions.append(
                    Submission(
                        id=len(manual_submissions),
                        user=os.path.splitext(filename)[0],
                        date=datetime.datetime.now(datetime.UTC),
                        tasks=tasks,
                        tests_passed=[True] * len(tasks),
                        is_deleted=False,
                        path_or_url=path,
                        system_id_override=os.path.splitext(filename)[0],
                    )
                )
        elif filename.endswith(".tsv"):
            manual_submissions.append(
                Submission(
                    id=len(manual_submissions),
                    user=os.path.splitext(filename)[0],
                    date=datetime.datetime.now(datetime.UTC),
                    tasks=list(ACTIVE_COMPETITION.task_choices),
                    tests_passed=[True] * len(ACTIVE_COMPETITION.task_choices),
                    is_deleted=False,
                    path_or_url=path,
                    system_id_override=os.path.splitext(filename)[0],
                )
            )
    return manual_submissions


def _filter_submissions_to_ingest(
    submissions: list[Submission],
    only_latest_per_user: bool,
) -> list[Submission]:
    if only_latest_per_user:
        submissions_to_ingest: list[Submission] = []
        users: set[str] = set()
        for submission in reversed(submissions):
            if submission.user not in users:
                users.add(submission.user)
                submissions_to_ingest.append(submission)
        print(
            f"Left only the latest submission from each user, "
            f"resulting in {len(submissions_to_ingest)} submissions to ingest.\n"
        )
        return submissions_to_ingest

    print(f"Keeping each submission for each user, resulting in {len(submissions)} submissions to ingest.\n")
    return list(submissions)


def _print_ingestion_results(
    submissions_to_ingest: list[Submission],
    skipped: set[Submission],
    successful: set[Submission],
    affected_rows: int,
) -> None:
    print()

    if skipped:
        print(
            f"{len(skipped)}/{len(submissions_to_ingest)} submissions were skipped because"
            " they were already ingested before.\n"
        )

    attempted = set(submissions_to_ingest) - skipped

    if successful:
        print(f"The following {len(successful)}/{len(attempted)} new submissions ingested successfully:")
        for submission in sorted(successful):
            print(f"- {submission}")
        print(f"\n{affected_rows} affected rows.\n")

    if failed := attempted - successful:
        print(f"The following {len(failed)}/{len(attempted)} new submissions failed to be ingested:")
        for submission in sorted(failed):
            print(f"- {submission}")
        print()

    print("Final stats after ingestion:")
    print_stats(submissions_to_ingest)

    print(
        pd.DataFrame(
            {
                "user": submission.user,
                **{task: (task in submission.tasks) for task in sorted(ACTIVE_COMPETITION.task_choices)},
            }
            for submission in sorted(submissions_to_ingest, key=lambda s: s.user)
        )
    )


async def async_main(only_latest_per_user: bool | None = None) -> None:
    # We sort them so they are ingested in order
    # so that the latest submission per user per task is the one that remains.

    if only_latest_per_user is None:
        only_latest_per_user = is_only_latest_submission_per_user()

    valid_submissions: list[Submission] = []

    if ACTIVE_COMPETITION.allow_codabench:
        valid_submissions.extend(_collect_codabench_submissions())

    manual_submissions = _collect_manual_submissions()
    valid_submissions.extend(manual_submissions)
    print(f"Obtained {len(manual_submissions)} manual submissions placed under `submissions/`.\n")

    submissions_to_ingest = _filter_submissions_to_ingest(valid_submissions, only_latest_per_user)

    async with mwahahavote.database.create_engine() as engine:
        already_ingested_system_ids = frozenset({system_id async for system_id in list_ingested_system_ids(engine)})

        affected_rows = 0
        successful: set[Submission] = set()
        skipped: set[Submission] = set()

        for submission in tqdm(submissions_to_ingest, desc="Ingesting submissions", unit="submission"):
            if submission.system_id in already_ingested_system_ids:
                skipped.add(submission)
            else:
                # noinspection PyBroadException
                try:
                    affected_rows += await ingest_submission(engine, ACTIVE_COMPETITION.phase_id, submission)
                    successful.add(submission)
                except Exception:
                    logging.exception(f"Failed to ingest the submission '{submission}'. See below.")

        _print_ingestion_results(submissions_to_ingest, skipped, successful, affected_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest submissions into the database.")
    parser.add_argument(
        "--all-submissions",
        "--each-submission",
        action="store_true",
        default=False,
        help="Ingest each submission for each user rather than only the latest.",
    )
    parser.add_argument(
        "--only-latest",
        action="store_true",
        default=False,
        help="Ingest only the latest submission for each user.",
    )
    args, _ = parser.parse_known_args()

    only_latest: bool | None = None
    if args.all_submissions:
        only_latest = False
    elif args.only_latest:
        only_latest = True

    asyncio.run(async_main(only_latest_per_user=only_latest))


if __name__ == "__main__":
    main()
