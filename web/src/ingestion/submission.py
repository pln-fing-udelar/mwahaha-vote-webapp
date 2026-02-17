import logging
import os
import tempfile
import zipfile
from collections.abc import AsyncIterable, Iterable
from typing import Any

import fsspec
import pandas as pd
import sqlalchemy
import sqlalchemy.dialects.mysql
import sqlalchemy.ext.asyncio
from pandas.io.sql import SQLTable

from ingestion.codabench import Submission
from mwahahavote.database import TASK_CHOICES


def print_stats(submissions: list[Submission]) -> None:
    """Prints statistics about the submissions."""
    print()
    print("Submission stats:")
    print()

    print(f"{len(submissions):>3} submissions.")

    non_deleted_submissions = [submission for submission in submissions if not submission.is_deleted]
    print(f"{len(non_deleted_submissions):>3} submissions that were not deleted.")

    print()
    print(f"Last submission date: {max(submission.date for submission in submissions)}")
    print()

    non_deleted_submissions_that_passed_a_test = [
        submission for submission in submissions if not submission.is_deleted and any(submission.tests_passed)
    ]
    print(
        f"{len(non_deleted_submissions_that_passed_a_test):>3} submissions that were not deleted and passed the test"
        f" (valid submissions)."
    )

    print()
    print(
        f"{sum(sum(submission.tests_passed) for submission in non_deleted_submissions_that_passed_a_test):>3}"
        f" valid submission-subtask pairs:"
    )
    print()

    for task in sorted(TASK_CHOICES):
        valid_task_submissions = sum(
            1
            for submission in non_deleted_submissions_that_passed_a_test
            for some_task, test_passed in zip(submission.tasks, submission.tests_passed, strict=True)
            if some_task == task and test_passed
        )
        print(f"- {task:>4}: {valid_task_submissions}")

    print()
    print("User stats:")
    print()

    print(f"{len(frozenset(submission.user for submission in submissions)):>3} users submitted at least once.")
    users_with_valid_submissions = sorted(
        frozenset(submission.user for submission in non_deleted_submissions_that_passed_a_test),
        key=lambda user: user.lower(),
    )
    print(
        f"{len(users_with_valid_submissions):>3} users that submitted at least one valid submission:"
        f" {users_with_valid_submissions}."
    )

    print()


async def list_ingested_system_ids(engine: sqlalchemy.ext.asyncio.AsyncEngine) -> AsyncIterable[str]:
    """List all system IDs in the database."""
    async with engine.begin() as connection:
        for row in await connection.execute(sqlalchemy.sql.text("SELECT system_id FROM systems")):
            yield row[0]


def _mysql_insert_on_conflict_update(
    table: SQLTable, connection: Any, keys: list[str], data_iter: Iterable[tuple]
) -> int:
    statement = sqlalchemy.dialects.mysql.insert(table.table).values(
        [dict(zip(keys, row, strict=True)) for row in data_iter]
    )
    return connection.execute(statement.on_duplicate_key_update(**statement.inserted)).rowcount


def _read_submission_file(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, delimiter="\t", index_col="id")
    df.index.rename("prompt_id", inplace=True)
    return df


async def ingest_submission(
    engine: sqlalchemy.ext.asyncio.AsyncEngine,
    phase_id: int,
    submission: Submission,
    system_exists_ok: bool = False,
    accept_null_texts: bool = True,
) -> int:
    """Ingest a submission into the database. Returns the number of affected rows."""
    async with engine.begin() as connection:
        with tempfile.TemporaryDirectory() as dir_:
            try:
                connection.execute(
                    sqlalchemy.sql.text("INSERT INTO systems (system_id) VALUES (:system_id)"),
                    {"system_id": submission.system_id},
                )
            except sqlalchemy.exc.IntegrityError:  # type: ignore[possibly-missing-attribute]
                if system_exists_ok:
                    logging.info("The system already exists in the table `systems`. Not adding a row.")
                else:
                    raise

            with fsspec.open(submission.compute_path_or_url()) as file, zipfile.ZipFile(file) as zip_file:
                zip_file.extractall(dir_)

            affected_rows = 0

            assert submission.tasks

            for task in submission.tasks:
                path = os.path.join(dir_, f"task-{task}.tsv")

                if not os.path.exists(path):
                    raise ValueError(f"The file that corresponds to the task '{task}' doesn't exist: {path}")

                if not os.path.isfile(path):
                    raise ValueError(f"The file that corresponds to the task '{task}' isn't a file: {path}")

                submission_df = _read_submission_file(path)

                reference_prompt_ids = frozenset(
                    row[0]
                    for row in await connection.execute(
                        sqlalchemy.sql.text(
                            "SELECT prompt_id FROM prompts WHERE phase_id = :phase_id AND task = :task"
                        ),
                        {"phase_id": phase_id, "task": task},
                    )
                )
                submitted_prompt_ids = frozenset(submission_df.index)

                if submitted_prompt_ids != reference_prompt_ids:
                    raise ValueError(
                        f"The submitted prompt IDs for the file from the submission '{submission}'"
                        f" do not match the reference IDs for the task '{task}'."
                        f" Missing IDs: {sorted(reference_prompt_ids - submitted_prompt_ids)}."
                        f" Extra IDs: {sorted(submitted_prompt_ids - reference_prompt_ids)}."
                    )

                if accept_null_texts:
                    if nan_prompt_ids := submission_df.index[submission_df["text"].isna()].tolist():
                        logging.warning(
                            f"Null 'text' values for the submission '{submission}' and task '{task}',"
                            f" for the following prompt IDs: {nan_prompt_ids}."
                        )

                        submission_df.loc[:, "text"].fillna("-", inplace=True)

                submission_df["system_id"] = submission.system_id

                affected_rows += await connection.run_sync(
                    lambda sync_connection, submission_df=submission_df: (
                        submission_df.to_sql("outputs", sync_connection, if_exists="append") or 0
                    )
                )

            return affected_rows
