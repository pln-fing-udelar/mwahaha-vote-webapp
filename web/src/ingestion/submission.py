lazy import logging
lazy import os
lazy import tempfile
lazy import zipfile
lazy from collections.abc import AsyncIterable, Iterable
lazy from typing import Any

lazy import fsspec
lazy import pandas as pd
lazy import sqlalchemy
lazy import sqlalchemy.dialects.mysql
lazy import sqlalchemy.ext.asyncio
lazy from pandas.io.sql import SQLTable

lazy from ingestion.codabench import Submission
lazy from mwahahavote.config import ACTIVE_COMPETITION, CompetitionConfig


def print_stats(submissions: list[Submission], competition: CompetitionConfig | None = None) -> None:
    """Prints statistics about the submissions."""
    if competition is None:
        competition = ACTIVE_COMPETITION

    print()
    print("Submission stats:")
    print()

    if not submissions:
        print("  0 submissions.")
        return

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

    for task in sorted(competition.task_choices):
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
    df = pd.read_csv(path, delimiter="\t", encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    if "id" not in df.columns:
        raise ValueError(f"Submission file '{path}' is missing the required 'id' column.")
    df = df.set_index("id")
    df.index.name = "prompt_id"
    return df


def _safe_extract_zip(zip_file: zipfile.ZipFile, extract_dir: str) -> None:
    target_dir = os.path.abspath(extract_dir)
    for member in zip_file.infolist():
        dest_path = os.path.abspath(os.path.join(target_dir, member.filename))
        if not (dest_path == target_dir or dest_path.startswith(target_dir + os.sep)):
            raise ValueError(f"Path traversal detected in zip archive member: {member.filename}")
    zip_file.extractall(target_dir)


def _extract_submission_archive(path_or_url: str, extract_dir: str) -> bool:
    is_url = path_or_url.startswith(("http://", "https://"))
    if is_url:
        is_zip = path_or_url.endswith(".zip")
    else:
        if not os.path.exists(path_or_url):
            raise ValueError(f"The file that corresponds to the submission doesn't exist: {path_or_url}")
        if not os.path.isfile(path_or_url):
            raise ValueError(f"The path that corresponds to the submission isn't a file: {path_or_url}")
        is_zip = path_or_url.endswith(".zip") or zipfile.is_zipfile(path_or_url)

    if is_zip:
        with fsspec.open(path_or_url, "rb") as file, zipfile.ZipFile(file) as zip_file:
            _safe_extract_zip(zip_file, extract_dir)

    return is_zip


def _resolve_task_file_path(task: str, path_or_url: str, extract_dir: str, is_zip: bool) -> str:
    if is_zip:
        path = os.path.join(extract_dir, f"task-{task}.tsv")
        if not os.path.exists(path):
            alt_path = os.path.join(extract_dir, f"{task}.tsv")
            if os.path.exists(alt_path):
                path = alt_path
            else:
                # Search nested subdirectories in case the participant zipped a folder
                for root, _, files in os.walk(extract_dir):
                    if f"task-{task}.tsv" in files:
                        path = os.path.join(root, f"task-{task}.tsv")
                        break
                    if f"{task}.tsv" in files:
                        path = os.path.join(root, f"{task}.tsv")
                        break
    else:
        path = path_or_url

    if not os.path.exists(path):
        raise ValueError(f"The file that corresponds to the task '{task}' doesn't exist: {path}")

    if not os.path.isfile(path):
        raise ValueError(f"The file that corresponds to the task '{task}' isn't a file: {path}")

    return path


async def ingest_submission(
    engine: sqlalchemy.ext.asyncio.AsyncEngine,
    phase_id: int,
    submission: Submission,
    system_exists_ok: bool = False,
    accept_null_texts: bool = True,
) -> int:
    """Ingest a submission into the database. Returns the number of affected rows."""
    async with engine.begin() as connection:
        try:
            await connection.execute(
                sqlalchemy.sql.text("INSERT INTO systems (system_id) VALUES (:system_id)"),
                {"system_id": submission.system_id},
            )
        except sqlalchemy.exc.IntegrityError:  # ty:ignore[possibly-missing-submodule]
            if system_exists_ok:
                logging.info("The system already exists in the table `systems`. Not adding a row.")
            else:
                raise

        path_or_url = submission.compute_path_or_url()

        with tempfile.TemporaryDirectory() as dir_:
            is_zip = _extract_submission_archive(path_or_url, dir_)
            affected_rows = 0

            assert submission.tasks

            for task in submission.tasks:
                path = _resolve_task_file_path(task, path_or_url, dir_, is_zip)
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

                        submission_df["text"] = submission_df["text"].fillna("-")

                submission_df["system_id"] = submission.system_id

                affected_rows += await connection.run_sync(
                    lambda sync_connection, submission_df=submission_df: (
                        submission_df.to_sql("outputs", sync_connection, if_exists="append") or 0
                    )
                )

            return affected_rows
