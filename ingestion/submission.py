import os
import tempfile
import zipfile
from collections.abc import Iterable
from typing import Any, cast

import pandas as pd
import sqlalchemy
import sqlalchemy.dialects.mysql
from pandas.io.sql import SQLTable
from typing_extensions import Reader

from mwahahavote.database import TASK_CHOICES, Task, engine, task_to_prompt_id_sql_like_expression


def _mysql_insert_on_conflict_update(
    table: SQLTable, connection: Any, keys: list[str], data_iter: Iterable[tuple]
) -> int:
    statement = sqlalchemy.dialects.mysql.insert(table.table).values(
        [dict(zip(keys, row, strict=True)) for row in data_iter]
    )
    return connection.execute(statement.on_duplicate_key_update(**statement.inserted)).rowcount


def read_submission(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, delimiter="\t", index_col="id")  # type: ignore
    df.index.rename("prompt_id", inplace=True)
    return df


def ingest_submission(file: str | Reader[bytes], user: str) -> int:
    """Ingest a submission file into the database.

    Returns the number of affected rows.
    """
    with engine.begin() as connection, tempfile.TemporaryDirectory() as dir_:
        connection.execute(
            sqlalchemy.sql.text("INSERT IGNORE INTO systems (system_id) VALUES (:system_id)"), {"system_id": user}
        )

        with zipfile.ZipFile(file) as zip_file:
            zip_file.extractall(dir_)

        affected_rows = 0

        for filename in sorted(zip_file.namelist()):
            filename = filename.lower()
            task, ext = os.path.splitext(filename)
            task = task.removeprefix("task-")

            if ext == ".tsv" and os.path.isfile(submission_path := os.path.join(dir_, filename)):
                if task not in TASK_CHOICES:
                    raise ValueError(f"Unknown task '{task}' from the filename '{filename}'.")

                task = cast(Task, task)

                submission_df = read_submission(submission_path)

                cursor = connection.execute(
                    sqlalchemy.sql.text("SELECT prompt_id FROM prompts WHERE prompt_id LIKE :prompt_id_like"),
                    {"prompt_id_like": task_to_prompt_id_sql_like_expression(task)},
                )
                reference_prompt_ids = frozenset(t[0] for t in cursor.fetchall())
                submitted_prompt_ids = frozenset(submission_df.index)

                if submitted_prompt_ids != reference_prompt_ids:
                    raise ValueError(
                        f"The submitted prompt IDs for the file from the user '{user}'"
                        f" do not match the reference IDs for the task '{task}'."
                        f" Missing IDs: {sorted(reference_prompt_ids - submitted_prompt_ids)}."
                        f" Extra IDs: {sorted(submitted_prompt_ids - reference_prompt_ids)}."
                    )

                submission_df["system_id"] = user
                affected_rows += (
                    submission_df.to_sql(
                        "outputs", connection, if_exists="append", method=_mysql_insert_on_conflict_update
                    )
                    or 0
                )

        return affected_rows
