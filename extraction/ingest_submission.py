#!/usr/bin/env python
import csv
import os
import shutil
import sys
import zipfile
from typing import Literal, cast, get_args

import pandas as pd
import sqlalchemy

from extraction import util

Task = Literal["task-a-en", "task-a-es", "task-a-zh", "task-b1", "task-b2"]


def read_submission(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, delimiter="\t", quoting=csv.QUOTE_NONE, index_col="id")  # type: ignore
    df.index.rename("prompt_id", inplace=True)
    return df


def task_to_id_sql_like_expression(task: Task) -> str:
    match task:
        case "task-a-en":
            return r"en\_____"
        case "task-a-es":
            return r"es\_____"
        case "task-a-zh":
            return r"zh\_____"
        case "task-b1":
            return r"img\_____"
        case "task-b2":
            return r"img\_2\_____"
        case _:
            raise ValueError(f"Unknown task: {task}")


def main() -> None:
    path = sys.argv[1]
    user = os.path.splitext(os.path.basename(path))[0]

    engine = util.create_engine()
    with engine.begin() as connection, zipfile.ZipFile(path) as zip_file:
        connection.execute(
            sqlalchemy.sql.text("INSERT INTO systems (system_id) VALUES (:system_id)"), {"system_id": user}
        )

        if os.path.exists(extract_dir := os.path.splitext(path)[0]):
            shutil.rmtree(extract_dir)

        zip_file.extractall(extract_dir)

        for filename in sorted(zip_file.namelist()):
            filename = filename.lower()
            task, ext = os.path.splitext(filename)

            if ext == ".tsv" and os.path.isfile(submission_path := os.path.join(extract_dir, filename)):
                if task not in get_args(Task):
                    raise ValueError(f"Unknown task '{task}' from the filename '{filename}'.")

                task = cast(Task, task)

                submission_df = read_submission(submission_path)

                cursor = connection.execute(
                    sqlalchemy.sql.text("SELECT prompt_id FROM prompts WHERE prompt_id LIKE :prompt_id_like"),
                    {"prompt_id_like": task_to_id_sql_like_expression(task)},
                )
                reference_prompt_ids = frozenset(t[0] for t in cursor.fetchall())
                submitted_prompt_ids = frozenset(submission_df.index)

                if submitted_prompt_ids != reference_prompt_ids:
                    raise ValueError(
                        f"Submitted prompt IDs do not match reference for the task '{task}'."
                        f" Missing IDs: {reference_prompt_ids - submitted_prompt_ids}."
                        f" Extra IDs: {submitted_prompt_ids - reference_prompt_ids}."
                    )

                submission_df["system_id"] = user
                submission_df.to_sql("outputs", connection, if_exists="append")


if __name__ == "__main__":
    main()
