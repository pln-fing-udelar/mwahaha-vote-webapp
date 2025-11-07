#!/usr/bin/env -S uv run --script --env-file ../.env
import csv
import os

import pandas as pd

import mwahahavote.database


def read_prompt_file(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, delimiter="\t", quoting=csv.QUOTE_NONE, index_col="id", na_values="-")  # type: ignore
    df.index.rename("prompt_id", inplace=True)

    task = os.path.splitext(os.path.basename(path))[0]
    df["task"] = task.removeprefix("task-")

    return df


def read_prompt_files(dir_: str) -> pd.DataFrame:
    return pd.concat(
        read_prompt_file(os.path.join(dir_, filename))
        for filename in sorted(os.listdir(dir_))
        if filename.endswith(".tsv")
    )


def main() -> None:
    print(
        "Number of rows affected:",
        read_prompt_files("prompts/").to_sql("prompts", mwahahavote.database.create_engine(), if_exists="append"),
    )


if __name__ == "__main__":
    main()
