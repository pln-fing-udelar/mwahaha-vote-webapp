#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
import asyncio
import csv
import os

import pandas as pd

import mwahahavote.database
from ingestion.codabench import EVALUATION_PHASE_ID


def read_prompt_file(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, delimiter="\t", quoting=csv.QUOTE_NONE, index_col="id", na_values="-")
    df.index.rename("prompt_id", inplace=True)

    task = os.path.splitext(os.path.basename(path))[0]
    df["task"] = task.removeprefix("task-")
    df["phase_id"] = EVALUATION_PHASE_ID

    return df


def read_prompt_files(dir_: str) -> pd.DataFrame:
    return pd.concat(
        read_prompt_file(os.path.join(dir_, filename))
        for filename in sorted(os.listdir(dir_))
        if filename.endswith(".tsv")
    )


async def async_main() -> None:
    async with mwahahavote.database.async_engine.begin() as connection:
        print(
            "Number of rows affected:",
            await connection.run_sync(
                lambda sync_connection: read_prompt_files("prompts/").to_sql(
                    "prompts", sync_connection, if_exists="append"
                )
            ),
        )


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
