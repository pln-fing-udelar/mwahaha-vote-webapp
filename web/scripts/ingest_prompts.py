#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
import asyncio
import csv
import os

import pandas as pd

import mwahahavote.database


def read_prompt_file(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, delimiter="\t", quoting=csv.QUOTE_NONE, index_col="id", na_values="-")
    df.index.rename("prompt_id", inplace=True)

    # task = os.path.splitext(os.path.basename(path))[0]
    df["task"] = "t3"
    df["phase_id"] = 1

    return df


def read_prompt_files(dir_: str) -> pd.DataFrame:
    return pd.concat(
        read_prompt_file(os.path.join(dir_, filename))
        for filename in sorted(os.listdir(dir_))
        if filename.endswith(".tsv")
    )


async def async_main() -> None:
    async with mwahahavote.database.create_engine() as engine, engine.begin() as connection:
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
