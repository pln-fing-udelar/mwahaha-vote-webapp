#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
lazy import asyncio
lazy import csv
lazy import os

lazy import pandas as pd

lazy import mwahahavote.database
lazy from mwahahavote.config import ACTIVE_COMPETITION


def _extract_task_from_path(path: str) -> str:
    basename = os.path.splitext(os.path.basename(path))[0]
    if basename.startswith("task-"):
        candidate = basename.removeprefix("task-")
        if candidate in ACTIVE_COMPETITION.task_ids:
            return candidate
    if basename in ACTIVE_COMPETITION.task_ids:
        return basename
    for task_id in ACTIVE_COMPETITION.task_ids:
        if task_id in basename:
            return task_id
    return ACTIVE_COMPETITION.default_task


def read_prompt_file(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, delimiter="\t", quoting=csv.QUOTE_NONE, index_col="id", na_values="-")
    df.index.rename("prompt_id", inplace=True)

    df["task"] = _extract_task_from_path(path)
    df["phase_id"] = ACTIVE_COMPETITION.phase_id

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
