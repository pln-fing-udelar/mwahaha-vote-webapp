#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
lazy import argparse
lazy import asyncio
lazy import csv
lazy import os

lazy import pandas as pd

lazy import mwahahavote.database
lazy from mwahahavote.config import ACTIVE_COMPETITION, COMPETITION_CHOICES, CompetitionConfig, get_competition


def _extract_task_from_path(path: str, competition: CompetitionConfig | None = None) -> str:
    if competition is None:
        competition = ACTIVE_COMPETITION
    basename = os.path.splitext(os.path.basename(path))[0]
    if basename.startswith("task-"):
        candidate = basename.removeprefix("task-")
        if candidate in competition.task_ids:
            return candidate
    if basename in competition.task_ids:
        return basename
    for task_id in competition.task_ids:
        if task_id in basename:
            return task_id
    return competition.default_task


def read_prompt_file(path: str, competition: CompetitionConfig | None = None) -> pd.DataFrame:
    if competition is None:
        competition = ACTIVE_COMPETITION
    df = pd.read_csv(path, delimiter="\t", quoting=csv.QUOTE_NONE, index_col="id", na_values="-", encoding="utf-8-sig")
    df.index.name = "prompt_id"

    df["task"] = _extract_task_from_path(path, competition)
    df["phase_id"] = competition.phase_id

    return df


def read_prompt_files(dir_: str, competition: CompetitionConfig | None = None) -> pd.DataFrame:
    if competition is None:
        competition = ACTIVE_COMPETITION
    tsv_files = [filename for filename in sorted(os.listdir(dir_)) if filename.endswith(".tsv")]
    if not tsv_files:
        raise ValueError(f"No .tsv prompt files found in '{dir_}'.")
    return pd.concat(read_prompt_file(os.path.join(dir_, filename), competition) for filename in tsv_files)


async def async_main(competition: CompetitionConfig | None = None) -> None:
    if competition is None:
        competition = ACTIVE_COMPETITION

    async with mwahahavote.database.create_engine() as engine, engine.begin() as connection:
        print(
            "Number of rows affected:",
            await connection.run_sync(
                lambda sync_connection: read_prompt_files("prompts/", competition).to_sql(
                    "prompts", sync_connection, if_exists="append"
                )
            ),
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest prompts into the database.")
    parser.add_argument(
        "--competition",
        "-c",
        choices=COMPETITION_CHOICES,
        default=None,
        help=f"Competition to target (default: {ACTIVE_COMPETITION.id}, or $COMPETITION env var).",
    )
    args, _ = parser.parse_known_args()
    competition = get_competition(args.competition) if args.competition is not None else None
    asyncio.run(async_main(competition=competition))


if __name__ == "__main__":
    main()
