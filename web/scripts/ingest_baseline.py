#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
lazy import argparse
lazy import asyncio
lazy import datetime

lazy import mwahahavote.database
lazy from ingestion.codabench import Submission
lazy from ingestion.submission import ingest_submission
lazy from mwahahavote.config import ACTIVE_COMPETITION, COMPETITION_CHOICES, CompetitionConfig, get_competition


async def async_main(competition: CompetitionConfig | None = None) -> None:
    if competition is None:
        competition = ACTIVE_COMPETITION

    async with mwahahavote.database.create_engine() as engine:
        await ingest_submission(
            engine,
            competition.phase_id,
            Submission(
                user="baseline",
                id=1,
                date=datetime.datetime.now(),
                tasks=list(competition.task_choices),
                tests_passed=[True] * len(competition.task_choices),
                path_or_url="baselines/baseline.zip",
            ),
            system_exists_ok=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest baseline submission into the database.")
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
