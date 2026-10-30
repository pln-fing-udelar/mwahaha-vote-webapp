#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
lazy import argparse
lazy import asyncio
lazy import json
lazy import os
lazy import pickle

lazy import pandas as pd

from mwahahavote.config import ACTIVE_COMPETITION, COMPETITION_CHOICES, CompetitionConfig, Task, get_competition
lazy import mwahahavote.database

PHASE_ID = ACTIVE_COMPETITION.phase_id


async def async_main(competition: CompetitionConfig | None = None) -> None:
    if competition is None:
        competition = ACTIVE_COMPETITION

    async with mwahahavote.database.create_engine() as engine:
        for task in sorted(competition.task_choices):
            task: Task
            # Note we aren't excluding any session ID here,
            # but it's fine because these vote counts are only used when a system doesn't have a score yet.
            system_id_to_vote_count = await mwahahavote.database.get_votes_per_system(
                engine, competition.phase_id, task
            )

            output_path = f"src/mwahahavote/static/scores/{task}.json"
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            if os.path.exists(input_path := f"scoring/elo_results_{task}.pkl"):
                with open(input_path, "rb") as file:
                    df: pd.DataFrame = pickle.load(file)["full"]["leaderboard_table_df"]
                    df.index.name = "system"
                    df = df.sort_values(by=["rating"], ascending=False)

                    considered_system_ids = set(df.index)
                    for system_id, votes in system_id_to_vote_count.items():
                        if system_id not in considered_system_ids:
                            df.loc[system_id] = [
                                float("NaN"),  # rating
                                float("NaN"),  # variance
                                float("NaN"),  # rating_q975
                                float("NaN"),  # rating_q025
                                votes,
                                float("NaN"),  # final_ranking
                            ]

                    df.num_battles = df.num_battles.astype(int)  # ty:ignore[unresolved-attribute]

                    df.reset_index().to_json(output_path, orient="records")
            else:
                with open(output_path, "w") as file:
                    json.dump(
                        [
                            {
                                "system": system_id,
                                "rating": None,
                                "variance": None,
                                "rating_q975": None,
                                "rating_q025": None,
                                "num_battles": votes,
                                "final_ranking": None,
                            }
                            for system_id, votes in system_id_to_vote_count.items()
                        ],
                        file,
                    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Postprocess FastChat Elo results into leaderboard JSON.")
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
