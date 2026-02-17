#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
import asyncio
import json
import os
import pickle

import pandas as pd

import mwahahavote.database
from ingestion.codabench import EVALUATION_PHASE_ID
from mwahahavote.database import TASK_CHOICES

PHASE_ID = EVALUATION_PHASE_ID


async def async_main() -> None:
    async with mwahahavote.database.create_engine() as engine:
        for task in sorted(TASK_CHOICES):
            system_id_to_vote_count = await mwahahavote.database.get_votes_per_system(engine, PHASE_ID, task)

            output_path = f"src/mwahahavote/static/scores/{task}.json"
            if os.path.exists(input_path := f"scoring/elo_results_{task}.pkl"):
                with open(input_path, "rb") as file:
                    df: pd.DataFrame = pickle.load(file)["full"]["leaderboard_table_df"]
                    df.index.name = "system"
                    df.sort_values(by=["rating"], ascending=False, inplace=True)

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

                    df.num_battles = df.num_battles.astype(int)

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
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
