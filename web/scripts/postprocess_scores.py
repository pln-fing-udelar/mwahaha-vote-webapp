#!/usr/bin/env python
import os
import pickle

import pandas as pd

from mwahahavote.database import TASK_CHOICES


def main() -> None:
    for task in sorted(TASK_CHOICES):
        output_path = f"src/mwahahavote/static/scores/{task}.json"
        if os.path.exists(input_path := f"scoring/elo_results_{task}.pkl"):
            with open(input_path, "rb") as file:
                df: pd.DataFrame = pickle.load(file)["full"]["leaderboard_table_df"]
                df.index.name = "system"
                df.sort_values(by=["rating"], ascending=False, inplace=True)
                df.reset_index().to_json(output_path, orient="records")
        else:
            with open(output_path, "w") as file:
                file.write("[]")


if __name__ == "__main__":
    main()
