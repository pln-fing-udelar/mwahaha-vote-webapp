#!/usr/bin/env python
import os
import pickle

import pandas as pd

from mwahahavote.database import TASK_CHOICES


def main() -> None:
    for task in sorted(TASK_CHOICES):
        path = f"scoring/elo_results_{task}.pkl"
        if os.path.exists(path):
            with open(path, "rb") as file:
                df: pd.DataFrame = pickle.load(file)["full"]["leaderboard_table_df"]
                df.index.name = "system"
                df.sort_values(by=["rating"], ascending=False, inplace=True)
                df.to_csv(f"scoring/elo_results_{task}.csv")


if __name__ == "__main__":
    main()
