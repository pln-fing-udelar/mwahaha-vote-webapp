#!/usr/bin/env python
import csv
import os

import pandas as pd

from . import util


def read_prompt_file(path: str) -> pd.DataFrame:
    # noinspection PyTypeChecker
    df = pd.read_csv(path, delimiter="\t", quoting=csv.QUOTE_NONE, index_col="id", na_values="-")
    df.index.rename("prompt_id", inplace=True)
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
        read_prompt_files("prompts/").to_sql("prompts", util.create_engine(), if_exists="append"),
    )


if __name__ == "__main__":
    main()
