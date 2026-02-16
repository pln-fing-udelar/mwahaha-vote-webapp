#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
import sys

import pandas as pd


def main() -> None:
    assert len(sys.argv) > 1

    df = pd.read_json(sys.argv[1])
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
