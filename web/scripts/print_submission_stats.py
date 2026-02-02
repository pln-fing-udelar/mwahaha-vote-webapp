#!/usr/bin/env -S uv run --script --env-file ../.env
from ingestion.codabench import list_submissions
from ingestion.submission import print_stats


def main() -> None:
    # We sort them so they are ingested in order
    # so that the latest submission per user per task is the one that remains.

    print("Obtaining the list of all submissions so far… ", end="")
    submissions = sorted(list_submissions())
    print("✅")

    print_stats(submissions)

    print()
    print("Submissions:")
    print()

    for submission in submissions:
        print(f"- {submission}")


if __name__ == "__main__":
    main()
