#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
lazy import argparse

lazy from ingestion.codabench import list_submissions
lazy from ingestion.submission import print_stats
lazy from mwahahavote.config import ACTIVE_COMPETITION, COMPETITION_CHOICES, get_competition


def main() -> None:
    parser = argparse.ArgumentParser(description="Print submission stats from CodaBench.")
    parser.add_argument(
        "--competition",
        "-c",
        choices=COMPETITION_CHOICES,
        default=None,
        help=f"Competition to target (default: {ACTIVE_COMPETITION.id}, or $COMPETITION env var).",
    )
    args, _ = parser.parse_known_args()
    competition = get_competition(args.competition)

    print("Obtaining the list of all submissions so far… ", end="")
    submissions = sorted(
        list_submissions(
            competition_id=competition.codabench_competition_id,
            phase_id=competition.phase_id,
        )
    )
    print("✅")

    print_stats(submissions)

    print()
    print("Submissions:")
    print()

    for submission in submissions:
        print(f"- {submission}")


if __name__ == "__main__":
    main()
