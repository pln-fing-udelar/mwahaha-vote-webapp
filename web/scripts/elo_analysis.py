#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
"""CLI script to compute Bradley-Terry Elo ratings from cleaned battle data."""

from zoneinfo import ZoneInfo
lazy import argparse
lazy import datetime
lazy import pickle

lazy import numpy as np
lazy import pandas as pd

from mwahahavote.scoring import pretty_print_elo_rating, report_elo_analysis_results


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute Bradley-Terry ratings from battle data.")
    parser.add_argument("--clean-battle-file", type=str, required=True, help="Path to cleaned battle JSON file.")
    parser.add_argument("--output", "-o", type=str, default=None, help="Output path for the results pickle file.")
    parser.add_argument("--num-bootstrap", type=int, default=100, help="Number of bootstrap rounds.")
    parser.add_argument("--rating-system", type=str, choices=["bt", "elo"], default="bt", help="Rating system.")
    parser.add_argument("--exclude-models", type=str, nargs="+", default=[], help="Models to exclude.")
    parser.add_argument("--exclude-tie", action="store_true", default=False, help="Exclude ties.")
    parser.add_argument("--exclude-unknown-lang", action="store_true", default=False, help="Exclude unknown language.")
    parser.add_argument("--langs", type=str, nargs="+", default=[], help="Languages to filter.")
    parser.add_argument("--category", nargs="+", default=["full"], help="Categories to evaluate.")
    parser.add_argument("--num-cpu", type=int, default=1, help="Number of CPU cores.")
    parser.add_argument("--tol", type=float, default=1e-6, help="L-BFGS-B gradient tolerance (gtol).")
    parser.add_argument("--quiet", "-q", action="store_true", default=False, help="Suppress output to stdout.")

    args = parser.parse_args()

    np.random.seed(42)

    battles = pd.read_json(args.clean_battle_file)

    results: dict[str, dict] = {}
    for cat in args.category:
        results[cat] = report_elo_analysis_results(
            battles,
            rating_system=args.rating_system,
            num_bootstrap=args.num_bootstrap,
            exclude_models=args.exclude_models,
            langs=args.langs,
            exclude_tie=args.exclude_tie,
            exclude_unknown_lang=args.exclude_unknown_lang,
            num_cpu=args.num_cpu,
            tol=args.tol,
        )

    for cat in args.category:
        last_updated_tstamp = results[cat]["last_updated_tstamp"]
        cutoff_date = datetime.datetime.fromtimestamp(last_updated_tstamp, tz=ZoneInfo("US/Pacific")).strftime("%Y%m%d")
        if not args.quiet:
            print(f"\n\n# Results for {cat} conversations")
            print("# Online Elo")
            pretty_print_elo_rating(results[cat]["elo_rating_online"])
            print("# Median")
            pretty_print_elo_rating(results[cat]["elo_rating_median"])
            print(f"last update : {cutoff_date}")

    out_path = args.output if args.output else f"elo_results_{cutoff_date}.pkl"
    with open(out_path, "wb") as fout:
        pickle.dump(results, fout)


if __name__ == "__main__":
    main()
