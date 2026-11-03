#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
"""Unified in-memory scoring pipeline computing Bradley-Terry Elo leaderboards."""

import os
from zoneinfo import ZoneInfo
lazy import argparse
lazy import asyncio
lazy import datetime
lazy import json
lazy import pickle
lazy from collections.abc import Sequence
lazy from pathlib import Path
lazy from typing import Any, cast

lazy import aioitertools
lazy import numpy as np
lazy import pandas as pd
lazy import sqlalchemy.ext.asyncio
lazy from prepare_data_for_scoring import (
    prepare_data_from_csv,
    vote_to_fastchat_format,
    vote_to_fastchat_language,
)

from mwahahavote.config import ACTIVE_COMPETITION, COMPETITION_CHOICES, CompetitionConfig, Task, get_competition
from mwahahavote.scoring import pretty_print_elo_rating, report_elo_analysis_results
lazy import mwahahavote.database

MIN_VOTES_PER_SYSTEM = 20

EXCLUDED_SESSION_IDS = {
    stripped_session_id
    for session_id in os.environ.get("EXCLUDED_SESSION_IDS", "").split(",")
    if (stripped_session_id := session_id.strip())
}


async def fetch_task_battles(
    engine: sqlalchemy.ext.asyncio.AsyncEngine,
    phase_id: int,
    task: Task,
    base_excluded: set[str],
    only_prolific_sessions: bool = False,
    include_ties: bool = True,
    min_votes_per_system: int = MIN_VOTES_PER_SYSTEM,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Fetch battle data and vote counts for a single task."""
    task_excluded = (
        {
            session_id
            async for session_id in mwahahavote.database.get_session_ids(engine, phase_id, task)
            if not session_id.startswith("prolific-id-")
        }
        if only_prolific_sessions
        else set()
    ) | base_excluded

    excluded_session_ids = list(task_excluded)

    # Vote counts used to filter systems that don't have enough votes
    system_id_to_vote_count = await mwahahavote.database.get_votes_per_system(
        engine, phase_id, task, excluded_session_ids
    )

    async for vote in mwahahavote.database.get_votes_for_battles_with_the_same_text(engine, phase_id, task):
        system_id_to_vote_count[vote.battle.output_a.system.id] += 1
        system_id_to_vote_count[vote.battle.output_b.system.id] += 1

    # Total vote counts (unfiltered) for unranked systems on leaderboard
    all_system_id_to_vote_count = await mwahahavote.database.get_votes_per_system(engine, phase_id, task)

    battles: list[dict[str, Any]] = [
        {
            "question_id": vote.battle.prompt.id,
            "model_a": vote.battle.output_a.system.id,
            "model_b": vote.battle.output_b.system.id,
            "winner": vote_to_fastchat_format(vote),
            "judge": vote.session_id,
            "conversation_a": "",
            "conversation_b": "",
            "turn": 0,
            "anony": True,
            "language": vote_to_fastchat_language(vote),
            "tstamp": round(vote.date.timestamp()),
        }
        async for vote in aioitertools.chain(
            mwahahavote.database.get_votes_for_scoring(engine, phase_id, task, excluded_session_ids),
            mwahahavote.database.get_votes_for_battles_with_the_same_text(engine, phase_id, task),
        )
        if (
            system_id_to_vote_count[vote.battle.output_a.system.id] >= min_votes_per_system
            and system_id_to_vote_count[vote.battle.output_b.system.id] >= min_votes_per_system
        )
        and (include_ties or vote.vote != "t")
    ]
    return battles, all_system_id_to_vote_count


def process_task_leaderboard(
    battles: list[dict[str, Any]],
    all_system_id_to_vote_count: dict[str, int],
    output_path: Path,
    competition: CompetitionConfig,
    num_bootstrap: int = 100,
    num_cpu: int = 1,
    tol: float = 1e-6,
    intermediate_dir: Path | None = None,
    task: str = "",
    quiet: bool = False,
) -> None:
    """Compute Bradley-Terry Elo ratings for task battles and write leaderboard JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if intermediate_dir is not None and task:
        intermediate_dir.mkdir(parents=True, exist_ok=True)
        with open(intermediate_dir / f"votes-{task}.json", "w", encoding="utf-8") as f:
            json.dump(battles, f)

    if not battles:
        if intermediate_dir is not None and task:
            (intermediate_dir / f"elo_results_{task}.pkl").unlink(missing_ok=True)
        unranked_records = [
            {
                "system": system_id,
                "rating": None,
                "variance": None,
                "rating_q975": None,
                "rating_q025": None,
                "num_battles": votes,
                "final_ranking": None,
            }
            for system_id, votes in all_system_id_to_vote_count.items()
            if system_id not in competition.ignored_users
        ]
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(unranked_records, f)
        if not quiet and task:
            print(f"Skipping the unvoted task {task}.")
        return

    np.random.seed(42)
    battles_df = pd.DataFrame(battles)
    results = report_elo_analysis_results(
        battles_df,
        rating_system="bt",
        num_bootstrap=num_bootstrap,
        num_cpu=num_cpu,
        tol=tol,
    )

    if intermediate_dir is not None and task:
        with open(intermediate_dir / f"elo_results_{task}.pkl", "wb") as f:
            pickle.dump({"full": results}, f)

    df: pd.DataFrame = results["leaderboard_table_df"]
    df.index.name = "system"
    df = df.sort_values(by=["rating"], ascending=False)

    if competition.ignored_users:
        df = df.drop(index=list(competition.ignored_users), errors="ignore")

    considered_system_ids = set(df.index)
    for system_id, votes in all_system_id_to_vote_count.items():
        if system_id not in considered_system_ids and system_id not in competition.ignored_users:
            df.loc[system_id] = [
                float("NaN"),
                float("NaN"),
                float("NaN"),
                float("NaN"),
                votes,
                float("NaN"),
            ]

    df.num_battles = df.num_battles.astype(int)  # ty:ignore[unresolved-attribute]
    df.reset_index().to_json(output_path, orient="records")

    if not quiet:
        last_updated_tstamp = results["last_updated_tstamp"]
        cutoff_date = datetime.datetime.fromtimestamp(last_updated_tstamp, tz=ZoneInfo("US/Pacific")).strftime("%Y%m%d")
        category_name = f"{task} conversations" if task else "full conversations"
        print(f"\n\n# Results for {category_name}")
        print("# Online Elo")
        pretty_print_elo_rating(results["elo_rating_online"])
        print("# Median")
        pretty_print_elo_rating(results["elo_rating_median"])
        print(f"last update : {cutoff_date}")


def _print_cached_task_results(
    task: str,
    vote_count: int,
    inter_path: Path | None,
) -> None:
    """Print results for a cached task if intermediate pickle results exist."""
    if vote_count == 0:
        print(f"Skipping the unvoted task {task}.")
        return

    if inter_path is not None and (inter_path / f"elo_results_{task}.pkl").exists():
        try:
            with open(inter_path / f"elo_results_{task}.pkl", "rb") as f:
                cached_results = pickle.load(f)["full"]
            last_updated_tstamp = cached_results["last_updated_tstamp"]
            cutoff_date = datetime.datetime.fromtimestamp(last_updated_tstamp, tz=ZoneInfo("US/Pacific")).strftime(
                "%Y%m%d"
            )
            print(f"\n\n# Results for {task} conversations (cached)")
            print("# Online Elo")
            pretty_print_elo_rating(cached_results["elo_rating_online"])
            print("# Median")
            pretty_print_elo_rating(cached_results["elo_rating_median"])
            print(f"last update : {cutoff_date}")
            return
        except Exception:
            pass

    print(f"Skipping task {task} (cached - no changes).")


async def async_main(
    competition: CompetitionConfig | None = None,
    scores_dir: Path | str = "src/mwahahavote/static/scores",
    only_prolific_sessions: bool = False,
    include_ties: bool = True,
    excluded_session_ids: Sequence[str] | set[str] | None = None,
    input_csv: Path | str | None = None,
    min_votes_per_system: int = MIN_VOTES_PER_SYSTEM,
    num_bootstrap: int = 100,
    num_cpu: int = 1,
    tol: float = 1e-6,
    tasks: Sequence[Task] | None = None,
    intermediate_dir: Path | str | None = None,
    cache_file: Path | str | None = "scoring/.scoring_cache.json",
    no_cache: bool = False,
    force: bool = False,
    quiet: bool = False,
) -> dict[str, bool]:
    """Run the complete scoring pipeline in memory, caching results for unchanged tasks."""
    if competition is None:
        competition = ACTIVE_COMPETITION

    scores_path = Path(scores_dir)
    inter_path = Path(intermediate_dir) if intermediate_dir is not None else None
    cache_path = Path(cache_file) if cache_file is not None else None

    cache: dict[str, dict[str, Any]] = {}
    if not no_cache and cache_path is not None and cache_path.exists():
        try:
            with open(cache_path, encoding="utf-8") as f:
                cache = json.load(f)
        except Exception:
            cache = {}

    tasks_recomputed: dict[str, bool] = {}

    if input_csv is not None:
        csv_tasks = prepare_data_from_csv(
            input_csv,
            output_dir=inter_path or Path("scoring"),
            include_ties=include_ties,
            min_votes_per_system=min_votes_per_system,
        )
        for task in csv_tasks:
            votes_file = (inter_path or Path("scoring")) / f"votes-{task}.json"
            battles = json.loads(votes_file.read_text(encoding="utf-8")) if votes_file.exists() else []
            process_task_leaderboard(
                battles=battles,
                all_system_id_to_vote_count={},
                output_path=scores_path / f"{task}.json",
                competition=competition,
                num_bootstrap=num_bootstrap,
                num_cpu=num_cpu,
                tol=tol,
                intermediate_dir=inter_path,
                task=task,
                quiet=quiet,
            )
            tasks_recomputed[task] = True
        return tasks_recomputed

    base_excluded = (
        set(excluded_session_ids) if excluded_session_ids is not None else EXCLUDED_SESSION_IDS
    ) | competition.excluded_session_ids

    async with mwahahavote.database.create_engine() as engine:
        tasks_to_process = tasks if tasks is not None else sorted(competition.task_choices)
        for task in tasks_to_process:
            task_excluded = (
                {
                    session_id
                    async for session_id in mwahahavote.database.get_session_ids(engine, competition.phase_id, task)
                    if not session_id.startswith("prolific-id-")
                }
                if only_prolific_sessions
                else set()
            ) | base_excluded

            excluded_list = list(task_excluded)
            vote_count, latest_date, vote_checksum, output_count = await mwahahavote.database.get_task_vote_metadata(
                engine, competition.phase_id, task, excluded_session_ids=excluded_list
            )
            task_meta = {
                "vote_count": int(vote_count),
                "latest_date": (
                    latest_date.isoformat()
                    if isinstance(latest_date, datetime.datetime)
                    else (str(latest_date) if latest_date is not None else None)
                ),
                "vote_checksum": int(vote_checksum),
                "output_count": int(output_count),
            }
            output_path = scores_path / f"{task}.json"

            intermediate_missing = inter_path is not None and (
                not (inter_path / f"votes-{task}.json").exists()
                or (vote_count > 0 and not (inter_path / f"elo_results_{task}.pkl").exists())
            )

            if (
                not force
                and not no_cache
                and output_path.exists()
                and not intermediate_missing
                and cache.get(task) == task_meta
            ):
                tasks_recomputed[task] = False
                if not quiet:
                    _print_cached_task_results(task, vote_count, inter_path)
                continue

            battles, all_system_id_to_vote_count = await fetch_task_battles(
                engine=engine,
                phase_id=competition.phase_id,
                task=task,
                base_excluded=base_excluded,
                only_prolific_sessions=only_prolific_sessions,
                include_ties=include_ties,
                min_votes_per_system=min_votes_per_system,
            )
            process_task_leaderboard(
                battles=battles,
                all_system_id_to_vote_count=all_system_id_to_vote_count,
                output_path=output_path,
                competition=competition,
                num_bootstrap=num_bootstrap,
                num_cpu=num_cpu,
                tol=tol,
                intermediate_dir=inter_path,
                task=task,
                quiet=quiet,
            )
            cache[task] = task_meta
            tasks_recomputed[task] = True

        if not no_cache and cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(cache, f, indent=2)

        return tasks_recomputed


def main() -> None:
    """CLI entry point for the scoring pipeline."""
    parser = argparse.ArgumentParser(description="Unified in-memory scoring pipeline for Bradley-Terry Elo rankings.")
    parser.add_argument(
        "--competition",
        "-c",
        choices=COMPETITION_CHOICES,
        default=None,
        help=f"Competition to target (default: {ACTIVE_COMPETITION.id}, or $COMPETITION env var).",
    )
    parser.add_argument(
        "--scores-dir",
        type=Path,
        default=Path("src/mwahahavote/static/scores"),
        help="Directory to write final scores JSON files (default: src/mwahahavote/static/scores).",
    )
    parser.add_argument(
        "--only-prolific-sessions",
        action="store_true",
        default=False,
        help="Consider only Prolific sessions for scoring.",
    )
    parser.add_argument(
        "--no-ties",
        action="store_true",
        default=False,
        help="Exclude tie votes from scoring.",
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=None,
        help="Path to CSV containing votes (bypasses database query).",
    )
    parser.add_argument(
        "--min-votes-per-system",
        type=int,
        default=MIN_VOTES_PER_SYSTEM,
        help=f"Minimum votes per system required to be included (default: {MIN_VOTES_PER_SYSTEM}).",
    )
    parser.add_argument(
        "--num-bootstrap",
        type=int,
        default=100,
        help="Number of bootstrap rounds (default: 100).",
    )
    parser.add_argument(
        "--num-cpu",
        type=int,
        default=1,
        help="Number of CPU cores (default: 1).",
    )
    parser.add_argument(
        "--tol",
        type=float,
        default=1e-6,
        help="L-BFGS-B gradient tolerance (default: 1e-6).",
    )
    parser.add_argument(
        "--tasks",
        nargs="+",
        default=None,
        help="Specific task IDs to score (default: all tasks in competition).",
    )
    parser.add_argument(
        "--dump-intermediate",
        type=Path,
        default=None,
        help="Optional directory to save intermediate votes-*.json and elo_results_*.pkl files.",
    )
    parser.add_argument(
        "--cache-file",
        type=Path,
        default=Path("scoring/.scoring_cache.json"),
        help="Path to incremental scoring cache file (default: scoring/.scoring_cache.json).",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        default=False,
        help="Disable cache and recompute all tasks without reading or writing cache.",
    )
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        default=False,
        help="Force recomputation of all tasks, updating cache afterwards.",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        default=False,
        help="Suppress output to stdout.",
    )

    args = parser.parse_args()
    competition = get_competition(args.competition) if args.competition is not None else None
    tasks = [cast(Task, t) for t in args.tasks] if args.tasks is not None else None
    asyncio.run(
        async_main(
            competition=competition,
            scores_dir=args.scores_dir,
            only_prolific_sessions=args.only_prolific_sessions,
            include_ties=not args.no_ties,
            input_csv=args.input_csv,
            min_votes_per_system=args.min_votes_per_system,
            num_bootstrap=args.num_bootstrap,
            num_cpu=args.num_cpu,
            tol=args.tol,
            tasks=tasks,
            intermediate_dir=args.dump_intermediate,
            cache_file=args.cache_file,
            no_cache=args.no_cache,
            force=args.force,
            quiet=args.quiet,
        )
    )


if __name__ == "__main__":
    main()
