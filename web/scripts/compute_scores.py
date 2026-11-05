#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
"""Unified in-memory scoring pipeline computing Bradley-Terry Elo leaderboards."""

import os
from zoneinfo import ZoneInfo
lazy import argparse
lazy import asyncio
lazy import datetime
lazy import json
lazy import pickle
lazy from collections import defaultdict
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
from mwahahavote.scoring import (
    pretty_print_elo_rating,
    report_elo_analysis_results,
    report_elo_analysis_results_from_cells,
)
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


def _write_unranked_leaderboard(
    all_system_id_to_vote_count: dict[str, int], output_path: Path, competition: CompetitionConfig
) -> None:
    """Write a leaderboard without ratings, for a task without battles to score."""
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


def _write_leaderboard(
    results: dict[str, Any],
    all_system_id_to_vote_count: dict[str, int],
    output_path: Path,
    competition: CompetitionConfig,
    task: str,
    quiet: bool,
) -> None:
    """Write the leaderboard JSON from the scoring results, and print them."""
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
        if results["elo_rating_online"] is not None:
            print("# Online Elo")
            pretty_print_elo_rating(results["elo_rating_online"])
        print("# Median")
        pretty_print_elo_rating(results["elo_rating_median"])
        print(f"last update : {cutoff_date}")


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
        _write_unranked_leaderboard(all_system_id_to_vote_count, output_path, competition)
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

    _write_leaderboard(results, all_system_id_to_vote_count, output_path, competition, task, quiet)


def _to_tstamp(date: datetime.datetime | str) -> int:
    if isinstance(date, str):  # Some databases return the date of an aggregate function as text.
        date = datetime.datetime.fromisoformat(date)
    return round(date.timestamp())


async def fetch_task_cells(
    engine: sqlalchemy.ext.asyncio.AsyncEngine,
    phase_id: int,
    task: Task,
    excluded_session_ids: Sequence[str] = (),
    include_ties: bool = True,
    min_votes_per_system: int = MIN_VOTES_PER_SYSTEM,
) -> tuple[pd.DataFrame, int | None, dict[str, int]]:
    """Fetch the battles of a single task grouped in (model_a, model_b, winner, count) cells, the timestamp of the last
    one, and the vote counts.

    It's what `fetch_task_battles` gives, but grouped by the database, so it doesn't have to read each vote.
    """
    vote_counts = await mwahahavote.database.get_vote_counts_for_scoring(engine, phase_id, task, excluded_session_ids)
    same_text_battle_counts = await mwahahavote.database.get_same_text_battle_counts(engine, phase_id, task)

    # Vote counts used to filter systems that don't have enough votes
    system_id_to_vote_count: defaultdict[str, int] = defaultdict(int)
    for vote_count in vote_counts:
        system_id_to_vote_count[vote_count.system_id_a] += vote_count.count
        system_id_to_vote_count[vote_count.system_id_b] += vote_count.count
    for system_id_a, system_id_b, count in same_text_battle_counts:
        system_id_to_vote_count[system_id_a] += count
        system_id_to_vote_count[system_id_b] += count

    # Total vote counts (unfiltered) for unranked systems on leaderboard
    if excluded_session_ids:
        all_vote_counts = await mwahahavote.database.get_vote_counts_for_scoring(engine, phase_id, task)
    else:
        all_vote_counts = vote_counts
    all_system_id_to_vote_count: dict[str, int] = defaultdict(int)
    for vote_count in all_vote_counts:
        all_system_id_to_vote_count[vote_count.system_id_a] += vote_count.count
        all_system_id_to_vote_count[vote_count.system_id_b] += vote_count.count
    # Some systems may not be part of the output as there are no votes for them:
    async for system_id in mwahahavote.database.get_systems(engine, phase_id, task):
        all_system_id_to_vote_count.setdefault(system_id, 0)

    # We only want the votes from those systems that appear at least once on each side of the votes.
    # Otherwise, it causes issues in the scoring calculation.
    systems_a = {vote_count.system_id_a for vote_count in vote_counts}
    systems_b = {vote_count.system_id_b for vote_count in vote_counts}

    vote_to_winner = {"a": "model_a", "b": "model_b", "t": "tie"}
    cells: list[tuple[str, str, str, int, int]] = [
        (
            vote_count.system_id_a,
            vote_count.system_id_b,
            vote_to_winner[vote_count.vote],
            vote_count.count,
            _to_tstamp(vote_count.last_date),  # ty: ignore[invalid-argument-type]
        )
        for vote_count in vote_counts
        if vote_count.system_id_a in systems_b and vote_count.system_id_b in systems_a
    ]
    # The tie votes added for the battles with the same text have this date.
    same_text_tstamp = _to_tstamp(datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC))
    cells.extend(
        (system_id_a, system_id_b, "tie", count, same_text_tstamp)
        for system_id_a, system_id_b, count in same_text_battle_counts
    )
    cells = [
        cell
        for cell in cells
        if (
            system_id_to_vote_count[cell[0]] >= min_votes_per_system
            and system_id_to_vote_count[cell[1]] >= min_votes_per_system
        )
        and (include_ties or cell[2] != "tie")
    ]

    cells_df = pd.DataFrame(cells, columns=["model_a", "model_b", "winner", "count", "tstamp"])
    last_tstamp = int(cells_df["tstamp"].max()) if len(cells_df) else None
    return cells_df[["model_a", "model_b", "winner", "count"]], last_tstamp, dict(all_system_id_to_vote_count)


def process_task_cells(
    cells: pd.DataFrame,
    last_tstamp: int | None,
    all_system_id_to_vote_count: dict[str, int],
    output_path: Path,
    competition: CompetitionConfig,
    num_bootstrap: int = 100,
    num_cpu: int = 1,
    tol: float = 1e-6,
    task: str = "",
    quiet: bool = False,
) -> None:
    """Like `process_task_leaderboard`, but for battles grouped in cells, so nothing is dumped as it needs each vote."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if cells.empty or last_tstamp is None:
        _write_unranked_leaderboard(all_system_id_to_vote_count, output_path, competition)
        if not quiet and task:
            print(f"Skipping the unvoted task {task}.")
        return

    np.random.seed(42)
    results = report_elo_analysis_results_from_cells(
        cells,
        last_tstamp,
        rating_system="bt",
        num_bootstrap=num_bootstrap,
        num_cpu=num_cpu,
        tol=tol,
    )
    _write_leaderboard(results, all_system_id_to_vote_count, output_path, competition, task, quiet)


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


def _is_task_cached(cached: dict[str, Any] | None, task_meta: dict[str, Any], needs_intermediate_files: bool) -> bool:
    """Whether the cache has the scores for the current votes of a task, and also the intermediate files, if needed."""
    if cached is None:
        return False
    cached = dict(cached)
    has_current_intermediate_files = cached.pop("intermediate", False)
    return cached == task_meta and (has_current_intermediate_files or not needs_intermediate_files)


async def _score_task(
    engine: sqlalchemy.ext.asyncio.AsyncEngine,
    competition: CompetitionConfig,
    task: Task,
    output_path: Path,
    excluded_list: list[str],
    base_excluded: set[str],
    only_prolific_sessions: bool,
    include_ties: bool,
    min_votes_per_system: int,
    num_bootstrap: int,
    num_cpu: int,
    tol: float,
    inter_path: Path | None,
    quiet: bool,
) -> None:
    """Score a single task and write its leaderboard."""
    if inter_path is None:
        # Nothing needs the votes one by one, so the database can group them.
        cells, last_tstamp, all_system_id_to_vote_count = await fetch_task_cells(
            engine=engine,
            phase_id=competition.phase_id,
            task=task,
            excluded_session_ids=excluded_list,
            include_ties=include_ties,
            min_votes_per_system=min_votes_per_system,
        )
        process_task_cells(
            cells=cells,
            last_tstamp=last_tstamp,
            all_system_id_to_vote_count=all_system_id_to_vote_count,
            output_path=output_path,
            competition=competition,
            num_bootstrap=num_bootstrap,
            num_cpu=num_cpu,
            tol=tol,
            task=task,
            quiet=quiet,
        )
    else:
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
                and _is_task_cached(cache.get(task), task_meta, needs_intermediate_files=inter_path is not None)
            ):
                tasks_recomputed[task] = False
                if not quiet:
                    _print_cached_task_results(task, vote_count, inter_path)
                continue

            await _score_task(
                engine=engine,
                competition=competition,
                task=task,
                output_path=output_path,
                excluded_list=excluded_list,
                base_excluded=base_excluded,
                only_prolific_sessions=only_prolific_sessions,
                include_ties=include_ties,
                min_votes_per_system=min_votes_per_system,
                num_bootstrap=num_bootstrap,
                num_cpu=num_cpu,
                tol=tol,
                inter_path=inter_path,
                quiet=quiet,
            )
            # Scoring without the intermediate files leaves the ones from before out of date.
            cache[task] = {**task_meta, "intermediate": inter_path is not None}
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
