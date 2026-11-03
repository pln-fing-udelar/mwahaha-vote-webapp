#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
import os
lazy import argparse
lazy import asyncio
lazy import csv
lazy import json
lazy from collections import Counter, defaultdict
lazy from pathlib import Path
lazy from typing import Any, Literal

lazy import aioitertools

from mwahahavote.config import ACTIVE_COMPETITION, COMPETITION_CHOICES, CompetitionConfig, get_competition
lazy import mwahahavote.database
lazy from mwahahavote.database import Vote

PHASE_ID = ACTIVE_COMPETITION.phase_id

# With few votes, we learned that it destabilizes the score calculation for all the systems.
MIN_VOTES_PER_SYSTEM = 20

EXCLUDED_SESSION_IDS = {
    stripped_session_id
    for session_id in os.environ.get("EXCLUDED_SESSION_IDS", "").split(",")
    if (stripped_session_id := session_id.strip())
}


def vote_to_fastchat_format(vote: Vote) -> Literal["model_a", "model_b", "tie"]:
    match vote.vote:
        case "a":
            return "model_a"
        case "b":
            return "model_b"
        case "t":
            return "tie"
        case _:
            raise ValueError(f"Unknown vote: {vote.vote}")


def vote_to_fastchat_language(vote: Vote) -> Literal["Chinese", "English", "Spanish"]:
    match vote.battle.prompt.language:
        case "en":
            return "English"
        case "es":
            return "Spanish"
        case "zh":
            return "Chinese"
        case _:
            raise ValueError(f"Unknown language: {vote.battle.prompt.language}")


def task_to_fastchat_language(task: str) -> Literal["Chinese", "English", "Spanish"]:
    match task:
        case "a-en":
            return "English"
        case "a-es" | "t3":
            return "Spanish"
        case "a-zh":
            return "Chinese"
        case _:
            return "English"


def prepare_data_from_csv(
    input_csv: Path | str,
    output_dir: Path | str = "scoring",
    include_ties: bool = True,
    min_votes_per_system: int = MIN_VOTES_PER_SYSTEM,
) -> list[str]:
    """Prepares FastChat scoring JSON files directly from a CSV file."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    rows_by_task: dict[str, list[dict[str, Any]]] = defaultdict(list)
    counts_by_task: dict[str, Counter[str]] = defaultdict(Counter)

    with open(input_csv, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            task = row.get("task")
            if not task:
                continue
            vote = row.get("vote")
            if not include_ties and vote == "t":
                continue
            sys_a = row.get("system_id_a")
            sys_b = row.get("system_id_b")
            if not sys_a or not sys_b or not vote:
                continue

            counts_by_task[task][sys_a] += 1
            counts_by_task[task][sys_b] += 1
            rows_by_task[task].append(row)

    generated_tasks: list[str] = []
    for task, rows in sorted(rows_by_task.items()):
        system_counts = counts_by_task[task]
        battles: list[dict[str, Any]] = []
        for r in rows:
            sys_a = r["system_id_a"]
            sys_b = r["system_id_b"]
            if system_counts[sys_a] < min_votes_per_system or system_counts[sys_b] < min_votes_per_system:
                continue
            vote_val = r["vote"]
            winner = "model_a" if vote_val == "a" else ("model_b" if vote_val == "b" else "tie")
            battles.append(
                {
                    "question_id": r.get("prompt_id", ""),
                    "model_a": sys_a,
                    "model_b": sys_b,
                    "winner": winner,
                    "judge": r.get("session_id") or r.get("judge") or "subset_judge",
                    "conversation_a": "",
                    "conversation_b": "",
                    "turn": 0,
                    "anony": True,
                    "language": task_to_fastchat_language(task),
                    "tstamp": 0,
                }
            )

        task_file = output_path / f"votes-{task}.json"
        with open(task_file, "w", encoding="utf-8") as f:
            json.dump(battles, f)
        generated_tasks.append(task)

    return generated_tasks


async def async_main(
    only_prolific_sessions: bool = False,
    include_ties: bool = True,
    competition: CompetitionConfig | None = None,
    excluded_session_ids: list[str] | set[str] | None = None,
    input_csv: Path | str | None = None,
    min_votes_per_system: int = MIN_VOTES_PER_SYSTEM,
    output_dir: Path | str = "scoring",
) -> None:
    if input_csv is not None:
        prepare_data_from_csv(
            input_csv,
            output_dir=output_dir,
            include_ties=include_ties,
            min_votes_per_system=min_votes_per_system,
        )
        return

    if competition is None:
        competition = ACTIVE_COMPETITION

    base_excluded = (
        set(excluded_session_ids) if excluded_session_ids is not None else EXCLUDED_SESSION_IDS
    ) | competition.excluded_session_ids

    async with mwahahavote.database.create_engine() as engine:
        for task in sorted(competition.task_choices):
            task_excluded = (
                {
                    session_id
                    async for session_id in mwahahavote.database.get_session_ids(engine, competition.phase_id, task)
                    if not session_id.startswith("prolific-id-")
                }
                if only_prolific_sessions
                else set()
            ) | base_excluded

            excluded_session_ids = list(task_excluded)

            system_id_to_vote_count = await mwahahavote.database.get_votes_per_system(
                engine, competition.phase_id, task, excluded_session_ids
            )

            async for vote in mwahahavote.database.get_votes_for_battles_with_the_same_text(
                engine, competition.phase_id, task
            ):
                system_id_to_vote_count[vote.battle.output_a.system.id] += 1
                system_id_to_vote_count[vote.battle.output_b.system.id] += 1

            os.makedirs("scoring", exist_ok=True)
            with open(f"scoring/votes-{task}.json", "w") as file:
                json.dump(
                    [
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
                            mwahahavote.database.get_votes_for_scoring(
                                engine, competition.phase_id, task, excluded_session_ids
                            ),
                            mwahahavote.database.get_votes_for_battles_with_the_same_text(
                                engine, competition.phase_id, task
                            ),
                        )
                        if (
                            system_id_to_vote_count[vote.battle.output_a.system.id] >= MIN_VOTES_PER_SYSTEM
                            and system_id_to_vote_count[vote.battle.output_b.system.id] >= MIN_VOTES_PER_SYSTEM
                        )
                        and (include_ties or vote.vote != "t")
                    ],
                    file,
                )


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare vote data for FastChat scoring.")
    parser.add_argument(
        "--competition",
        "-c",
        choices=COMPETITION_CHOICES,
        default=None,
        help=f"Competition to target (default: {ACTIVE_COMPETITION.id}, or $COMPETITION env var).",
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
        "--output-dir",
        type=Path,
        default=Path("scoring"),
        help="Directory to write output JSON files (default: scoring).",
    )
    args, _ = parser.parse_known_args()
    competition = get_competition(args.competition) if args.competition is not None else None
    asyncio.run(
        async_main(
            only_prolific_sessions=args.only_prolific_sessions,
            include_ties=not args.no_ties,
            competition=competition,
            input_csv=args.input_csv,
            min_votes_per_system=args.min_votes_per_system,
            output_dir=args.output_dir,
        )
    )


if __name__ == "__main__":
    main()
