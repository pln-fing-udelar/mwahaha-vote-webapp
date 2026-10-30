#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
import os
lazy import argparse
lazy import asyncio
lazy import json
lazy from typing import Literal

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


async def async_main(
    only_prolific_sessions: bool = False,
    include_ties: bool = True,
    competition: CompetitionConfig | None = None,
) -> None:
    if competition is None:
        competition = ACTIVE_COMPETITION

    async with mwahahavote.database.create_engine() as engine:
        for task in sorted(competition.task_choices):
            excluded_session_ids = (
                [
                    session_id
                    async for session_id in mwahahavote.database.get_session_ids(engine, competition.phase_id, task)
                    if not session_id.startswith("prolific-id-")
                ]
                if only_prolific_sessions
                else []
            )

            excluded_session_ids += EXCLUDED_SESSION_IDS

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
    args, _ = parser.parse_known_args()
    competition = get_competition(args.competition) if args.competition is not None else None
    asyncio.run(
        async_main(
            only_prolific_sessions=args.only_prolific_sessions,
            include_ties=not args.no_ties,
            competition=competition,
        )
    )


if __name__ == "__main__":
    main()
