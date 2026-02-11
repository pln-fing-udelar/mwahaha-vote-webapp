#!/usr/bin/env python
import itertools
import json
import os
from typing import Literal

from ingestion.codabench import EVALUATION_PHASE_ID
from mwahahavote.database import (
    TASK_CHOICES,
    Vote,
    get_votes_for_battles_with_the_same_text,
    get_votes_for_scoring,
    get_votes_per_system,
)

PHASE_ID = EVALUATION_PHASE_ID

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


def main() -> None:
    for task in sorted(TASK_CHOICES):
        system_id_to_vote_count = dict(get_votes_per_system(PHASE_ID, task, EXCLUDED_SESSION_IDS))

        for vote in get_votes_for_battles_with_the_same_text(PHASE_ID, task):
            system_id_to_vote_count[vote.battle.output_a.system.id] += 1
            system_id_to_vote_count[vote.battle.output_b.system.id] += 1

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
                    for vote in itertools.chain(
                        get_votes_for_scoring(PHASE_ID, task, EXCLUDED_SESSION_IDS),
                        get_votes_for_battles_with_the_same_text(PHASE_ID, task),
                    )
                    if (
                        system_id_to_vote_count[vote.battle.output_a.system.id] >= MIN_VOTES_PER_SYSTEM
                        and system_id_to_vote_count[vote.battle.output_b.system.id] >= MIN_VOTES_PER_SYSTEM
                    )
                ],
                file,
            )


if __name__ == "__main__":
    main()
