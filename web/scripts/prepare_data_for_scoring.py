#!/usr/bin/env python
import json
from typing import Literal

from mwahahavote.database import TASK_CHOICES, Vote, get_votes_for_scoring


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
                    for vote in get_votes_for_scoring(task)
                ],
                file,
            )


if __name__ == "__main__":
    main()
