#!/usr/bin/env python
import json

from mwahahavote.database import TASK_CHOICES, get_votes


def main() -> None:
    for task in sorted(TASK_CHOICES):
        vote_dicts = [
            {
                "question_id": vote.battle.prompt.id,
                "model_a": vote.battle.output_a.system.id,
                "model_b": vote.battle.output_b.system.id,
                "winner": "model_a" if vote.vote == "a" else ("model_b" if vote.vote == "b" else "tie"),
                "judge": vote.session_id,
                "conversation_a": "",
                "conversation_b": "",
                "turn": 0,
                "anony": True,
                "language": (
                    "Spanish"
                    if vote.battle.prompt.language == "es"
                    else ("Chinese" if vote.battle.prompt.language == "zh" else "English")
                ),
                # Now I convert it into Unix timestamp int:
                "tstamp": round(vote.date.timestamp()),
            }
            for vote in get_votes(task)
        ]
        with open(f"scoring/votes-{task}.json", "w") as file:
            json.dump(vote_dicts, file)


if __name__ == "__main__":
    main()
