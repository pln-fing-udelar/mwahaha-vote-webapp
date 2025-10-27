import functools
import os
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import requests

from mwahahavote.database import Task

# Reference API (old?): https://qtim-challenges.southcentralus.cloudapp.azure.com/api/docs/

BASE_URL = "https://www.codabench.org/api/"

COMPETITION_ID = 9719


def get_environ_session_id() -> str:
    return os.environ["CODABENCH_SESSION_ID"]


def task_id_to_task(task_id: int) -> Task:
    match task_id:
        case 21359:
            return "a-es"
        case 21358:
            return "a-en"
        case 21360:
            return "a-zh"
        case 21361:
            return "b1"
        case 22992:
            return "b2"
        case _:
            raise ValueError(f"Unknown task ID: {task_id}")


@functools.total_ordering
@dataclass(frozen=True)
class Submission:
    id: int
    user: str
    task: Task

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: Any) -> bool:
        return self.id == other.id if isinstance(other, type(self)) else NotImplemented

    def __lt__(self, other: Any) -> bool:
        return self.id < other.id


def list_submissions(competition_id: int = COMPETITION_ID, session_id: str | None = None) -> Iterable[Submission]:
    """List all "children" submissions for a competition that passed the submission test and were not deleted."""

    # TODO: filter by phase?

    session_id = session_id or get_environ_session_id()
    # noinspection SpellCheckingInspection
    response = requests.get(
        BASE_URL + "submissions",
        params={"phase__competition": competition_id, "show_is_soft_deleted": False},
        cookies={"sessionid": session_id},
    )
    response.raise_for_status()
    for dict_ in response.json():
        # For some reason, the "scores" field could be empty:
        scores = dict_["scores"] or [{"score": "0.0000000000"}]

        # There's a concept of "parent" and "children" submissions.
        # It seems that, if a submission is for multiple tasks,
        # it's a parent one and there's one child submission per task, which actually point to the same file.
        # We just check the child ones (the ones that don't have children) as they contain the actual task and score.
        if not dict_["children"] and bool(float(scores[0]["score"])):
            yield Submission(id=dict_["id"], user=dict_["owner"], task=task_id_to_task(dict_["task"]["id"]))


def get_submission_url(submission_id: int, session_id: str | None = None) -> str:
    session_id = session_id or get_environ_session_id()
    # noinspection SpellCheckingInspection
    response = requests.get(
        BASE_URL + f"submissions/{submission_id}/get_details",
        cookies={"sessionid": session_id},
    )
    response.raise_for_status()
    return response.json()["data_file"]
