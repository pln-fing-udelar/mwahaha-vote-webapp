import functools
import os
from collections.abc import Iterable, MutableSet
from dataclasses import dataclass
from typing import Any

import requests

from mwahahavote.database import Task

# Reference API (old?): https://qtim-challenges.southcentralus.cloudapp.azure.com/api/docs/

BASE_URL = "https://www.codabench.org/api/"

COMPETITION_ID = 9719

EVALUATION_TRIAL_PHASE_ID = 15784
EVALUATION_PHASE_ID = 15785


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
    tasks: MutableSet[Task]

    @property
    def system_id(self) -> str:
        return f"{self.user}-{self.id}"

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: Any) -> bool:
        return self.id == other.id if isinstance(other, type(self)) else NotImplemented

    def __lt__(self, other: Any) -> bool:
        return self.id < other.id


def list_submissions(  # noqa: C901
    competition_id: int | None = COMPETITION_ID,
    phase_id: int | None = None,
    session_id: str | None = None,
) -> Iterable[Submission]:
    """List all "parent" or single-task submissions for a competition that passed the submission test for at least one
    task and were not deleted.
    """

    query_params = {}

    if phase_id is None:
        query_params["phase__competition"] = competition_id
    else:
        query_params["phase"] = phase_id

    session_id = session_id or get_environ_session_id()
    # noinspection SpellCheckingInspection
    response = requests.get(BASE_URL + "submissions", params=query_params, cookies={"sessionid": session_id})
    response.raise_for_status()

    # There's a concept of "parent" and "children" submissions.
    # It seems that, if a submission is for multiple tasks,
    # it's a parent one and there's one child submission per task, which actually point to the same file.
    # If a submission is for a single task then it's neither parent nor child -- just a single node.

    # We need both the parents and the children.
    # The parents are used to obtain the parent submission ID, which we use to define the system ID.
    # The children are used to check which tasks were submitted and passed the submission test.

    parentless_submissions: dict[int, Submission] = {}

    submission_dicts = response.json()

    for dict_ in submission_dicts:
        if not dict_["parent"]:  # If it's not a child submission.
            id_ = dict_["id"]
            user = dict_["owner"]

            if dict_["children"]:
                # If it's a parent submission, we create it with an empty set of tasks.
                # If it ends up having no tasks, it'll be discarded later.
                parentless_submissions[id_] = Submission(id=id_, user=user, tasks=set())
            else:  # If it's not a parent submission.
                # If it's a single-task submission, we need to check that it passed the submission test.
                #
                # For some reason, the "scores" field could be empty:
                if (scores := dict_["scores"]) and bool(float(scores[0]["score"])):
                    parentless_submissions[id_] = Submission(
                        id=id_, user=user, tasks={task_id_to_task(dict_["task"]["id"])}
                    )

    for dict_ in submission_dicts:
        # We check if it's a child submission and also if its parent is in the list of parentless submissions.
        # A parent submission doesn't appear in the list if it was soft-deleted
        # (though, for some reason, the children still do...).
        if (parent_submission_id := dict_["parent"]) and (
            parent_submission := parentless_submissions.get(parent_submission_id)
        ):
            # For some reason, the "scores" field could be empty:
            if (scores := dict_["scores"]) and bool(float(scores[0]["score"])):
                parent_submission.tasks.add(task_id_to_task(dict_["task"]["id"]))

    for parent_submission in parentless_submissions.values():
        if parent_submission.tasks:
            yield parent_submission


def get_submission_url(submission_id: int, session_id: str | None = None) -> str:
    session_id = session_id or get_environ_session_id()
    # noinspection SpellCheckingInspection
    response = requests.get(
        BASE_URL + f"submissions/{submission_id}/get_details",
        cookies={"sessionid": session_id},
    )
    response.raise_for_status()
    return response.json()["data_file"]
