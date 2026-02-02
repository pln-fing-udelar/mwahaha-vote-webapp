import dataclasses
import datetime
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

EVALUATION_TRIAL_PHASE_ID = 15784
EVALUATION_PHASE_ID = 15785


def get_environ_session_id() -> str:
    """Gets the CodaBench session ID from the `CODABENCH_SESSION_ID` environment variable."""
    return os.environ["CODABENCH_SESSION_ID"]


def is_session_id_valid(session_id: str) -> bool:
    """Checks whether a session ID is valid.

    Turns out that the /api/submissions/ endpoint doesn't fail when the session ID is invalid, but instead returns only
    the last 10 submissions. This can be error-prone, so we add this check.
    """
    # noinspection SpellCheckingInspection
    response = requests.get("https://www.codabench.org/", cookies={"sessionid": session_id})
    response.raise_for_status()
    return "user_dropdown" in response.text  # If it's logged in fine, the user dropdown will appear.


def task_id_to_task(task_id: int) -> Task:
    """Converts a CodaBench task ID to our internal string representation."""
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
    """A CodaBench submission."""

    id: int
    user: str
    date: datetime.datetime
    tasks: list[Task] = dataclasses.field(default_factory=list)
    tests_passed: list[bool] = dataclasses.field(default_factory=list)
    is_deleted: bool = False

    @property
    def system_id(self) -> str:
        return self.user  # For the dev phase, it was: f"{self.user}-{self.id}"

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: Any) -> bool:
        return self.id == other.id if isinstance(other, type(self)) else NotImplemented

    def __lt__(self, other: Any) -> bool:
        return self.id < other.id


def _list_submission_dicts(
    competition_id: int | None = COMPETITION_ID, phase_id: int | None = EVALUATION_PHASE_ID, session_id: str | None = None
) -> list[dict[str, Any]]:
    """List all submission dicts for a competition or phase."""
    session_id = session_id or get_environ_session_id()
    if not is_session_id_valid(session_id):
        raise ValueError("The provided session ID is not valid.")

    query_params: dict[str, Any] = {"show_is_soft_deleted": True}

    if phase_id is None:
        query_params["phase__competition"] = competition_id
    else:
        query_params["phase"] = phase_id

    # noinspection SpellCheckingInspection
    response = requests.get(BASE_URL + "submissions", params=query_params, cookies={"sessionid": session_id})

    response.raise_for_status()

    return response.json()


def list_submissions(
    competition_id: int | None = COMPETITION_ID, phase_id: int | None = EVALUATION_PHASE_ID, session_id: str | None = None
) -> Iterable[Submission]:
    """List all "parent" or single-task submissions for a competition."""

    submission_dicts = _list_submission_dicts(competition_id=competition_id, phase_id=phase_id, session_id=session_id)

    # There's a concept of "parent" and "children" submissions.
    # It seems that if a submission is for multiple tasks,
    # it's a parent one and there's one child submission per task, which actually points to the same file.
    # If a submission is for a single task, then it's neither parent nor child -- just a single node.

    # We need both the parents and the children.
    # The parents are used to get the parent submission ID, which we use to define the system ID.
    # The children are used to check which tasks were submitted and passed the submission test.

    parentless_submissions: dict[int, Submission] = {}

    for d in submission_dicts:
        if not d["parent"]:  # If it's not a child submission.
            id_ = d["id"]
            user = d["owner"]
            date = datetime.datetime.fromisoformat(d["created_when"].replace("Z", "+00:00"))
            is_deleted = d["is_soft_deleted"]

            if d["children"]:
                # If it's a parent submission, we create it with an empty set of tasks.
                # If it ends up having no tasks, it'll be discarded later.
                parentless_submissions[id_] = Submission(id=id_, user=user, date=date, is_deleted=is_deleted)
            else:  # If it's not a parent submission.
                # If it's a single-task submission, we need to check that it passed the submission test.
                task = task_id_to_task(d["task"]["id"])
                # For some reason, the "scores" field could be empty:
                test_passed = bool((scores := d["scores"]) and bool(float(scores[0]["score"])))
                parentless_submissions[id_] = Submission(
                    id=id_, user=user, date=date, tasks=[task], tests_passed=[test_passed], is_deleted=is_deleted
                )

    for d in submission_dicts:
        if parent_submission_id := d["parent"]:
            parent_submission = parentless_submissions[parent_submission_id]
            parent_submission.tasks.append(task_id_to_task(d["task"]["id"]))
            # For some reason, the "scores" field could be empty:
            parent_submission.tests_passed.append(bool((scores := d["scores"]) and bool(float(scores[0]["score"]))))

            assert parent_submission.is_deleted or not d["is_soft_deleted"], (
                "If a parent submission is not deleted, its children cannot be deleted."
            )

    assert all(submission.tasks for submission in parentless_submissions.values()), (
        "All submissions must have at least one task."
    )

    return parentless_submissions.values()


def get_submission_url(submission_id: int, session_id: str | None = None) -> str:
    """Returns the URL to download a submission."""
    session_id = session_id or get_environ_session_id()
    # noinspection SpellCheckingInspection
    response = requests.get(
        BASE_URL + f"submissions/{submission_id}/get_details",
        cookies={"sessionid": session_id},
    )
    response.raise_for_status()
    return response.json()["data_file"]
