import dataclasses
import datetime
import functools
from dataclasses import dataclass
from typing import Any
lazy import os
lazy from collections.abc import Iterable

from pydantic import BaseModel, TypeAdapter
lazy import httpx2

from mwahahavote.config import ACTIVE_COMPETITION, CompetitionConfig, Task, is_only_latest_submission_per_user

# Reference API (old?): https://qtim-challenges.southcentralus.cloudapp.azure.com/api/docs/

BASE_URL = "https://www.codabench.org/api/"


class _CodaBenchTask(BaseModel):
    id: int


class _CodaBenchScore(BaseModel):
    score: float


class _CodaBenchSubmission(BaseModel):
    id: int
    owner: str
    created_when: datetime.datetime
    is_soft_deleted: bool
    parent: int | None = None
    children: list[int] = []
    task: _CodaBenchTask | None = None
    scores: list[_CodaBenchScore] = []


class _CodaBenchSubmissionDetail(BaseModel):
    data_file: str


_SUBMISSION_LIST_ADAPTER = TypeAdapter(list[_CodaBenchSubmission])


def get_environ_session_id() -> str:
    """Gets the CodaBench session ID from the `CODABENCH_SESSION_ID` environment variable."""
    return os.environ["CODABENCH_SESSION_ID"]


def is_session_id_valid(session_id: str) -> bool:
    """Checks whether a session ID is valid.

    Turns out that the /api/submissions/ endpoint doesn't fail when the session ID is invalid, but instead returns only
    the last 10 submissions. This can be error-prone, so we add this check.
    """
    # noinspection SpellCheckingInspection
    response = httpx2.get("https://www.codabench.org/", cookies={"sessionid": session_id})
    response.raise_for_status()
    return "user_dropdown" in response.text  # If it's logged in fine, the user dropdown will appear.


def task_id_to_task(task_id: int, competition: CompetitionConfig | None = None) -> Task:
    """Converts a CodaBench task ID to our internal string representation."""
    if competition is None:
        competition = ACTIVE_COMPETITION
    if task_id in competition.codabench_task_map:
        return competition.codabench_task_map[task_id]
    if not competition.codabench_task_map:
        return competition.default_task
    raise ValueError(f"Unknown task ID: {task_id}")


def get_submission_url(submission_id: int, session_id: str | None = None) -> str:
    """Returns the URL to download a submission."""
    session_id = session_id or get_environ_session_id()
    # noinspection SpellCheckingInspection
    response = httpx2.get(BASE_URL + f"submissions/{submission_id}/get_details", cookies={"sessionid": session_id})
    response.raise_for_status()
    return _CodaBenchSubmissionDetail.model_validate(response.json()).data_file


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
    path_or_url: str | None = None
    system_id_override: str | None = None

    @property
    def system_id(self) -> str:
        if self.system_id_override is not None:
            return self.system_id_override
        if is_only_latest_submission_per_user():
            return self.user
        return f"{self.user}-{self.id}"

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: Any) -> bool:
        return self.id == other.id if isinstance(other, type(self)) else NotImplemented

    def __lt__(self, other: Any) -> bool:
        return self.id < other.id

    def compute_path_or_url(self, session_id: str | None = None) -> str:
        """Computes the path or URL to the submission file."""
        return self.path_or_url or get_submission_url(self.id, session_id=session_id)


def _list_submission_dicts(
    competition_id: int | None = None,
    phase_id: int | None = None,
    session_id: str | None = None,
) -> list[dict[str, Any]]:
    """List all submission dicts for a competition or phase."""
    session_id = session_id or get_environ_session_id()
    if not is_session_id_valid(session_id):
        raise ValueError("The provided session ID is not valid.")

    query_params: dict[str, Any] = {"show_is_soft_deleted": True}

    if phase_id is None and competition_id is None:
        phase_id = ACTIVE_COMPETITION.phase_id
        competition_id = ACTIVE_COMPETITION.codabench_competition_id

    if phase_id is None:
        query_params["phase__competition"] = competition_id
    else:
        query_params["phase"] = phase_id

    # noinspection SpellCheckingInspection
    response = httpx2.get(BASE_URL + "submissions/", params=query_params, cookies={"sessionid": session_id})

    response.raise_for_status()

    return response.json()


def list_submissions(
    competition_id: int | None = None,
    phase_id: int | None = None,
    session_id: str | None = None,
    competition: CompetitionConfig | None = None,
) -> Iterable[Submission]:
    """List all "parent" or single-task submissions for a competition."""
    if competition is None:
        competition = ACTIVE_COMPETITION

    if phase_id is None and competition_id is None:
        phase_id = competition.phase_id
        competition_id = competition.codabench_competition_id

    raw_submissions = _SUBMISSION_LIST_ADAPTER.validate_python(
        _list_submission_dicts(competition_id=competition_id, phase_id=phase_id, session_id=session_id)
    )

    # There's a concept of "parent" and "children" submissions.
    # It seems that if a submission is for multiple tasks,
    # it's a parent one and there's one child submission per task, which actually points to the same file.
    # If a submission is for a single task, then it's neither parent nor child -- just a single node.

    # We need both the parents and the children.
    # The parents are used to get the parent submission ID, which we use to define the system ID.
    # The children are used to check which tasks were submitted and passed the submission test.

    parentless_submissions: dict[int, Submission] = {}

    for item in raw_submissions:
        if not item.parent:  # If it's not a child submission.
            if item.children:
                # If it's a parent submission, we create it with an empty set of tasks.
                # If it ends up having no tasks, it'll be discarded later.
                parentless_submissions[item.id] = Submission(
                    id=item.id, user=item.owner, date=item.created_when, is_deleted=item.is_soft_deleted
                )
            else:  # If it's not a parent submission.
                # If it's a single-task submission, we need to check that it passed the submission test.
                assert item.task is not None, "Single-task submission must have a task."
                task = task_id_to_task(item.task.id, competition=competition)
                # For some reason, the "scores" field could be empty:
                test_passed = bool(item.scores and item.scores[0].score)
                parentless_submissions[item.id] = Submission(
                    id=item.id,
                    user=item.owner,
                    date=item.created_when,
                    tasks=[task],
                    tests_passed=[test_passed],
                    is_deleted=item.is_soft_deleted,
                )

    for item in raw_submissions:
        if parent_submission_id := item.parent:
            parent_submission = parentless_submissions[parent_submission_id]
            assert item.task is not None, "Child submission must have a task."
            parent_submission.tasks.append(task_id_to_task(item.task.id, competition=competition))
            # For some reason, the "scores" field could be empty:
            parent_submission.tests_passed.append(bool(item.scores and item.scores[0].score))

            assert parent_submission.is_deleted or not item.is_soft_deleted, (
                "If a parent submission is not deleted, its children cannot be deleted."
            )

    assert all(submission.tasks for submission in parentless_submissions.values()), (
        "All submissions must have at least one task."
    )

    return parentless_submissions.values()
