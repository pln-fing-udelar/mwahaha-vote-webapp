import os
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import requests

# Reference API (old?): https://qtim-challenges.southcentralus.cloudapp.azure.com/api/docs/

BASE_URL = "https://www.codabench.org/api/"

COMPETITION_ID = 9719


def get_environ_session_id() -> str:
    return os.environ["CODABENCH_SESSION_ID"]


@dataclass(frozen=True, order=True)
class Submission:
    id: int
    user: str

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: Any) -> bool:
        return self.id == other.id if isinstance(other, type(self)) else NotImplemented


def list_submissions(competition_id: int = COMPETITION_ID, session_id: str | None = None) -> Iterable[Submission]:
    """List all submissions for a competition that passed the submission test and were not deleted."""

    # TODO: filter by phase?

    session_id = session_id or get_environ_session_id()
    # noinspection SpellCheckingInspection
    response = requests.get(
        BASE_URL + "submissions",
        params={"phase__competition": competition_id, "show_is_soft_deleted": False},
        cookies={"sessionid": session_id},
    )
    response.raise_for_status()
    for dict_ in response.json():  # For some reason, the "scores" field could be empty:
        if bool(float(next(iter(dict_["scores"]), {"score": "0.0000000000"})["score"])):
            yield Submission(id=dict_["id"], user=dict_["owner"])


def get_submission_url(submission_id: int, session_id: str | None = None) -> str:
    session_id = session_id or get_environ_session_id()
    # noinspection SpellCheckingInspection
    response = requests.get(
        BASE_URL + f"submissions/{submission_id}/get_details",
        cookies={"sessionid": session_id},
    )
    response.raise_for_status()
    return response.json()["data_file"]
