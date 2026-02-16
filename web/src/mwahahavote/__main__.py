import logging
import os
import random
import string
from collections.abc import AsyncIterator, Iterable
from datetime import timedelta
from typing import Any, cast, override

import httpx
import sentry_sdk
from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sentry_sdk.integrations.logging import LoggingIntegration
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from mwahahavote import database
from mwahahavote.database import TASK_CHOICES, VOTE_CHOICES, Battle, Task, VoteString, prompt_id_to_task

logger = logging.getLogger(__name__)

PHASE_ID = 15785

PROLIFIC_COMPLETION_CODES: dict[Task, str] = {
    "a-en": "C1O4X1ZA",
    "a-es": "C5SU1Q6U",
    "a-zh": "C11V5NNR",
    "b1": "CCI5PILX",
    "b2": "C13AY54S",
}

TURNSTILE_SECRET_KEY = os.environ["TURNSTILE_SECRET_KEY"]
IS_LOCAL_DEVELOPMENT = "VIRTUAL_HOST" not in os.environ

REQUEST_BATTLE_BATCH_SIZE = 3

SESSION_ID_MAX_AGE = int(timedelta(weeks=1000).total_seconds())

sentry_sdk.init(
    send_default_pii=True,
    traces_sample_rate=1.0,
    integrations=[LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)],
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,  # type: ignore[arg-type]
    allow_origins=[
        "http://localhost:5000",
        *(f"https://{host.strip()}" for host in os.environ.get("VIRTUAL_HOST", "").split(",") if host.strip()),
    ],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
    max_age=600,
)

templates = Jinja2Templates(directory="src/mwahahavote/templates")


def _generate_id() -> str:  # From https://stackoverflow.com/a/2257449/1165181
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=100))


def _get_session_id(request: Request) -> str:
    if (prolific_id := request.query_params.get("PROLIFIC_PID")) and (
        session_id := request.query_params.get("SESSION_ID")
    ):
        return f"prolific-id-{prolific_id}-{session_id}"
    else:
        return request.cookies.get("id") or _generate_id()


class CacheControlMiddleware(BaseHTTPMiddleware):
    @override
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        session_id = _get_session_id(request)
        request.state.session_id = session_id

        response = await call_next(request)
        response.headers["Cache-Control"] = "max-age=0, no-cache"
        response.set_cookie(key="id", value=session_id, max_age=SESSION_ID_MAX_AGE)
        return response


app.add_middleware(CacheControlMiddleware)  # type: ignore[arg-type]


async def _passes_turnstile(token: str) -> bool:
    if IS_LOCAL_DEVELOPMENT:
        return True

    if not TURNSTILE_SECRET_KEY:
        return False

    async with httpx.AsyncClient() as client:
        try:
            return (
                (
                    await client.post(
                        "https://challenges.cloudflare.com/turnstile/v0/siteverify",
                        json={"secret": TURNSTILE_SECRET_KEY, "response": token},
                        timeout=5.0,
                    )
                )
                .json()
                .get("success", False)
            )
        except Exception:
            logger.exception("Turnstile verification error.")
            return True


def _simplify_battle_object(battle: Battle) -> dict[str, Any]:
    """Removes redundant fields and simplifies the battle representation for JSON serialization."""
    return {
        "prompt_id": battle.prompt.id,
        "prompt": battle.prompt.verbalized,
        "prompt_image_url": battle.prompt.url,
        "system_id_a": battle.output_a.system.id,
        "output_a": battle.output_a.text,
        "system_id_b": battle.output_b.system.id,
        "output_b": battle.output_b.text,
    }


async def _get_battle_objects(
    phase_id: int, session_id: str, task: Task, batch_size: int, ignored_output_ids: Iterable[tuple[str, str]] = ()
) -> AsyncIterator[dict[str, Any]]:
    num_returned = 0

    async for battle in database.random_least_voted_unseen_battles(
        phase_id, session_id, task, batch_size, ignored_output_ids
    ):
        yield _simplify_battle_object(battle)
        num_returned += 1

    if (num_missing := batch_size - num_returned) > 0:
        async for battle in database.random_battles(phase_id, task, batch_size=num_missing):
            yield _simplify_battle_object(battle)


@app.get("/battles")
async def battles_route(request: Request, task: str = Query("a-en")) -> list[dict[str, Any]]:
    task = cast(Task, task if task in TASK_CHOICES else "a-en")
    return [
        battle
        async for battle in _get_battle_objects(PHASE_ID, request.state.session_id, task, REQUEST_BATTLE_BATCH_SIZE)
    ]


@app.post("/vote")
# Can't set the return type because it'd be like `dict[str, Any] | HTTPException` but that'd raise a `FastAPIError`:
async def vote_and_get_new_battle_route(request: Request) -> Any:
    session_id = request.state.session_id

    form_data = await request.form()

    turnstile_token = str(form_data.get("turnstile_token", ""))
    if not await _passes_turnstile(turnstile_token):
        return HTTPException(status_code=403, detail="Turnstile verification failed")

    if all(
        key in form_data
        for key in ("prompt_id", "system_id_a", "system_id_b", "vote", "is_offensive_a", "is_offensive_b")
    ):
        vote_str = str(form_data["vote"])
        if vote_str not in VOTE_CHOICES:
            raise ValueError(f"Invalid vote: {vote_str}")
        vote = cast(VoteString, vote_str)

        database.add_vote(
            session_id,
            str(form_data["prompt_id"]),
            str(form_data["system_id_a"]),
            str(form_data["system_id_b"]),
            vote,
            is_offensive_a=str(form_data["is_offensive_a"]).lower() == "true",
            is_offensive_b=str(form_data["is_offensive_b"]).lower() == "true",
        )

    task: Task = "a-en"
    if "prompt_id" in form_data:
        try:
            task = prompt_id_to_task(str(form_data["prompt_id"]))
        except ValueError:
            logger.exception("Prompt id not found.")

    return await anext(
        _get_battle_objects(
            PHASE_ID,
            session_id,
            task,
            batch_size=1,
            ignored_output_ids=[  # type: ignore
                tuple(str_.split("-", maxsplit=1))  # type: ignore
                for str_ in form_data.getlist("ignored_output_ids[]")
            ],
        ),
        {},
    )


@app.get("/l")
def leaderboard_route() -> Response:
    return FileResponse("src/mwahahavote/static/leaderboard.html")


@app.get("/session-vote-count")
def session_vote_count_route(request: Request) -> int:
    return database.session_vote_count_without_skips(request.state.session_id)


@app.get("/vote-count")
def vote_count_route() -> int:
    return database.vote_count_without_skips()


@app.get("/votes-per-session")
def get_votes_per_session_route() -> dict[str, int]:
    return database.get_votes_per_session(PHASE_ID)


@app.get("/votes.csv")
def get_votes() -> Response:
    csv_content = database.get_votes(PHASE_ID).to_csv(index=False)
    return Response(
        content=csv_content, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=votes.csv"}
    )


@app.post("/prolific-consent")
def prolific_consent_route(request: Request) -> Response:
    database.prolific_consent(request.state.session_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/prolific-finish")
async def prolific_finish_route(request: Request) -> Response:
    form_data = await request.form()
    comments = str(form_data.get("comments", ""))

    task_str = str(form_data.get("task", "a-en"))
    task: Task = cast(Task, task_str if task_str in TASK_CHOICES else "a-en")

    database.prolific_finish(request.state.session_id, comments)

    completion_code = PROLIFIC_COMPLETION_CODES[task]
    return RedirectResponse(
        url=f"https://app.prolific.co/submissions/complete?cc={completion_code}", status_code=status.HTTP_303_SEE_OTHER
    )


@app.get("/stats")
def stats_route(request: Request) -> Response:
    stats = database.stats()
    stats["histogram"] = [["Vote count", "Prompt count"]] + [[str(a), b] for a, b in stats["histogram"].items()]
    stats["votes-per-category"] = [["Vote", "Prompt count"], *list(stats["votes-per-category"].items())]
    return templates.TemplateResponse("stats.html", {"request": request, "stats": stats})


app.mount("/", StaticFiles(directory="src/mwahahavote/static", html=True), name="static")
