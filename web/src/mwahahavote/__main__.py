import itertools
import logging
import os
import random
import string
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
        response = await call_next(request)
        response.headers["Cache-Control"] = "max-age=0, no-cache"
        response.set_cookie(key="id", value=_get_session_id(request), max_age=SESSION_ID_MAX_AGE)
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


@app.get("/battles")
def battles_route(request: Request, task: str = Query("a-en")) -> list[dict[str, Any]]:
    session_id = _get_session_id(request)

    if task not in TASK_CHOICES:
        task = "a-en"
    task = cast(Task, task)

    battles = [
        _simplify_battle_object(battle)
        for battle in database.random_least_voted_unseen_battles(PHASE_ID, session_id, task, REQUEST_BATTLE_BATCH_SIZE)
    ]

    if len(battles) < REQUEST_BATTLE_BATCH_SIZE:
        battles.extend(
            _simplify_battle_object(battle)
            for battle in database.random_battles(PHASE_ID, task, REQUEST_BATTLE_BATCH_SIZE - len(battles))
        )

    return battles


@app.post("/vote")
async def vote_and_get_new_battle_route(request: Request) -> dict[str, Any]:
    session_id = _get_session_id(request)

    form_data = await request.form()

    turnstile_token = str(form_data.get("turnstile_token", ""))
    if not await _passes_turnstile(turnstile_token):
        # Can't add `HTTPException` to the return type of this function because it'd raise a `FastAPIError`.
        return HTTPException(status_code=403, detail="Turnstile verification failed")  # type: ignore[invalid-return-type]

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
            pass

    ignored_output_id_strs = form_data.getlist("ignored_output_ids[]")
    ignored_output_ids: list[tuple[str, str]] = [tuple(str_.split("-", maxsplit=1)) for str_ in ignored_output_id_strs]  # type: ignore

    battles = (
        _simplify_battle_object(battle)
        for battle in itertools.chain(
            database.random_least_voted_unseen_battles(PHASE_ID, session_id, task, 1, ignored_output_ids),
            database.random_battles(PHASE_ID, task, 1),
        )
    )

    battle = next(iter(battles), {})

    return battle


@app.get("/l")
def leaderboard_route() -> Response:
    return FileResponse("src/mwahahavote/static/leaderboard.html")


@app.get("/session-vote-count")
def session_vote_count_route(request: Request) -> int:
    return database.session_vote_count_without_skips(_get_session_id(request))


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
    database.prolific_consent(_get_session_id(request))
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/prolific-finish")
async def prolific_finish_route(request: Request) -> Response:
    form_data = await request.form()
    comments = str(form_data.get("comments", ""))
    database.prolific_finish(_get_session_id(request), comments)
    return RedirectResponse(
        url="https://app.prolific.co/submissions/complete?cc=CC4WY7K5", status_code=status.HTTP_303_SEE_OTHER
    )


@app.get("/stats")
def stats_route(request: Request) -> Response:
    stats = database.stats()
    stats["histogram"] = [["Vote count", "Prompt count"]] + [[str(a), b] for a, b in stats["histogram"].items()]
    stats["votes-per-category"] = [["Vote", "Prompt count"], *list(stats["votes-per-category"].items())]
    return templates.TemplateResponse("stats.html", {"request": request, "stats": stats})


app.mount("/", StaticFiles(directory="src/mwahahavote/static", html=True), name="static")
