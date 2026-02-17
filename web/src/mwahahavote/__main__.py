import logging
import os
import random
import string
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, NamedTuple, TypedDict, cast

import httpx
import sentry_sdk
import sqlalchemy.ext.asyncio
from cryptography.fernet import Fernet, InvalidToken
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, ORJSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sentry_sdk.integrations.logging import LoggingIntegration
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from mwahahavote import database
from mwahahavote.database import TASK_CHOICES, VOTE_CHOICES, Battle, Task, VoteString

logger = logging.getLogger(__name__)

PHASE_ID = 15785

# noinspection SpellCheckingInspection
PROLIFIC_COMPLETION_CODES: dict[Task, str] = {
    "a-en": "C1O4X1ZA",
    "a-es": "C5SU1Q6U",
    "a-zh": "C11V5NNR",
    "b1": "CCI5PILX",
    "b2": "C13AY54S",
}

TURNSTILE_SECRET_KEY = os.environ["TURNSTILE_SECRET_KEY"]
IS_LOCAL_DEVELOPMENT = "VIRTUAL_HOST" not in os.environ

REQUEST_BATTLE_BATCH_SIZE = 16

SESSION_ID_MAX_AGE = int(timedelta(weeks=1000).total_seconds())

sentry_sdk.init(
    send_default_pii=True,
    traces_sample_rate=1.0,
    integrations=[LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)],
)


fernet_cipher = Fernet(os.environ["BATTLE_TOKEN_SECRET"].encode())


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[dict[str, Any]]:
    async with database.create_engine() as database_engine:
        yield {"database_engine": database_engine}


def _generate_id() -> str:  # From https://stackoverflow.com/a/2257449/1165181
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=100))


def _get_session_id(request: Request) -> str:
    if (prolific_id := request.query_params.get("PROLIFIC_PID")) and (
        session_id := request.query_params.get("SESSION_ID")
    ):
        return f"prolific-id-{prolific_id}-{session_id}"
    else:
        return request.cookies.get("id") or _generate_id()


class CacheControlMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        session_id = _get_session_id(Request(scope))
        scope.setdefault("state", {})["session_id"] = session_id

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers.append("Cache-Control", "max-age=0, no-cache")
                headers.append("Set-Cookie", f"id={session_id}; Max-Age={SESSION_ID_MAX_AGE}; Path=/; SameSite=lax")
            await send(message)

        await self.app(scope, receive, send_with_headers)


app = FastAPI(lifespan=_lifespan)

app.add_middleware(CacheControlMiddleware)  # type: ignore[arg-type]
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


def _encrypt_as_battle_token(prompt_id: str, system_id_a: str, system_id_b: str) -> str:
    return fernet_cipher.encrypt(f"{prompt_id}|{system_id_a}|{system_id_b}".encode()).decode("ascii")


class BattleId(NamedTuple):
    prompt_id: str
    system_id_a: str
    system_id_b: str


def _decrypt_battle_token(id_: str) -> BattleId:
    try:
        plaintext = fernet_cipher.decrypt(id_.encode(), ttl=None).decode("utf-8")

        parts = plaintext.split("|")
        if len(parts) != 3:
            raise ValueError("Invalid token format")

        prompt_id, system_id_a, system_id_b = parts

        if not prompt_id or not system_id_a or not system_id_b:
            raise ValueError("Token contains empty IDs")

        return BattleId(prompt_id, system_id_a, system_id_b)
    except InvalidToken as e:
        raise ValueError("Invalid or tampered battle token") from e
    except Exception as e:
        raise ValueError("Failed to decrypt battle token") from e


def _perturb_text(text: str) -> str:
    """Return a perturbed version of the input text using spacing modifications. The resulting text looks nearly
    identical to the human eye but has the following non-deterministic slight modifications:

    - Sometimes doubles single spaces (one space becomes two)
    - Sometimes removes spaces after periods and other punctuation
    """

    if not text:
        return text

    double_space_rate = random.uniform(0.02, 0.1)
    remove_space_rate = random.uniform(0.15, 0.3)

    result: list[str] = []
    i = 0

    while i < len(text):
        char = text[i]

        # Handle periods followed by a space:
        if char == "." and i + 1 < len(text) and text[i + 1] == " ":
            result.append(".")
            # Use position-based seed for consistency
            position_hash = (random.getstate()[1][i % 624] + i) / (2**32)
            if position_hash < remove_space_rate:
                # Remove the space after a period
                i += 1  # Skip the space
            i += 1
            continue

        # Handle other punctuation followed by space (,!?;:):
        elif char in "!?,;:" and i + 1 < len(text) and text[i + 1] == " ":
            result.append(char)
            # Use position-based seed for consistency
            position_hash = (random.getstate()[1][i % 624] + i) / (2**32)
            if position_hash < remove_space_rate * 0.5:  # Less aggressive for other punctuation
                # Remove the space after punctuation
                i += 1  # Skip the space
            i += 1
            continue

        # Handle regular spaces:
        elif char == " ":
            # Use position-based seed for consistency
            position_hash = (random.getstate()[1][i % 624] + i) / (2**32)
            if position_hash < double_space_rate:
                result.append("  ")
            else:
                result.append(" ")

        else:
            result.append(char)

        i += 1

    return "".join(result)


async def _passes_turnstile(token: str) -> bool:
    if IS_LOCAL_DEVELOPMENT:
        return True

    if not TURNSTILE_SECRET_KEY:
        return False

    if not token:
        return False

    async with httpx.AsyncClient() as client:
        try:
            return (
                (
                    await client.post(
                        "https://challenges.cloudflare.com/turnstile/v0/siteverify",
                        json={"secret": TURNSTILE_SECRET_KEY, "response": token},
                    )
                )
                .json()
                .get("success", False)
            )
        except Exception:
            logger.exception("Turnstile verification error.")
            return True


class SimplifiedBattleDict(TypedDict):
    token: str
    prompt: str | None
    prompt_image_url: str | None
    output_a: str
    output_b: str


def _simplify_battle_object(battle: Battle) -> SimplifiedBattleDict:
    """Removes redundant fields and simplifies the battle representation for JSON serialization."""
    return {
        "token": _encrypt_as_battle_token(battle.prompt.id, battle.output_a.system.id, battle.output_b.system.id),
        "prompt": _perturb_text(battle.prompt.verbalized) if battle.prompt.verbalized else battle.prompt.verbalized,
        "prompt_image_url": battle.prompt.url,  # TODO: perturb the URL? We could add stuff like useless query params.
        "output_a": _perturb_text(battle.output_a.text),
        "output_b": _perturb_text(battle.output_b.text),
    }


async def _get_battle_objects(
    engine: sqlalchemy.ext.asyncio.AsyncEngine,
    phase_id: int,
    session_id: str,
    task: Task,
    batch_size: int,
    ignored_output_ids: Iterable[tuple[str, str]] = (),
) -> AsyncIterator[SimplifiedBattleDict]:
    num_returned = 0

    async for battle in database.random_least_voted_unseen_battles(
        engine, phase_id, session_id, task, batch_size, ignored_output_ids
    ):
        yield _simplify_battle_object(battle)
        num_returned += 1

    if (num_missing := batch_size - num_returned) > 0:
        async for battle in database.random_battles(engine, phase_id, task, batch_size=num_missing):
            yield _simplify_battle_object(battle)


@app.get("/battles", response_class=ORJSONResponse)
async def battles_route(
    request: Request,
    task: str = Query("a-en"),
    batch_size: int = Query(REQUEST_BATTLE_BATCH_SIZE),
    ignored_tokens: Iterable[str] = Query(()),
) -> list[SimplifiedBattleDict]:
    task = cast(Task, task if task in TASK_CHOICES else "a-en")

    batch_size = max(min(batch_size, REQUEST_BATTLE_BATCH_SIZE), 1)

    ignored_output_ids: list[tuple[str, str]] = []
    for ignored_token in ignored_tokens:
        try:
            ignored_prompt_id, ignored_system_id_a, ignored_system_id_b = _decrypt_battle_token(str(ignored_token))
            ignored_output_ids.append((ignored_prompt_id, ignored_system_id_a))
            ignored_output_ids.append((ignored_prompt_id, ignored_system_id_b))
        except ValueError:
            logger.exception(f"Invalid battle token in ignored_tokens: {ignored_token}")

    return [
        battle
        async for battle in _get_battle_objects(
            request.state.database_engine, PHASE_ID, request.state.session_id, task, batch_size, ignored_output_ids
        )
    ]


@app.post("/vote")
async def vote_route(request: Request, background_tasks: BackgroundTasks) -> Response:
    form_data = await request.form()

    if not await _passes_turnstile(str(form_data.get("turnstile_token", ""))):
        raise HTTPException(status_code=403, detail="Turnstile verification failed")

    if all(key in form_data for key in ("vote", "is_offensive_a", "is_offensive_b")):
        vote_str = str(form_data["vote"])
        if vote_str not in VOTE_CHOICES:
            raise HTTPException(status_code=400, detail="Invalid vote")
        vote = cast(VoteString, vote_str)

        if not (battle_token := str(form_data.get("token", ""))):
            raise HTTPException(status_code=400, detail="Battle token required")

        try:
            prompt_id, system_id_a, system_id_b = _decrypt_battle_token(battle_token)
        except ValueError as e:
            raise HTTPException(status_code=400, detail="Invalid battle ID") from e

        background_tasks.add_task(
            database.add_vote,
            request.state.database_engine,
            request.state.session_id,
            prompt_id,
            system_id_a,
            system_id_b,
            vote,
            is_offensive_a=str(form_data["is_offensive_a"]).lower() == "true",
            is_offensive_b=str(form_data["is_offensive_b"]).lower() == "true",
        )

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/l")
async def leaderboard_route() -> Response:
    return FileResponse("src/mwahahavote/static/leaderboard.html")


@app.get("/session-vote-count")
async def session_vote_count_route(request: Request) -> int:
    return await database.session_vote_count_without_skips(request.state.database_engine, request.state.session_id)


@app.get("/vote-count")
async def vote_count_route(request: Request) -> int:
    return await database.vote_count_without_skips(request.state.database_engine)


@app.get("/votes-per-session", response_class=ORJSONResponse)
async def get_votes_per_session_route(request: Request) -> dict[str, int]:
    return await database.get_votes_per_session(request.state.database_engine, PHASE_ID)


@app.get("/votes.csv")
async def get_votes_route(request: Request) -> Response:
    return Response(
        content=(await database.get_votes(request.state.database_engine, PHASE_ID)).to_csv(index=False),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=votes.csv"},
    )


@app.post("/prolific-consent")
async def prolific_consent_route(request: Request, background_tasks: BackgroundTasks) -> Response:
    background_tasks.add_task(database.prolific_consent, request.state.database_engine, request.state.session_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/prolific-finish")
async def prolific_finish_route(request: Request, background_tasks: BackgroundTasks) -> Response:
    form_data = await request.form()

    comments = str(form_data.get("comments", ""))

    task_str = str(form_data.get("task", "a-en"))
    task: Task = cast(Task, task_str if task_str in TASK_CHOICES else "a-en")

    background_tasks.add_task(
        database.prolific_finish, request.state.database_engine, request.state.session_id, comments
    )

    return RedirectResponse(
        url=f"https://app.prolific.co/submissions/complete?cc={PROLIFIC_COMPLETION_CODES[task]}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@app.get("/stats")
async def stats_route(request: Request) -> Response:
    stats = await database.stats(request.state.database_engine)
    stats["histogram"] = [["Vote count", "Prompt count"]] + [[str(a), b] for a, b in stats["histogram"].items()]
    stats["votes-per-category"] = [["Vote", "Prompt count"], *list(stats["votes-per-category"].items())]
    return templates.TemplateResponse("stats.html", {"request": request, "stats": stats})


app.mount("/", StaticFiles(directory="src/mwahahavote/static", html=True), name="static")
