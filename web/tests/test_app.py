import re
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx2
import pytest
import sqlalchemy.ext.asyncio
from starlette.requests import Request

import mwahahavote.__main__ as main_mod
from mwahahavote import database
from mwahahavote.__main__ import (
    BattleId,
    CacheControlMiddleware,
    _decrypt_battle_token,
    _encrypt_as_battle_token,
    _generate_id,
    _get_battle_objects,
    _get_session_id,
    _lifespan,
    _passes_turnstile,
    _perturb_text,
    _perturb_url,
    _simplify_battle_object,
    app,
    fernet_cipher,
)
from mwahahavote.database import Battle, Output, Prompt, System


def test_generate_id() -> None:
    session_id = _generate_id()
    assert len(session_id) == 16
    assert re.fullmatch(r"[0-9a-f]{16}", session_id)


def test_get_session_id() -> None:
    # Prolific query params take precedence
    scope_prolific: dict[str, Any] = {
        "type": "http",
        "query_string": b"PROLIFIC_PID=pid123&SESSION_ID=sess456",
        "headers": [(b"cookie", b"id=existingcookie")],
    }
    assert _get_session_id(Request(scope_prolific)) == "prolific-id-pid123-sess456"

    # Existing cookie used when not Prolific
    scope_cookie: dict[str, Any] = {
        "type": "http",
        "query_string": b"",
        "headers": [(b"cookie", b"id=mycookie12345678")],
    }
    assert _get_session_id(Request(scope_cookie)) == "mycookie12345678"

    # New ID generated when neither present
    scope_empty: dict[str, Any] = {
        "type": "http",
        "query_string": b"",
        "headers": [],
    }
    generated = _get_session_id(Request(scope_empty))
    assert len(generated) == 16


async def test_lifespan() -> None:
    mock_engine = MagicMock()

    @database.asynccontextmanager
    async def fake_create_engine() -> Any:
        yield mock_engine

    with patch("mwahahavote.database.create_engine", fake_create_engine):
        async with _lifespan(app) as state:
            assert state == {"database_engine": mock_engine}


async def test_cache_control_middleware_non_http() -> None:
    inner_app = AsyncMock()
    middleware = CacheControlMiddleware(inner_app)
    scope: dict[str, Any] = {"type": "lifespan"}
    receive = AsyncMock()
    send = AsyncMock()

    await middleware(scope, receive, send)
    inner_app.assert_awaited_once_with(scope, receive, send)


def test_encrypt_and_decrypt_battle_token() -> None:
    token = _encrypt_as_battle_token("en_1", "sys_a", "sys_b")
    decrypted = _decrypt_battle_token(token)
    assert decrypted == BattleId("en_1", "sys_a", "sys_b")

    # Tampered token
    with pytest.raises(ValueError, match="Invalid or tampered battle token"):
        _decrypt_battle_token("invalid-token-string")

    # Wrong number of parts
    bad_parts_token = fernet_cipher.encrypt(b"only_two|parts").decode("ascii")
    with pytest.raises(ValueError, match="Failed to decrypt battle token"):
        _decrypt_battle_token(bad_parts_token)

    # Empty ID in parts
    empty_id_token = fernet_cipher.encrypt(b"en_1||sys_b").decode("ascii")
    with pytest.raises(ValueError, match="Failed to decrypt battle token"):
        _decrypt_battle_token(empty_id_token)


def test_perturb_text() -> None:
    assert _perturb_text("") == ""

    sample = "Hello world. How are you! Good, thanks; yes: really? Sure. Next."
    # Force all perturbation branches (hash < rate) by mocking random.uniform to 1.0
    with patch("random.uniform", return_value=1.0):
        perturbed_high = _perturb_text(sample)
        assert "  " in perturbed_high
        assert ".H" in perturbed_high

    # Force non-perturbation branches (hash >= rate) by mocking random.uniform to 0.0
    with patch("random.uniform", return_value=0.0):
        perturbed_zero = _perturb_text(sample)
        assert perturbed_zero == sample


async def test_passes_turnstile(monkeypatch: pytest.MonkeyPatch) -> None:
    # Local development bypasses Turnstile
    monkeypatch.setattr(main_mod, "IS_LOCAL_DEVELOPMENT", True)
    assert await _passes_turnstile("") is True

    # Production checks
    monkeypatch.setattr(main_mod, "IS_LOCAL_DEVELOPMENT", False)

    # Missing secret key -> False
    monkeypatch.setattr(main_mod, "TURNSTILE_SECRET_KEY", "")
    assert await _passes_turnstile("some-token") is False

    # Missing token -> False
    monkeypatch.setattr(main_mod, "TURNSTILE_SECRET_KEY", "secret-key")
    assert await _passes_turnstile("") is False

    # Turnstile API returns success: True
    mock_response_ok = MagicMock()
    mock_response_ok.json.return_value = {"success": True}
    mock_client_ok = AsyncMock()
    mock_client_ok.post.return_value = mock_response_ok
    mock_client_ok.__aenter__.return_value = mock_client_ok

    with patch("httpx2.AsyncClient", return_value=mock_client_ok):
        assert await _passes_turnstile("valid-token") is True

    # Turnstile API returns success: False
    mock_response_fail = MagicMock()
    mock_response_fail.json.return_value = {"success": False}
    mock_client_fail = AsyncMock()
    mock_client_fail.post.return_value = mock_response_fail
    mock_client_fail.__aenter__.return_value = mock_client_fail

    with patch("httpx2.AsyncClient", return_value=mock_client_fail):
        assert await _passes_turnstile("invalid-token") is False

    # Turnstile API raises exception -> fails open (True)
    mock_client_err = AsyncMock()
    mock_client_err.post.side_effect = RuntimeError("Network error")
    mock_client_err.__aenter__.return_value = mock_client_err

    with patch("httpx2.AsyncClient", return_value=mock_client_err):
        assert await _passes_turnstile("token-on-error") is True


def test_simplify_battle_object() -> None:
    assert _perturb_url("") == ""
    assert re.fullmatch(r"https://example\.com/pic\.gif\?_=[0-9a-f]{16}", _perturb_url("https://example.com/pic.gif"))
    assert re.fullmatch(
        r"https://example\.com/pic\.gif\?v=1&_=[0-9a-f]{16}", _perturb_url("https://example.com/pic.gif?v=1")
    )

    prompt_with_text = Prompt(id="en_1", headline="Some news")
    battle = Battle(
        output_a=Output(prompt=prompt_with_text, system=System("sys_a"), text="Joke A"),
        output_b=Output(prompt=prompt_with_text, system=System("sys_b"), text="Joke B"),
    )
    simplified = _simplify_battle_object(battle)
    assert _decrypt_battle_token(simplified["token"]) == BattleId("en_1", "sys_a", "sys_b")
    assert simplified["prompt"] is not None
    assert simplified["prompt_image_url"] is None

    prompt_url_only = Prompt(id="img_1", url="https://example.com/pic.gif")
    battle_img = Battle(
        output_a=Output(prompt=prompt_url_only, system=System("sys_a"), text="Caption A"),
        output_b=Output(prompt=prompt_url_only, system=System("sys_b"), text="Caption B"),
    )
    simplified_img = _simplify_battle_object(battle_img)
    assert simplified_img["prompt"] is None
    assert simplified_img["prompt_image_url"] is not None
    assert re.fullmatch(r"https://example\.com/pic\.gif\?_=[0-9a-f]{16}", simplified_img["prompt_image_url"])


async def test_get_battle_objects(
    seeded_db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
) -> None:
    prompt = Prompt(id="en_1", headline="News")
    sample_battle = Battle(
        output_a=Output(prompt=prompt, system=System("sys_a"), text="A"),
        output_b=Output(prompt=prompt, system=System("sys_b"), text="B"),
    )

    async def fake_unseen(*_args: Any, **_kwargs: Any) -> Any:
        yield sample_battle
        yield sample_battle

    with patch("mwahahavote.database.random_least_voted_unseen_battles", fake_unseen):
        results = [b async for b in _get_battle_objects(seeded_db_engine, 1, "sess_1", "t3", batch_size=2)]
        assert len(results) == 2


async def test_routes_end_to_end(
    seeded_db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(main_mod, "IS_LOCAL_DEVELOPMENT", True)

    # Inject seeded_db_engine into request.state via middleware wrapper
    async def inject_db(scope: Any, receive: Any, send: Any) -> None:
        scope.setdefault("state", {})["database_engine"] = seeded_db_engine
        await app(scope, receive, send)

    transport = httpx2.ASGITransport(app=inject_db)
    async with httpx2.AsyncClient(transport=transport, base_url="http://testserver") as client:
        # GET /battles with invalid task (falls back to 't3') and out-of-range batch_size (clamped to [1, 4])
        resp_battles_clamped = await client.get("/battles", params={"task": "invalid_task", "batch_size": "99"})
        assert resp_battles_clamped.status_code == 200
        assert len(resp_battles_clamped.json()) <= 4

        resp_battles_min = await client.get("/battles", params={"task": "invalid_task", "batch_size": "0"})
        assert resp_battles_min.status_code == 200
        assert len(resp_battles_min.json()) == 1

        # GET /battles with valid and invalid ignored_tokens
        valid_ignored = _encrypt_as_battle_token("en_1", "sys_a", "sys_b")
        resp_battles = await client.get(
            "/battles",
            params=[
                ("task", "t3"),
                ("batch_size", "2"),
                ("ignored_tokens[]", valid_ignored),
                ("ignored_tokens[]", "invalid-token"),
            ],
        )
        assert resp_battles.status_code == 200
        assert resp_battles.headers["cache-control"] == "max-age=0, no-cache"
        assert "id=" in resp_battles.headers["set-cookie"]
        battles_data = resp_battles.json()
        assert len(battles_data) == 2
        battle_token = battles_data[0]["token"]

        # POST /vote - valid vote
        resp_vote = await client.post(
            "/vote",
            data={
                "token": battle_token,
                "vote": "a",
                "is_offensive_a": "false",
                "is_offensive_b": "true",
                "turnstile_token": "tok",
            },
        )
        assert resp_vote.status_code == 204

        # POST /vote - missing form fields (no-op 204)
        resp_vote_incomplete = await client.post("/vote", data={"turnstile_token": "tok"})
        assert resp_vote_incomplete.status_code == 204

        # POST /vote - invalid vote string (400)
        resp_vote_bad_choice = await client.post(
            "/vote",
            data={
                "token": battle_token,
                "vote": "invalid",
                "is_offensive_a": "false",
                "is_offensive_b": "false",
            },
        )
        assert resp_vote_bad_choice.status_code == 400

        # POST /vote - missing battle token (400)
        resp_vote_no_token = await client.post(
            "/vote",
            data={
                "token": "",
                "vote": "a",
                "is_offensive_a": "false",
                "is_offensive_b": "false",
            },
        )
        assert resp_vote_no_token.status_code == 400

        # POST /vote - invalid battle token (400)
        resp_vote_bad_token = await client.post(
            "/vote",
            data={
                "token": "tampered-token",
                "vote": "a",
                "is_offensive_a": "false",
                "is_offensive_b": "false",
            },
        )
        assert resp_vote_bad_token.status_code == 400

        # POST /vote - Turnstile failure (403)
        with patch("mwahahavote.__main__._passes_turnstile", AsyncMock(return_value=False)):
            resp_vote_403 = await client.post(
                "/vote",
                data={
                    "token": battle_token,
                    "vote": "a",
                    "is_offensive_a": "false",
                    "is_offensive_b": "false",
                },
            )
            assert resp_vote_403.status_code == 403

        # GET /session-vote-count & /vote-count & /votes-per-session
        resp_svc = await client.get("/session-vote-count")
        assert resp_svc.status_code == 200
        assert resp_svc.json() == 1

        resp_vc = await client.get("/vote-count")
        assert resp_vc.status_code == 200
        assert resp_vc.json() == 1

        resp_vps = await client.get("/votes-per-session")
        assert resp_vps.status_code == 200
        assert sum(resp_vps.json().values()) == 1

        # GET /votes.csv
        resp_csv = await client.get("/votes.csv")
        assert resp_csv.status_code == 200
        assert resp_csv.headers["content-type"].startswith("text/csv")
        assert "attachment; filename=votes.csv" in resp_csv.headers["content-disposition"]
        assert "sys_a" in resp_csv.text or "sys_b" in resp_csv.text

        # GET /leaderboard
        resp_lb = await client.get("/leaderboard")
        assert resp_lb.status_code == 200
        assert "MWAHAHA Humor Gen Comp Leaderboard" in resp_lb.text

        # GET /stats
        resp_stats = await client.get("/stats")
        assert resp_stats.status_code == 200
        assert "Estadísticas" in resp_stats.text

        # GET / (static index.html)
        resp_index = await client.get("/")
        assert resp_index.status_code == 200
        assert "Which Computer Program Output is Funnier?" in resp_index.text
