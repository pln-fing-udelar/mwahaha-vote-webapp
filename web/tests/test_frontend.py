import asyncio
import json
import socket
from collections.abc import AsyncGenerator
from typing import Any

import pytest
import sqlalchemy.ext.asyncio
import uvicorn
from playwright.async_api import Browser, Page, Route, async_playwright

import mwahahavote.__main__ as main_mod
from mwahahavote.__main__ import app


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
async def browser() -> AsyncGenerator[Browser]:
    async with async_playwright() as p:
        try:
            b = await p.chromium.launch(channel="chrome", headless=True, args=["--no-sandbox"])
        except Exception:
            b = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        try:
            yield b
        finally:
            await b.close()


@pytest.fixture
async def live_server_url(
    seeded_db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[str]:
    monkeypatch.setattr(main_mod, "IS_LOCAL_DEVELOPMENT", True)

    async def wrapped_app(scope: Any, receive: Any, send: Any) -> None:
        scope.setdefault("state", {})["database_engine"] = seeded_db_engine
        await app(scope, receive, send)

    port = _get_free_port()
    config = uvicorn.Config(wrapped_app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.02)

    try:
        yield f"http://localhost:{port}"
    finally:
        server.should_exit = True
        await task


async def _stub_external_trackers(page: Page) -> None:
    async def _empty_js(route: Route) -> None:
        await route.fulfill(status=200, content_type="application/javascript", body="")

    await page.route("https://challenges.cloudflare.com/**", _empty_js)
    await page.route("https://js.sentry-cdn.com/**", _empty_js)
    await page.route("https://www.googletagmanager.com/**", _empty_js)


async def test_voting_page_flow(browser: Browser, live_server_url: str) -> None:
    context = await browser.new_context()
    page = await context.new_page()
    await _stub_external_trackers(page)

    vote_requests: list[str] = []
    page.on("request", lambda req: vote_requests.append(req.post_data or "") if req.url.endswith("/vote") else None)

    await page.goto(live_server_url)
    assert await page.title() == "Which Computer Program Output is Funnier?"

    # Wait for initial battles to load and populate #output-a-text
    await page.wait_for_function("() => battles.length > 0 && $('#output-a-text').text().trim().length > 0")

    # Voting without Turnstile token shows CAPTCHA toast and does not post to /vote
    await page.click("#vote-left")
    toast_text = await page.locator(".md-toast").text_content()
    assert toast_text is not None and "CAPTCHA" in toast_text
    assert len(vote_requests) == 0

    # Provide Turnstile token, mark left as offensive, and vote left ("a")
    await page.evaluate("onTurnstileSuccess('test-turnstile-token')")
    await page.check("#is-offensive-left")

    async with page.expect_response(lambda resp: resp.url.endswith("/vote") and resp.status == 204):
        await page.click("#vote-left")

    assert len(vote_requests) == 1
    assert "vote=a" in vote_requests[0]
    assert "is_offensive_a=true" in vote_requests[0]
    assert await page.is_checked("#is-offensive-left") is False

    # Second vote triggers prefetch of replacement battles with ignored_tokens[]
    await page.evaluate("onTurnstileSuccess('test-turnstile-token-2')")
    async with (
        page.expect_request(lambda req: "ignored_tokens" in req.url) as prefetch_req_info,
        page.expect_response(lambda resp: resp.url.endswith("/vote") and resp.status == 204),
    ):
        await page.click("#tie")
    prefetch_req = await prefetch_req_info.value
    assert "batch_size=2" in prefetch_req.url

    # Test skip and right vote options
    await page.evaluate("onTurnstileSuccess('test-turnstile-token-3')")
    async with page.expect_response(lambda resp: resp.url.endswith("/vote") and resp.status == 204):
        await page.click("#skip")

    await page.evaluate("onTurnstileSuccess('test-turnstile-token-4')")
    async with page.expect_response(lambda resp: resp.url.endswith("/vote") and resp.status == 204):
        await page.click("#vote-right")
    assert len(vote_requests) == 4

    await context.close()


async def test_voting_page_empty_battles(browser: Browser, live_server_url: str) -> None:
    context = await browser.new_context()
    page = await context.new_page()
    await _stub_external_trackers(page)

    await page.route(
        "**/battles*",
        lambda route: route.fulfill(status=200, content_type="application/json", body="[]"),
    )

    await page.goto(live_server_url)
    await page.wait_for_selector("text=There are no battles to display for this task.")
    assert await page.locator("#answers").is_visible() is False
    await context.close()


async def test_cookie_consent_banner(browser: Browser, live_server_url: str) -> None:
    context = await browser.new_context()
    page = await context.new_page()
    await _stub_external_trackers(page)

    await page.goto(live_server_url)
    await page.wait_for_selector("#cookie-consent-banner")
    await page.click("#cookie-accept-all")

    consent_all = json.loads(await page.evaluate("localStorage.getItem('cookie-consent-v1')"))
    assert consent_all["analytics"] is True
    assert consent_all["essential"] is True

    # Re-open via Cookie Settings link and select Essential Only
    await page.click("#cookie-settings-link")
    await page.wait_for_selector("#cookie-essential-only")
    await page.click("#cookie-essential-only")

    consent_essential = json.loads(await page.evaluate("localStorage.getItem('cookie-consent-v1')"))
    assert consent_essential["analytics"] is False
    await context.close()


async def test_prolific_session_flow(browser: Browser, live_server_url: str) -> None:
    context = await browser.new_context()
    page = await context.new_page()
    await _stub_external_trackers(page)

    await page.goto(f"{live_server_url}/?PROLIFIC_PID=user999&SESSION_ID=sess888")
    await page.wait_for_function("() => battles.length > 0")

    # Prolific ID input is populated and consent modal is translated
    assert await page.input_value("#prolific-id") == "user999"
    assert await page.locator("[data-i18n='consent.title']").text_content() == "Information and Consent Form"

    # Wait for session-vote-count to initialize prolificVoteCount
    await page.wait_for_function("() => $('#prolific-vote-count').text().includes('Progress: 0/100')")

    # Submit consent form and dismiss help modal
    await page.evaluate(
        "() => { $('#prolific-consent input[type=checkbox]').prop('checked', true);"
        " $('#prolific-consent form').submit(); $('#helpModal').modal('hide'); }"
    )
    assert await page.evaluate("localStorage.getItem('consent-prolific-id-user999')") == "done"

    # Cast a vote at count 99 to trigger the finished modal at 100
    await page.evaluate("prolificVoteCount = 99; onTurnstileSuccess('tok'); vote('a');")
    await page.wait_for_function("() => $('#prolific-finished').hasClass('in')")

    # Test Spanish and Chinese modal translations
    await page.evaluate("translateModal('es')")
    assert await page.locator("[data-i18n='finished.title']").text_content() == "¡Terminaste de anotar!"
    await page.evaluate("translateModal('zh')")
    assert await page.locator("[data-i18n='finished.title']").text_content() == "您已完成标注！"  # noqa: RUF001

    await context.close()


async def test_leaderboard_page(browser: Browser, live_server_url: str) -> None:
    context = await browser.new_context()
    page = await context.new_page()
    await _stub_external_trackers(page)

    mock_scores = [
        {
            "system": "funny_bot",
            "rating": 1120.4,
            "variance": 15.0,
            "rating_q975": 1145.0,
            "rating_q025": 1095.0,
            "num_battles": 80,
            "final_ranking": 1,
        },
        {
            "system": "baseline",
            "rating": 1000.0,
            "variance": 10.0,
            "rating_q975": 1020.0,
            "rating_q025": 980.0,
            "num_battles": 120,
            "final_ranking": 2,
        },
        {
            "system": "new_comer",
            "rating": None,
            "variance": None,
            "rating_q975": None,
            "rating_q025": None,
            "num_battles": 4,
            "final_ranking": None,
        },
    ]

    await page.route(
        "**/scores/t3.json",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            headers={"Last-Modified": "Wed, 28 Oct 2026 12:00:00 GMT"},
            body=json.dumps(mock_scores),
        ),
    )

    await page.goto(f"{live_server_url}/leaderboard")
    await page.wait_for_selector("#leaderboard-body tr")

    rows_text = await page.locator("#leaderboard-body").text_content()
    assert rows_text is not None
    assert "funny_bot" in rows_text
    assert "baseline" in rows_text
    assert "new_comer" in rows_text
    assert "TBD" in rows_text

    total_votes_text = await page.locator("#total-votes").text_content()
    assert total_votes_text == "Total Votes: 102"

    # Sort by Votes (num_battles) descending -> baseline (120) should be first row
    await page.click("th[data-key='num_battles']")
    first_row_desc = await page.locator("#leaderboard-body tr").first.text_content()
    assert first_row_desc is not None and "baseline" in first_row_desc

    # Click again to sort ascending -> new_comer (4) should be first row
    await page.click("th[data-key='num_battles']")
    first_row_asc = await page.locator("#leaderboard-body tr").first.text_content()
    assert first_row_asc is not None and "new_comer" in first_row_asc

    # Toggle dark mode
    initial_dark = await page.evaluate("document.documentElement.classList.contains('dark')")
    await page.click("#theme-toggle")
    after_dark = await page.evaluate("document.documentElement.classList.contains('dark')")
    assert after_dark is not initial_dark

    await context.close()
