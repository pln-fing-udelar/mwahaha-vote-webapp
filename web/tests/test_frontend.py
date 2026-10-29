import asyncio
import json
import socket
from collections.abc import AsyncGenerator
from typing import Any

import pytest
import pytest_asyncio
import sqlalchemy.ext.asyncio
import uvicorn
from playwright.async_api import Browser, Page, Route, async_playwright

import mwahahavote.__main__ as main_mod
from mwahahavote import database
from mwahahavote.__main__ import app

pytestmark = pytest.mark.asyncio(loop_scope="module")


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest_asyncio.fixture(scope="module", loop_scope="module")
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
    await page.route("https://www.gstatic.com/**", _empty_js)


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
    await page.check("#is-offensive-right")
    async with page.expect_response(lambda resp: resp.url.endswith("/vote") and resp.status == 204):
        await page.click("#vote-right")
    assert len(vote_requests) == 4
    assert "is_offensive_b=true" in vote_requests[3]
    assert await page.is_checked("#is-offensive-right") is False

    await context.close()


async def test_voting_page_image_newlines_and_emoji_rendering(browser: Browser, live_server_url: str) -> None:
    context = await browser.new_context()
    page = await context.new_page()
    await _stub_external_trackers(page)

    # Stub image requests so headless browser does not hit external host
    await page.route(
        "https://example.com/**",
        lambda route: route.fulfill(status=200, content_type="image/gif", body=b"GIF89a"),
    )
    await page.route(
        "https://raw.githubusercontent.com/**",
        lambda route: route.fulfill(status=200, content_type="image/png", body=b""),
    )

    custom_battles = [
        {
            "token": "tok_img_1",
            "prompt": "Caption line 1\nCaption line 2 😄",
            "prompt_image_url": "https://example.com/funny.gif",
            "output_a": "Joke A line 1\nJoke A line 2 😂",
            "output_b": "Joke B single line 🎉",
        }
    ]
    await page.route(
        "**/battles*",
        lambda route: route.fulfill(status=200, content_type="application/json", body=json.dumps(custom_battles)),
    )

    await page.goto(live_server_url)
    await page.wait_for_selector("#prompt-image")

    assert await page.locator("#prompt-image").get_attribute("src") == "https://example.com/funny.gif"
    prompt_html = await page.locator("#prompt-text").inner_html()
    assert "Caption line 1<br>Caption line 2" in prompt_html
    assert "emoji" in prompt_html

    output_a_html = await page.locator("#output-a-text").inner_html()
    assert "Joke A line 1<br>Joke A line 2" in output_a_html
    assert "emoji" in output_a_html

    output_b_html = await page.locator("#output-b-text").inner_html()
    assert "emoji" in output_b_html

    await context.close()


async def test_voting_page_circular_buffer_wraparound(browser: Browser, live_server_url: str) -> None:
    context = await browser.new_context()
    page = await context.new_page()
    await _stub_external_trackers(page)

    call_count = 0

    async def handle_battles(route: Route) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            body = [
                {
                    "token": f"init_{i}",
                    "prompt": f"Prompt {i}",
                    "prompt_image_url": None,
                    "output_a": f"InitA_{i}",
                    "output_b": f"InitB_{i}",
                }
                for i in range(4)
            ]
        else:
            body = [
                {
                    "token": f"repl_{call_count}_{i}",
                    "prompt": f"Replaced Prompt {call_count}_{i}",
                    "prompt_image_url": None,
                    "output_a": f"ReplA_{call_count}_{i}",
                    "output_b": f"ReplB_{call_count}_{i}",
                }
                for i in range(2)
            ]
        await route.fulfill(status=200, content_type="application/json", body=json.dumps(body))

    await page.route("**/battles*", handle_battles)
    await page.route("**/vote", lambda route: route.fulfill(status=204, body=""))

    await page.goto(live_server_url)
    await page.wait_for_function("() => $('#output-a-text').text().includes('InitA_0')")

    # Vote 4 times to wrap index around from 0 -> 1 -> 2 -> 3 -> 0
    for i in range(4):
        await page.evaluate(f"onTurnstileSuccess('tok-{i}')")
        async with page.expect_response(lambda resp: resp.url.endswith("/vote")):
            await page.click("#vote-left")
        await page.wait_for_function(f"() => index === {(i + 1) % 4}")

    # At index 0 after 4 votes, battle 0 should have been replaced by ReplA_2_0
    await page.wait_for_function("() => $('#output-a-text').text().includes('ReplA_2_0')")
    assert await page.evaluate("nextReplaceIndex") == 0

    await context.close()


async def test_voting_page_vote_post_failure(browser: Browser, live_server_url: str) -> None:
    context = await browser.new_context()
    page = await context.new_page()
    await _stub_external_trackers(page)

    await page.route("**/vote", lambda route: route.fulfill(status=500, body="Internal Server Error"))

    await page.goto(live_server_url)
    await page.wait_for_function("() => battles.length > 0 && $('#output-a-text').text().trim().length > 0")

    await page.evaluate("onTurnstileSuccess('tok-fail')")
    async with page.expect_response(lambda resp: resp.url.endswith("/vote") and resp.status == 500):
        await page.click("#vote-left")

    # Wait for the .fail() callback to show the error toast and reset turnstileToken
    await page.wait_for_function(
        "() => $('.md-toast').text().includes('Sorry, an error occurred!') && turnstileToken === null"
    )
    await context.close()


async def test_voting_page_help_modal(browser: Browser, live_server_url: str) -> None:
    context = await browser.new_context()
    page = await context.new_page()
    await _stub_external_trackers(page)

    await page.goto(live_server_url)
    await page.wait_for_function("() => battles.length > 0")

    await page.click("#help-link")
    await page.wait_for_function("() => $('#helpModal').hasClass('in')")
    modal_text = await page.locator("#helpModal").text_content()
    assert modal_text is not None and "Frequently Asked Questions (FAQ)" in modal_text

    await page.click("#helpModal .modal-footer button[data-dismiss='modal']")
    await page.wait_for_function("() => !$('#helpModal').hasClass('in')")
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

    # Skipping ("n") should NOT increment prolificVoteCount
    await page.evaluate("onTurnstileSuccess('tok-skip'); vote('n');")
    assert await page.evaluate("prolificVoteCount") == 0
    assert "Progress: 0/100" in (await page.locator("#prolific-vote-count").text_content() or "")

    # Cast a non-skip vote at count 99 to trigger the finished modal at 100
    await page.evaluate("prolificVoteCount = 99; onTurnstileSuccess('tok'); vote('a');")
    await page.wait_for_function("() => $('#prolific-finished').hasClass('in')")

    # Test Spanish and Chinese modal translations
    await page.evaluate("translateModal('es')")
    assert await page.locator("[data-i18n='finished.title']").text_content() == "¡Terminaste de anotar!"
    await page.evaluate("translateModal('zh')")
    assert await page.locator("[data-i18n='finished.title']").text_content() == "您已完成标注！"  # noqa: RUF001

    await context.close()


async def test_prolific_returning_user_and_modulo_count(browser: Browser, live_server_url: str) -> None:
    context = await browser.new_context()
    page = await context.new_page()
    await _stub_external_trackers(page)

    # Pre-populate localStorage so consent is already marked "done"
    await page.add_init_script("localStorage.setItem('consent-prolific-id-retuser', 'done');")
    # Stub /session-vote-count to return 105 (simulating a user on their second Prolific batch)
    await page.route(
        "**/session-vote-count",
        lambda route: route.fulfill(status=200, content_type="application/json", body="105"),
    )

    await page.goto(f"{live_server_url}/?PROLIFIC_PID=retuser&SESSION_ID=sess123")
    await page.wait_for_function("() => $('#prolific-vote-count').text().includes('Progress: 5/100')")

    # Consent modal should not be shown for returning user
    assert await page.evaluate("$('#prolific-consent').hasClass('in')") is False
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
            "system": "silver_joker",
            "rating": 1060.0,
            "variance": 12.0,
            "rating_q975": 1080.0,
            "rating_q025": 1040.0,
            "num_battles": 60,
            "final_ranking": 2,
        },
        {
            "system": "bronze_pun",
            "rating": 1020.0,
            "variance": 11.0,
            "rating_q975": 1040.0,
            "rating_q025": 1000.0,
            "num_battles": 50,
            "final_ranking": 3,
        },
        {
            "system": "baseline",
            "rating": 1000.0,
            "variance": 10.0,
            "rating_q975": 1020.0,
            "rating_q025": 980.0,
            "num_battles": 120,
            "final_ranking": 4,
        },
        {
            "system": "fourth_place",
            "rating": 960.0,
            "variance": 10.0,
            "rating_q975": 980.0,
            "rating_q025": 940.0,
            "num_battles": 40,
            "final_ranking": 5,
        },
        {
            "system": "new_comer",
            "rating": None,
            "variance": None,
            "rating_q975": None,
            "rating_q025": None,
            "num_battles": 10,
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
    for name in ("funny_bot", "silver_joker", "bronze_pun", "baseline", "fourth_place", "new_comer", "TBD"):
        assert name in rows_text

    # Verify row styling classes for rank 1, rank 2, rank 3, baseline, and default rank
    row_classes = await page.locator("#leaderboard-body tr").evaluate_all("rows => rows.map(r => r.className)")
    assert any("bg-amber-100" in c for c in row_classes)  # Rank 1
    assert any("bg-gray-300" in c for c in row_classes)  # Rank 2
    assert any("bg-orange-200" in c for c in row_classes)  # Rank 3
    assert any("bg-purple-100" in c for c in row_classes)  # Baseline
    assert any("hover:bg-blue-50/30" in c for c in row_classes)  # Rank >= 4 non-baseline

    # Verify total votes ((80 + 60 + 50 + 120 + 40 + 10) / 2 = 180) and Last-Modified footer
    total_votes_text = await page.locator("#total-votes").text_content()
    assert total_votes_text == "Total Votes: 180"
    last_updated_text = await page.locator("#last-updated").text_content()
    assert last_updated_text is not None and last_updated_text.startswith("Last updated:")
    assert await page.locator("#last-updated").get_attribute("title") is not None

    # Sort by Votes (num_battles) descending -> baseline (120) should be first row
    await page.click("th[data-key='num_battles']")
    first_row_desc = await page.locator("#leaderboard-body tr").first.text_content()
    assert first_row_desc is not None and "baseline" in first_row_desc

    # Click again to sort ascending -> new_comer (10) should be first row
    await page.click("th[data-key='num_battles']")
    first_row_asc = await page.locator("#leaderboard-body tr").first.text_content()
    assert first_row_asc is not None and "new_comer" in first_row_asc

    # Toggle dark mode twice to exercise both branches
    initial_dark = await page.evaluate("document.documentElement.classList.contains('dark')")
    await page.click("#theme-toggle")
    after_first_toggle = await page.evaluate("document.documentElement.classList.contains('dark')")
    assert after_first_toggle is not initial_dark
    await page.click("#theme-toggle")
    after_second_toggle = await page.evaluate("document.documentElement.classList.contains('dark')")
    assert after_second_toggle is initial_dark

    await context.close()


async def test_leaderboard_empty_404_and_zero_ci_range(browser: Browser, live_server_url: str) -> None:
    context = await browser.new_context()
    page = await context.new_page()
    await _stub_external_trackers(page)

    # First test 404 from scores/t3.json and invalid ?task=invalid_task URL normalization
    await page.route("**/scores/t3.json", lambda route: route.fulfill(status=404, body="Not Found"))

    await page.goto(f"{live_server_url}/leaderboard?task=invalid_task")
    await page.wait_for_selector("text=No data available for this task.")
    assert page.url.endswith("?task=t3")
    assert await page.locator("#total-votes").text_content() == "Total Votes: 0"

    # Now test range === 0 guard when all systems have identical CI bounds
    identical_ci_scores = [
        {
            "system": "equal_sys",
            "rating": 1000.0,
            "variance": 0.0,
            "rating_q975": 1000.0,
            "rating_q025": 1000.0,
            "num_battles": 20,
            "final_ranking": 1,
        }
    ]
    await page.unroute("**/scores/t3.json")
    await page.route(
        "**/scores/t3.json",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(identical_ci_scores),
        ),
    )
    await page.evaluate("loadInitialData()")
    await page.wait_for_selector("text=[1000, 1000]")
    body_html = await page.locator("#leaderboard-body").inner_html()
    assert "NaN" not in body_html

    await context.close()


async def test_stats_page(
    browser: Browser,
    live_server_url: str,
    seeded_db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
) -> None:
    await database.add_vote(seeded_db_engine, "sess_stats", "en_1", "sys_a", "sys_b", "a", False, False)

    context = await browser.new_context()
    page = await context.new_page()
    await _stub_external_trackers(page)

    await page.goto(f"{live_server_url}/stats")
    assert await page.title() == "Estadísticas"
    content = await page.content()
    assert "Cantidad total de votos: 1" in content
    assert "Cantidad de sesiones: 1" in content
    assert await page.locator("#chart-histogram").count() == 1
    assert await page.locator("#chart-votes-per-category").count() == 1

    await context.close()
