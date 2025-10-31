import itertools
import os
import random
import string
from datetime import timedelta
from typing import Any, cast

import sentry_sdk
from flask import Flask, Response, jsonify, render_template, request, send_from_directory
from sentry_sdk.integrations.flask import FlaskIntegration

from mwahahavote import database
from mwahahavote.database import TASK_CHOICES, Battle, Task

REQUEST_BATTLE_BATCH_SIZE = 3

SESSION_ID_MAX_AGE = int(timedelta(weeks=1000).total_seconds())


def _create_app() -> Flask:
    app_ = Flask(__name__)

    app_.secret_key = os.environ["FLASK_SECRET_KEY"]
    app_.config["SESSION_TYPE"] = "filesystem"

    return app_


sentry_sdk.init(integrations=[FlaskIntegration()], traces_sample_rate=1.0)

app = _create_app()


def _generate_id() -> str:  # From https://stackoverflow.com/a/2257449/1165181
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=100))


def _get_session_id() -> str:
    return request.cookies.get("id") or _generate_id()


def _simplify_battle_object(battle: Battle) -> dict[str, Any]:
    """Removes redundant fields and simplifies the battle representation for JSON serialization."""
    return {
        "prompt_image_url": battle.prompt.url,
        "prompt": battle.prompt.verbalized,
        "system_a": battle.output_a.system.id,
        "output_a": battle.output_a.text,
        "system_b": battle.output_b.system.id,
        "output_b": battle.output_b.text,
    }


@app.after_request
def add_header(response: Response) -> Response:
    response.cache_control.max_age = 0
    response.cache_control.no_cache = True

    response.set_cookie("id", _get_session_id(), max_age=SESSION_ID_MAX_AGE)

    return response


@app.route("/battles")
def battles_route() -> Response:
    session_id = _get_session_id()

    task = request.args.get("task", "a-en")
    if task not in TASK_CHOICES:
        task = "a-en"
    task = cast(Task, task)

    battles = [
        _simplify_battle_object(battle)
        for battle in database.random_least_voted_unseen_battles(session_id, task, REQUEST_BATTLE_BATCH_SIZE)
    ]

    if len(battles) < REQUEST_BATTLE_BATCH_SIZE:
        battles.extend(
            _simplify_battle_object(battle)
            for battle in database.random_battles(task, REQUEST_BATTLE_BATCH_SIZE - len(battles))
        )

    return jsonify(battles)


@app.route("/vote", methods=["POST"])
def vote_and_get_new_tweet_route() -> Response:
    session_id = _get_session_id()

    task = request.form.get("task", "a-en")
    if task not in TASK_CHOICES:
        task = "a-en"
    task = cast(Task, task)

    if "tweet_id" in request.form and "vote" in request.form and "is_offensive" in request.form:
        is_offensive = request.form["is_offensive"].lower() == "true"
        database.add_vote(session_id, request.form["tweet_id"], request.form["vote"], is_offensive)

    ignore_tweet_ids = request.form.getlist("ignored_output_ids[]")

    tweets = itertools.chain(
        database.random_least_voted_unseen_battles(session_id, task, 1, ignore_tweet_ids),
        database.random_battles(task, 1),
    )

    tweet = next(iter(tweets), {})

    return jsonify(tweet)


@app.route("/session-vote-count")
def session_vote_count_route() -> Response:
    return jsonify(database.session_vote_count_with_skips(_get_session_id()))


@app.route("/vote-count")
def vote_count_route() -> Response:
    return jsonify(database.vote_count_without_skips())


@app.route("/stats")
def stats_route() -> str:
    stats = database.stats()

    stats["votes-not-consider-test"] = stats["votes"] - sum(stats["test-tweets-vote-count"])

    stats["test-tweets-vote-count"] = ", ".join(str(c) for c in stats["test-tweets-vote-count"])

    stats["histogram"] = [["Cantidad de votos", "Cantidad de tweets"]] + [
        [str(a), b] for a, b in stats["histogram"].items()
    ]

    stats["votes-per-category"]["No humor"] = stats["votes-per-category"]["x"]
    del stats["votes-per-category"]["x"]
    stats["votes-per-category"]["Saltear"] = stats["votes-per-category"]["n"]
    del stats["votes-per-category"]["n"]
    stats["votes-per-category"] = [["Voto", "Cantidad de tweets"]] + [
        [str(a), b] for a, b in stats["votes-per-category"].items()
    ]

    return render_template("stats.html", stats=stats)


@app.route("/", defaults={"path": "index.html"})
@app.route("/<path:path>")
def static_files_route(path: str) -> Response:
    return send_from_directory("static", path)
