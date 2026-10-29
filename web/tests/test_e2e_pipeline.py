import dataclasses
import json
import pickle
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import ingest_prompts
import ingest_submissions
import numpy as np
import pandas as pd
import postprocess_scores
import prepare_data_for_scoring
import pytest
import scores_json_to_pretty_text_table
import sqlalchemy
import sqlalchemy.ext.asyncio
from fastchat.serve.monitor.elo_analysis import report_elo_analysis_results
from httpx import ASGITransport, AsyncClient

import mwahahavote.__main__ as main_mod
from ingestion import codabench, submission
from mwahahavote import database
from mwahahavote.__main__ import app

PROMPT_ROWS = [
    ("en_1", "coffee", "monday", "-", "-", "-"),
    ("en_2", "cat", "keyboard", "-", "-", "-"),
    ("en_3", "meeting", "email", "-", "-", "-"),
    ("en_4", "deadline", "sleep", "-", "-", "-"),
    ("es_1", "-", "-", "Científicos descubren que el lunes dura 48 horas", "-", "-"),
    ("es_2", "-", "-", "El colectivo llega justo cuando prendés otra cosa", "-", "-"),
    ("es_3", "-", "-", "Reunión de cinco minutos cumple su tercera hora", "-", "-"),
    ("es_4", "-", "-", "Pronóstico anuncia lluvia justo después de lavar el auto", "-", "-"),
    ("zh_1", "-", "-", "-", "https://example.com/1.gif", "Write a caption"),
    ("zh_2", "-", "-", "-", "https://example.com/2.gif", "-"),
    ("zh_3", "-", "-", "-", "https://example.com/3.gif", "Funny caption"),
    ("zh_4", "-", "-", "-", "https://example.com/4.gif", "-"),
]
PROMPT_IDS = [r[0] for r in PROMPT_ROWS]
RANKED_ORDER = ["team_alpha", "baseline", "team_beta", "team_gamma"]


def _mock_create_engine(db_engine: sqlalchemy.ext.asyncio.AsyncEngine):  # noqa: ANN202
    @asynccontextmanager
    async def _ctx() -> AsyncGenerator[sqlalchemy.ext.asyncio.AsyncEngine]:
        yield db_engine

    return _ctx


def _write_prompts_and_submissions(tmp_path: Path) -> Path:
    prompts_dir = tmp_path / "prompts"
    prompts_dir.mkdir()
    prompt_tsv_lines = ["id\tword1\tword2\theadline\turl\tprompt"] + [
        f"{pid}\t{w1}\t{w2}\t{hl}\t{url}\t{pr}" for pid, w1, w2, hl, url, pr in PROMPT_ROWS
    ]
    (prompts_dir / "task-t3.tsv").write_text("\n".join(prompt_tsv_lines) + "\n", encoding="utf-8")

    submissions_dir = tmp_path / "submissions"
    submissions_dir.mkdir()
    systems_spec = {
        "team_alpha": [f"Alpha hilarious punchline for {pid}" for pid in PROMPT_IDS],
        "baseline": [f"Baseline solid joke for {pid}" for pid in PROMPT_IDS],
        "team_beta": [f"Beta decent joke for {pid}" for pid in PROMPT_IDS],
        "team_gamma": [f"Gamma weak joke for {pid}" for pid in PROMPT_IDS],
        "team_low_votes": [f"Low-vote newcomer joke for {pid}" for pid in PROMPT_IDS],
    }
    # Make team_alpha and team_beta produce the exact same text on es_1 to test automatic same-text tie synthesis
    es_1_idx = PROMPT_IDS.index("es_1")
    systems_spec["team_alpha"][es_1_idx] = "Identical shared joke on es_1"
    systems_spec["team_beta"][es_1_idx] = "Identical shared joke on es_1"
    # Give team_gamma a blank output on zh_4 to test null-text fallback ("-")
    zh_4_idx = PROMPT_IDS.index("zh_4")
    systems_spec["team_gamma"][zh_4_idx] = ""

    for sys_name, jokes in systems_spec.items():
        lines = ["id\ttext"] + [f"{pid}\t{joke}" for pid, joke in zip(PROMPT_IDS, jokes, strict=True)]
        (submissions_dir / f"{sys_name}.tsv").write_text("\n".join(lines) + "\n", encoding="utf-8")

    # Also add an invalid submission file (missing prompt IDs) to verify graceful failure reporting
    (submissions_dir / "team_broken.tsv").write_text("id\ttext\nen_1\tOnly one prompt\n", encoding="utf-8")
    return submissions_dir


async def _seed_tournament_votes(db_engine: sqlalchemy.ext.asyncio.AsyncEngine) -> None:
    pairs = [
        ("team_alpha", "baseline"),
        ("team_alpha", "team_beta"),
        ("team_alpha", "team_gamma"),
        ("baseline", "team_beta"),
        ("baseline", "team_gamma"),
        ("team_beta", "team_gamma"),
    ]
    vote_idx = 0
    for stronger, weaker in pairs:
        for round_num in range(10):
            pid = PROMPT_IDS[(vote_idx + round_num) % len(PROMPT_IDS)]
            if {stronger, weaker} == {"team_alpha", "team_beta"} and pid == "es_1":
                pid = "en_1"
            session_id = f"prolific-id-{(vote_idx % 5) + 1}"
            if round_num % 2 == 0:
                sys_a, sys_b = stronger, weaker
                outcome = "b" if round_num == 8 else ("t" if round_num == 9 else "a")
            else:
                sys_a, sys_b = weaker, stronger
                outcome = "a" if round_num == 7 else ("t" if round_num == 9 else "b")
            await database.add_vote(db_engine, session_id, pid, sys_a, sys_b, outcome, False, False)
            vote_idx += 1

    # Add 3 valid non-skip votes for team_low_votes (< MIN_VOTES_PER_SYSTEM = 20),
    # plus 2 skip ('n') votes, plus 4 votes from an excluded spammer session.
    await database.add_vote(db_engine, "prolific-id-1", "en_1", "team_low_votes", "baseline", "b", False, False)
    await database.add_vote(db_engine, "prolific-id-2", "en_2", "baseline", "team_low_votes", "a", False, False)
    await database.add_vote(db_engine, "regular-user-1", "en_3", "team_low_votes", "team_gamma", "a", False, False)
    await database.add_vote(db_engine, "regular-user-1", "en_4", "team_low_votes", "team_alpha", "n", False, False)
    await database.add_vote(db_engine, "regular-user-2", "es_2", "team_beta", "team_low_votes", "n", False, False)
    for idx, pid in enumerate(["en_1", "en_2", "en_3", "en_4"]):
        await database.add_vote(
            db_engine,
            "spammer-session-99",
            pid,
            "team_low_votes" if idx % 2 == 0 else "team_gamma",
            "team_gamma" if idx % 2 == 0 else "team_low_votes",
            "a",
            False,
            False,
        )


async def test_full_mwhaha_competition_e2e_pipeline(
    db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Simulates the complete MWAHAHA/HAHA competition lifecycle end-to-end:
    1. Ingest prompts from TSV files (`ingest_prompts.py`)
    2. Ingest baseline and participant submissions from TSV files (`ingest_submissions.py`)
    3. Serve battles via `/battles` and record human annotations via `/vote` (plus batch pairwise votes)
    4. Export votes for scoring (`prepare_data_for_scoring.py`) with session exclusions and same-text tie synthesis
    5. Compute Bradley-Terry Elo ratings + bootstrap confidence intervals (`fastchat.serve.monitor.elo_analysis`)
    6. Postprocess leaderboard JSON (`postprocess_scores.py`) and pretty-print (`scores_json_to_pretty_text_table.py`)
    7. Serve the final leaderboard JSON and `/votes.csv` through the FastAPI app
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(database, "create_engine", _mock_create_engine(db_engine))
    monkeypatch.setattr(main_mod, "IS_LOCAL_DEVELOPMENT", True)

    async def wrapped_app(scope: Any, receive: Any, send: Any) -> None:
        scope.setdefault("state", {})["database_engine"] = db_engine
        await app(scope, receive, send)

    submissions_dir = _write_prompts_and_submissions(tmp_path)

    await ingest_prompts.async_main()
    async with db_engine.connect() as conn:
        prompt_count = (await conn.execute(sqlalchemy.sql.text("SELECT COUNT(*) FROM prompts"))).scalar_one()
        assert prompt_count == 12

    await ingest_submissions.async_main()
    first_ingest_out = capsys.readouterr().out
    assert "5/6 new submissions ingested successfully" in first_ingest_out
    assert "1/6 new submissions failed to be ingested" in first_ingest_out
    (submissions_dir / "team_broken.tsv").unlink()

    # Re-running ingest_submissions should skip the 5 already-ingested submissions
    await ingest_submissions.async_main()
    second_ingest_out = capsys.readouterr().out
    assert "5/5 submissions were skipped because they were already ingested before." in second_ingest_out

    # Exercise the live FastAPI `/battles` and `/vote` endpoints as an annotator session
    async with AsyncClient(transport=ASGITransport(app=wrapped_app), base_url="http://test") as client:
        battles_resp = await client.get("/battles", params={"task": "t3", "batch_size": 4})
        assert battles_resp.status_code == 200
        fetched_battles = battles_resp.json()
        assert len(fetched_battles) == 4
        for b in fetched_battles:
            assert b["output_a"].strip() != b["output_b"].strip()
            vote_resp = await client.post(
                "/vote",
                data={"token": b["token"], "vote": "a", "is_offensive_a": "false", "is_offensive_b": "false"},
            )
            assert vote_resp.status_code == 204

    async with db_engine.begin() as conn:
        await conn.execute(sqlalchemy.sql.text("DELETE FROM votes"))
    await _seed_tournament_votes(db_engine)

    # Run `prepare_data_for_scoring.py` with `spammer-session-99` excluded
    monkeypatch.setattr(prepare_data_for_scoring, "EXCLUDED_SESSION_IDS", {"spammer-session-99"})
    await prepare_data_for_scoring.async_main()

    votes_json_path = tmp_path / "scoring" / "votes-t3.json"
    assert votes_json_path.is_file()
    exported_battles = json.loads(votes_json_path.read_text(encoding="utf-8"))

    # 60 human battles between the 4 main systems + 1 synthetic same-text tie on es_1 = 61
    assert len(exported_battles) == 61
    judges = {b["judge"] for b in exported_battles}
    assert "spammer-session-99" not in judges
    assert "<placeholder>" in judges

    models_in_export = {b["model_a"] for b in exported_battles} | {b["model_b"] for b in exported_battles}
    assert models_in_export == {"team_alpha", "baseline", "team_beta", "team_gamma"}
    assert {b["language"] for b in exported_battles} == {"English", "Spanish", "Chinese"}

    # Run FastChat Bradley-Terry Elo analysis with bootstrap confidence intervals
    np.random.seed(42)
    battles_df = pd.read_json(votes_json_path)
    elo_results = {"full": report_elo_analysis_results(battles_df, rating_system="bt", num_bootstrap=30, num_cpu=1)}
    pkl_path = tmp_path / "scoring" / "elo_results_t3.pkl"
    with open(pkl_path, "wb") as f:
        pickle.dump(elo_results, f)

    # Run `postprocess_scores.py` to generate final leaderboard JSON
    await postprocess_scores.async_main()

    scores_json_path = tmp_path / "src" / "mwahahavote" / "static" / "scores" / "t3.json"
    assert scores_json_path.is_file()
    leaderboard = json.loads(scores_json_path.read_text(encoding="utf-8"))

    assert len(leaderboard) == 5
    ranked_entries = [row for row in leaderboard if row["rating"] is not None]
    unranked_entries = [row for row in leaderboard if row["rating"] is None]
    assert [row["system"] for row in ranked_entries] == RANKED_ORDER
    for row in ranked_entries:
        assert row["rating_q975"] >= row["rating"] >= row["rating_q025"]
        assert row["variance"] >= 0
        assert row["num_battles"] >= 30
        assert row["final_ranking"] >= 1

    low_row = unranked_entries[0]
    assert low_row["system"] == "team_low_votes"
    assert low_row["rating"] is None
    assert low_row["num_battles"] == 7

    # Verify `scores_json_to_pretty_text_table.py` formats the leaderboard table cleanly
    capsys.readouterr()
    monkeypatch.setattr("sys.argv", ["scores_json_to_pretty_text_table.py", str(scores_json_path)])
    scores_json_to_pretty_text_table.main()
    table_out = capsys.readouterr().out
    for sys_name in (*RANKED_ORDER, "team_low_votes"):
        assert sys_name in table_out

    # Verify `/votes.csv` and `/stats` reflect the tournament data
    monkeypatch.undo()
    monkeypatch.setattr(main_mod, "IS_LOCAL_DEVELOPMENT", True)
    async with AsyncClient(transport=ASGITransport(app=wrapped_app), base_url="http://test") as client:
        csv_resp = await client.get("/votes.csv")
        assert csv_resp.status_code == 200
        assert "team_alpha" in csv_resp.text
        stats_resp = await client.get("/stats")
        assert stats_resp.status_code == 200
        assert "Cantidad total de votos: 69" in stats_resp.text
        assert "Cantidad de votos: 67" in stats_resp.text


async def test_mwhaha_codabench_filtering_and_prolific_only_scoring_e2e(
    db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Simulates CodaBench API submission listing, filtering (latest valid non-deleted submission per user),
    and scoring with `only_prolific_sessions=True` and `include_ties=False`.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(database, "create_engine", _mock_create_engine(db_engine))
    _write_prompts_and_submissions(tmp_path)
    await ingest_prompts.async_main()

    codabench_payload: list[dict[str, Any]] = [
        # Alice's earlier valid submission (id=100) -> should be superseded by id=105
        {
            "id": 100,
            "parent": None,
            "children": [],
            "owner": "alice",
            "created_when": "2026-01-10T10:00:00Z",
            "is_soft_deleted": False,
            "task": {"id": 21358},
            "scores": [{"score": "1.0"}],
        },
        # Alice's latest valid submission (id=105) -> kept
        {
            "id": 105,
            "parent": None,
            "children": [],
            "owner": "alice",
            "created_when": "2026-01-12T10:00:00Z",
            "is_soft_deleted": False,
            "task": {"id": 21358},
            "scores": [{"score": "1.0"}],
        },
        # Alice's even later soft-deleted submission (id=106) -> ignored because is_soft_deleted=True
        {
            "id": 106,
            "parent": None,
            "children": [],
            "owner": "alice",
            "created_when": "2026-01-13T10:00:00Z",
            "is_soft_deleted": True,
            "task": {"id": 21358},
            "scores": [{"score": "1.0"}],
        },
        # Bob's submission that failed the validation test (score 0.0) -> ignored
        {
            "id": 110,
            "parent": None,
            "children": [],
            "owner": "bob",
            "created_when": "2026-01-11T10:00:00Z",
            "is_soft_deleted": False,
            "task": {"id": 21358},
            "scores": [{"score": "0.0"}],
        },
        # Dave's parent + child submission that passed the test -> kept
        {
            "id": 200,
            "parent": None,
            "children": [201],
            "owner": "dave",
            "created_when": "2026-01-14T10:00:00Z",
            "is_soft_deleted": False,
        },
        {
            "id": 201,
            "parent": 200,
            "children": [],
            "owner": "dave",
            "created_when": "2026-01-14T10:00:00Z",
            "is_soft_deleted": False,
            "task": {"id": 21358},
            "scores": [{"score": "1.0"}],
        },
    ]

    mock_resp = MagicMock()
    mock_resp.json.return_value = codabench_payload
    with (
        patch("ingestion.codabench.is_session_id_valid", return_value=True),
        patch("httpx2.get", return_value=mock_resp),
    ):
        all_submissions = sorted(codabench.list_submissions(session_id="sid"))

    valid_submissions = [
        dataclasses.replace(
            sub,
            tasks=[t for t, passed in zip(sub.tasks, sub.tests_passed, strict=True) if passed],
            tests_passed=[True] * sum(sub.tests_passed),
        )
        for sub in all_submissions
        if not sub.is_deleted and any(sub.tests_passed)
    ]

    # Deduplicate keeping only the latest valid submission per user
    latest_by_user: list[codabench.Submission] = []
    seen_users: set[str] = set()
    for sub in reversed(valid_submissions):
        if sub.user not in seen_users:
            seen_users.add(sub.user)
            latest_by_user.append(sub)

    assert {(s.user, s.id) for s in latest_by_user} == {("alice", 105), ("dave", 200)}

    # Point alice and dave to valid TSV files on disk and ingest them
    for sub in latest_by_user:
        tsv_path = tmp_path / f"{sub.user}_{sub.id}.tsv"
        lines = ["id\ttext"] + [f"{pid}\t{sub.user} joke for {pid}" for pid in PROMPT_IDS]
        tsv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        await submission.ingest_submission(
            db_engine, phase_id=1, submission=dataclasses.replace(sub, path_or_url=str(tsv_path))
        )

    # Record 22 prolific votes (20 wins/losses + 2 ties) and 5 non-prolific votes
    for i in range(22):
        pid = PROMPT_IDS[i % len(PROMPT_IDS)]
        sys_a, sys_b = ("alice", "dave") if i % 2 == 0 else ("dave", "alice")
        if i >= 20:
            vote_val = "t"
        else:
            vote_val = "a" if sys_a == "alice" else "b"
        await database.add_vote(db_engine, f"prolific-id-{i}", pid, sys_a, sys_b, vote_val, False, False)

    for i in range(5):
        pid = PROMPT_IDS[i % len(PROMPT_IDS)]
        await database.add_vote(db_engine, f"public-anon-{i}", pid, "alice", "dave", "b", False, False)

    # Export with `only_prolific_sessions=True, include_ties=False`
    monkeypatch.setattr(prepare_data_for_scoring, "EXCLUDED_SESSION_IDS", set())
    await prepare_data_for_scoring.async_main(only_prolific_sessions=True, include_ties=False)

    exported = json.loads((tmp_path / "scoring" / "votes-t3.json").read_text(encoding="utf-8"))
    # Only the 20 non-tie prolific battles should be exported (public-anon and ties excluded)
    assert len(exported) == 20
    assert all(b["judge"].startswith("prolific-id-") for b in exported)
    assert all(b["winner"] in {"model_a", "model_b"} for b in exported)
