import datetime
import json
import pickle
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import ingest_baseline
import ingest_prompts
import ingest_submissions
import pandas as pd
import postprocess_scores
import prepare_data_for_scoring
import print_submission_stats
import pytest
import scores_json_to_pretty_text_table
import sqlalchemy
import sqlalchemy.ext.asyncio

from ingestion.codabench import Submission
from mwahahavote import database
from mwahahavote.database import Battle, Output, Prompt, System, Vote, VoteString


def _make_vote(prompt_id: str, vote_str: str) -> Vote:
    p = Prompt(id=prompt_id, headline="Some headline")
    battle = Battle(
        output_a=Output(prompt=p, system=System("sys_a"), text="A"),
        output_b=Output(prompt=p, system=System("sys_b"), text="B"),
    )
    return Vote(
        battle=battle,
        session_id="sess_1",
        vote=cast(VoteString, vote_str),
        date=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        is_offensive_a=False,
        is_offensive_b=False,
    )


def test_prepare_data_for_scoring_helpers() -> None:
    assert prepare_data_for_scoring.vote_to_fastchat_format(_make_vote("en_1", "a")) == "model_a"
    assert prepare_data_for_scoring.vote_to_fastchat_format(_make_vote("en_1", "b")) == "model_b"
    assert prepare_data_for_scoring.vote_to_fastchat_format(_make_vote("en_1", "t")) == "tie"

    with pytest.raises(ValueError, match="Unknown vote"):
        prepare_data_for_scoring.vote_to_fastchat_format(_make_vote("en_1", "n"))

    assert prepare_data_for_scoring.vote_to_fastchat_language(_make_vote("en_1", "a")) == "English"
    assert prepare_data_for_scoring.vote_to_fastchat_language(_make_vote("es_1", "a")) == "Spanish"
    assert prepare_data_for_scoring.vote_to_fastchat_language(_make_vote("zh_1", "a")) == "Chinese"

    with patch.object(Prompt, "language", new_callable=lambda: property(lambda _self: "fr")):
        with pytest.raises(ValueError, match="Unknown language"):
            prepare_data_for_scoring.vote_to_fastchat_language(_make_vote("en_1", "a"))


async def test_prepare_data_for_scoring_main(
    seeded_db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "scoring").mkdir()
    monkeypatch.setattr(prepare_data_for_scoring, "MIN_VOTES_PER_SYSTEM", 1)

    await database.add_vote(seeded_db_engine, "prolific-id-1-1", "en_1", "sys_a", "sys_b", "a", False, False)
    await database.add_vote(seeded_db_engine, "prolific-id-2-1", "es_1", "sys_b", "sys_a", "b", False, False)
    await database.add_vote(seeded_db_engine, "regular-sess", "zh_1", "sys_a", "sys_b", "t", False, False)

    @asynccontextmanager
    async def fake_create_engine() -> Any:
        yield seeded_db_engine

    with patch("mwahahavote.database.create_engine", fake_create_engine):
        await prepare_data_for_scoring.async_main(only_prolific_sessions=True, include_ties=True)

    output_file = tmp_path / "scoring" / "votes-a-en.json"
    assert output_file.exists()
    records = json.loads(output_file.read_text(encoding="utf-8"))
    assert len(records) >= 2
    assert {r["winner"] for r in records}.issuperset({"model_a", "model_b"})

    # Test exclusion of session IDs (excluding ties removes synthetic tie votes as well)
    with patch("mwahahavote.database.create_engine", fake_create_engine):
        await prepare_data_for_scoring.async_main(
            only_prolific_sessions=False,
            include_ties=False,
            excluded_session_ids=["prolific-id-1-1", "prolific-id-2-1", "regular-sess"],
        )
    records_excluded = json.loads(output_file.read_text(encoding="utf-8"))
    assert len(records_excluded) == 0


async def test_postprocess_scores(
    seeded_db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "scoring").mkdir()
    scores_dir = tmp_path / "src" / "mwahahavote" / "static" / "scores"
    scores_dir.mkdir(parents=True)

    await database.add_vote(seeded_db_engine, "s1", "en_1", "sys_a", "sys_b", "a", False, False)

    @asynccontextmanager
    async def fake_create_engine() -> Any:
        yield seeded_db_engine

    # Case 1: No pickle file exists -> writes null ratings with num_battles
    with patch("mwahahavote.database.create_engine", fake_create_engine):
        await postprocess_scores.async_main()

    out_json = scores_dir / "a-en.json"
    assert out_json.exists()
    data_no_pkl = json.loads(out_json.read_text(encoding="utf-8"))
    assert len(data_no_pkl) == 3
    assert all(row["rating"] is None for row in data_no_pkl)

    # Case 2: Pickle file exists with leaderboard_table_df for sys_a and sys_b (sys_c missing -> added with NaN)
    df = pd.DataFrame(
        {
            "rating": [1050.0, 950.0],
            "variance": [10.0, 12.0],
            "rating_q975": [1070.0, 970.0],
            "rating_q025": [1030.0, 930.0],
            "num_battles": [20, 20],
            "final_ranking": [1, 2],
        },
        index=["sys_a", "sys_b"],
    )
    pkl_path = tmp_path / "scoring" / "elo_results_a-en.pkl"
    with pkl_path.open("wb") as f:
        pickle.dump({"full": {"leaderboard_table_df": df}}, f)

    with patch("mwahahavote.database.create_engine", fake_create_engine):
        await postprocess_scores.async_main()

    data_with_pkl = json.loads(out_json.read_text(encoding="utf-8"))
    by_sys = {row["system"]: row for row in data_with_pkl}
    assert by_sys["sys_a"]["rating"] == 1050.0
    assert by_sys["sys_b"]["rating"] == 950.0
    assert by_sys["sys_c"]["rating"] is None


async def test_ingest_prompts(
    db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    prompts_dir = tmp_path / "prompts"
    prompts_dir.mkdir()
    (prompts_dir / "a-en.tsv").write_text(
        "id\tword1\tword2\theadline\nen_10\tcat\tdog\t-\nen_11\t-\t-\tFunny headline\n",
        encoding="utf-8",
    )

    df = ingest_prompts.read_prompt_files(str(prompts_dir))
    assert len(df) == 2
    assert list(df["task"]) == ["a-en", "a-en"]
    assert list(df["phase_id"]) == [15785, 15785]

    @asynccontextmanager
    async def fake_create_engine() -> Any:
        yield db_engine

    with patch("mwahahavote.database.create_engine", fake_create_engine):
        await ingest_prompts.async_main()

    async with db_engine.connect() as conn:
        count = (await conn.execute(sqlalchemy.sql.text("SELECT COUNT(*) FROM prompts"))).one()[0]
        assert count == 2

    # Empty prompts dir raises ValueError
    empty_prompts = tmp_path / "empty_prompts"
    empty_prompts.mkdir()
    with pytest.raises(ValueError, match=r"No \.tsv prompt files found"):
        ingest_prompts.read_prompt_files(str(empty_prompts))

    # _extract_task_from_path handles various path formats
    assert ingest_prompts._extract_task_from_path("task-a-en.tsv") == "a-en"
    assert ingest_prompts._extract_task_from_path("a-en.tsv") == "a-en"
    assert ingest_prompts._extract_task_from_path("prompts_a-es.tsv") == "a-es"
    assert ingest_prompts._extract_task_from_path("unrelated.tsv") == "a-en"


async def test_ingest_submissions(
    db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    # Test available_tasks_in_file with a zip containing task-a-en.tsv
    zip_path = tmp_path / "test_sub.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("task-a-en.tsv", "id\ttext\nen_1\tJoke\n")
    assert list(ingest_submissions.available_tasks_in_file(str(zip_path))) == ["a-en"]

    # Seed reference prompt and one already-ingested system
    async with db_engine.begin() as conn:
        await conn.execute(
            sqlalchemy.sql.text(
                "INSERT INTO prompts (prompt_id, word1, word2, headline, url, prompt, task, phase_id)"
                " VALUES ('en_1', NULL, NULL, 'H1', NULL, NULL, 'a-en', 15785)"
            )
        )
        await conn.execute(sqlalchemy.sql.text("INSERT INTO systems (system_id) VALUES ('existing_user')"))

    subs_dir = tmp_path / "submissions"
    subs_dir.mkdir()
    # 1. Already ingested system (skipped)
    (subs_dir / "existing_user.tsv").write_text("id\ttext\nen_1\tJoke\n", encoding="utf-8")
    # 2. Valid new submission (succeeds)
    (subs_dir / "new_user.tsv").write_text("id\ttext\nen_1\tNew joke\n", encoding="utf-8")
    # 3. Invalid submission with wrong prompt ID (fails and logs exception)
    (subs_dir / "bad_user.tsv").write_text("id\ttext\nwrong_id\tBad joke\n", encoding="utf-8")

    @asynccontextmanager
    async def fake_create_engine() -> Any:
        yield db_engine

    with (
        patch("mwahahavote.database.create_engine", fake_create_engine),
        patch("ingest_submissions._collect_codabench_submissions", return_value=[]),
    ):
        await ingest_submissions.async_main()

    async with db_engine.connect() as conn:
        systems = {row[0] for row in await conn.execute(sqlalchemy.sql.text("SELECT system_id FROM systems"))}
        assert "new_user" in systems


async def test_ingest_baseline(
    db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
) -> None:
    @asynccontextmanager
    async def fake_create_engine() -> Any:
        yield db_engine

    with (
        patch("mwahahavote.database.create_engine", fake_create_engine),
        patch("ingest_baseline.ingest_submission", AsyncMock(return_value=1)) as mock_ingest,
    ):
        await ingest_baseline.async_main()
        mock_ingest.assert_awaited_once()


def test_script_entrypoints(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.argv", ["script.py", "--competition", "haha"])
    for mod in (prepare_data_for_scoring, postprocess_scores, ingest_prompts, ingest_submissions, ingest_baseline):
        with patch.object(mod, "async_main", AsyncMock()) as mock_async_main:
            mod.main()
            mock_async_main.assert_awaited_once()
            assert mock_async_main.call_args.kwargs["competition"].id == "haha"


def test_print_submission_stats_and_pretty_table(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    dt = datetime.datetime(2026, 1, 15, tzinfo=datetime.UTC)
    sample_sub = Submission(id=1, user="alice", date=dt, tasks=["t3"], tests_passed=[True], is_deleted=False)
    with patch("print_submission_stats.list_submissions", return_value=[sample_sub]):
        print_submission_stats.main()
    out = capsys.readouterr().out
    assert "Obtaining the list of all submissions so far" in out
    assert "alice" in out

    scores_file = tmp_path / "scores.json"
    scores_file.write_text(json.dumps([{"system": "sys_a", "rating": 1050}]), encoding="utf-8")
    monkeypatch.setattr("sys.argv", ["scores_json_to_pretty_text_table.py", str(scores_file)])
    scores_json_to_pretty_text_table.main()
    table_out = capsys.readouterr().out
    assert "sys_a" in table_out
    assert "1050" in table_out
