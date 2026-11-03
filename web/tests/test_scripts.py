import dataclasses
import datetime
import gzip
import json
import pickle
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import anonymize_dump
import compute_scores
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
from mwahahavote import config, database
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

    # Test exclusion via competition.excluded_session_ids union
    comp_with_excluded = dataclasses.replace(
        config.COMPETITIONS["mwahaha"],
        excluded_session_ids=frozenset({"prolific-id-1-1", "prolific-id-2-1", "regular-sess"}),
    )
    with patch("mwahahavote.database.create_engine", fake_create_engine):
        await prepare_data_for_scoring.async_main(
            only_prolific_sessions=False,
            include_ties=False,
            competition=comp_with_excluded,
            excluded_session_ids=[],
        )
    records_comp_excluded = json.loads(output_file.read_text(encoding="utf-8"))
    assert len(records_comp_excluded) == 0


async def test_prepare_data_from_csv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    csv_file = tmp_path / "subset.csv"
    csv_file.write_text(
        "source_file,source_row,prompt_id,system_id_a,system_id_b,vote,task\n"
        "clean.csv,1,en_1,sys_a,sys_b,a,a-en\n"
        "clean.csv,2,en_2,sys_b,sys_a,t,a-en\n"
        "clean.csv,3,es_1,sys_c,sys_d,b,a-es\n"
        "clean.csv,4,zh_1,sys_e,sys_f,a,a-zh\n"
        "clean.csv,5,other_1,sys_g,sys_h,a,unknown_task\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "out_scoring"

    # With ties and min_votes=1
    tasks = prepare_data_for_scoring.prepare_data_from_csv(
        csv_file, output_dir=out_dir, include_ties=True, min_votes_per_system=1
    )
    assert set(tasks) == {"a-en", "a-es", "a-zh", "unknown_task"}
    en_battles = json.loads((out_dir / "votes-a-en.json").read_text(encoding="utf-8"))
    assert len(en_battles) == 2
    assert en_battles[0]["winner"] == "model_a"
    assert en_battles[0]["language"] == "English"
    assert en_battles[1]["winner"] == "tie"

    es_battles = json.loads((out_dir / "votes-a-es.json").read_text(encoding="utf-8"))
    assert len(es_battles) == 1
    assert es_battles[0]["language"] == "Spanish"

    # Without ties
    prepare_data_for_scoring.prepare_data_from_csv(
        csv_file, output_dir=out_dir, include_ties=False, min_votes_per_system=1
    )
    en_battles_no_ties = json.loads((out_dir / "votes-a-en.json").read_text(encoding="utf-8"))
    assert len(en_battles_no_ties) == 1
    assert en_battles_no_ties[0]["winner"] == "model_a"

    # min_votes filter
    prepare_data_for_scoring.prepare_data_from_csv(
        csv_file, output_dir=out_dir, include_ties=True, min_votes_per_system=5
    )
    en_battles_filtered = json.loads((out_dir / "votes-a-en.json").read_text(encoding="utf-8"))
    assert len(en_battles_filtered) == 0

    # Test async_main with input_csv
    await prepare_data_for_scoring.async_main(
        input_csv=csv_file,
        output_dir=out_dir,
        include_ties=True,
        min_votes_per_system=1,
    )
    assert (out_dir / "votes-a-en.json").exists()


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

    # Case 3: Ignored user in competition is excluded from output
    comp_with_ignored = dataclasses.replace(config.COMPETITIONS["mwahaha"], ignored_users=frozenset({"sys_b"}))
    with patch("mwahahavote.database.create_engine", fake_create_engine):
        await postprocess_scores.async_main(competition=comp_with_ignored)

    data_with_ignored = json.loads(out_json.read_text(encoding="utf-8"))
    assert "sys_b" not in {row["system"] for row in data_with_ignored}


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


def test_anonymize_dump_logic(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # 1. Test parse_prolific_tuples and sanitize_prolific_line
    line = (
        "INSERT INTO `prolific` VALUES "
        "('prolific-id-user1-sess1','2026-02-15 16:16:17',NULL,NULL),"
        "('prolific-id-user2-sess2','2026-02-06 22:46:50','2026-02-06 23:22:25','Fun, enjoyable study!'),"
        "('prolific-id-user3','2026-02-05 16:13:38','2026-02-05 16:51:13','Has escaped \\'quotes\\' inside');\n"
    )
    sanitized = anonymize_dump.sanitize_prolific_line(line)
    assert "Fun, enjoyable study!" not in sanitized
    assert "Has escaped" not in sanitized
    assert sanitized.startswith("INSERT INTO `prolific` VALUES ")
    assert anonymize_dump.sanitize_prolific_line("SELECT 1;") == "SELECT 1;"

    # 2. Test build_anonymization_maps and anonymize_prolific_id
    raw_ids = [
        "prolific-id-userB-sess2",
        "prolific-id-userA-sess1",
        "prolific-id-userA-sess3",
        "prolific-id-userC",
    ]
    w_map, s_map = anonymize_dump.build_anonymization_maps(raw_ids)
    assert w_map["userA"] == "worker_0001"
    assert w_map["userB"] == "worker_0002"
    assert w_map["userC"] == "worker_0003"
    assert s_map["sess1"] == "session_0001"
    assert s_map["sess2"] == "session_0002"
    assert s_map["sess3"] == "session_0003"

    assert (
        anonymize_dump.anonymize_prolific_id("prolific-id-userA-sess1", w_map, s_map)
        == "prolific-id-worker_0001-session_0001"
    )
    assert anonymize_dump.anonymize_prolific_id("prolific-id-userC", w_map, s_map) == "prolific-id-worker_0003"
    assert anonymize_dump.anonymize_prolific_id("regular_session", w_map, s_map) == "regular_session"

    # 3. Test anonymize_dump on dummy sql and gzipped sql
    raw_sql = (
        "-- MySQL dump\n"
        "SET @@GLOBAL.GTID_PURGED='xyz';\n"
        + line
        + "INSERT INTO `votes` VALUES ('p1','sys_a','sys_b','prolific-id-userA-sess1','a','2026-02-15 16:20:00',0,0);\n"
    )
    dump_in = tmp_path / "dump.sql"
    dump_in.write_text(raw_sql, encoding="utf-8")
    dump_out = tmp_path / "dump.sql.gz"

    lines, gtid_stripped = anonymize_dump.anonymize_dump(dump_in, dump_out, w_map, s_map)
    assert lines == 4
    assert gtid_stripped == 1

    with gzip.open(dump_out, "rt", encoding="utf-8") as f:
        dump_content = f.read()
    assert "GTID_PURGED" not in dump_content
    assert "prolific-id-userA-sess1" not in dump_content
    assert "prolific-id-worker_0001-session_0001" in dump_content
    assert "Fun, enjoyable study!" not in dump_content

    # 4. Test anonymize_csv on dummy csv
    csv_in = tmp_path / "annotations.csv"
    csv_in.write_text(
        "task,prompt_id,session_id,vote\nt3,p1,prolific-id-userA-sess1,a\nt3,p2,regular_user_123,b\n",
        encoding="utf-8",
    )
    csv_out = tmp_path / "annotations-sanitized.csv.gz"
    rows, anon_rows = anonymize_dump.anonymize_csv(csv_in, csv_out, w_map, s_map)
    assert rows == 2
    assert anon_rows == 1

    with gzip.open(csv_out, "rt", encoding="utf-8") as f:
        csv_content = f.read()
    assert "prolific-id-worker_0001-session_0001" in csv_content
    assert "regular_user_123" in csv_content
    assert "prolific-id-userA-sess1" not in csv_content

    # 5. Test main CLI execution
    monkeypatch.setattr(
        "sys.argv",
        [
            "anonymize_dump.py",
            "-i",
            str(dump_in),
            "-o",
            str(tmp_path / "cli_out.sql"),
            "--csv-inputs",
            str(csv_in),
            "--csv-outputs",
            str(tmp_path / "cli_csv_out.csv"),
        ],
    )
    anonymize_dump.main()
    assert (tmp_path / "cli_out.sql").exists()
    assert (tmp_path / "cli_csv_out.csv").exists()


async def test_compute_scores_async_main(
    seeded_db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    scores_dir = tmp_path / "scores"
    intermediate_dir = tmp_path / "scoring"

    await database.add_vote(seeded_db_engine, "sess-1", "en_1", "sys_a", "sys_b", "a", False, False)
    await database.add_vote(seeded_db_engine, "sess-2", "en_1", "sys_b", "sys_c", "a", False, False)
    await database.add_vote(seeded_db_engine, "sess-3", "en_1", "sys_c", "sys_a", "b", False, False)

    @asynccontextmanager
    async def fake_create_engine() -> Any:
        yield seeded_db_engine

    with patch("mwahahavote.database.create_engine", fake_create_engine):
        await compute_scores.async_main(
            scores_dir=scores_dir,
            intermediate_dir=intermediate_dir,
            min_votes_per_system=1,
            num_bootstrap=10,
            tasks=["a-en"],
        )

    out_file = scores_dir / "a-en.json"
    assert out_file.exists()
    records = json.loads(out_file.read_text(encoding="utf-8"))
    assert len(records) == 3
    by_sys = {r["system"]: r for r in records}
    assert all(by_sys[s]["rating"] is not None for s in ["sys_a", "sys_b", "sys_c"])
    assert (intermediate_dir / "votes-a-en.json").exists()
    assert (intermediate_dir / "elo_results_a-en.pkl").exists()

    # Test caching: 2nd run with unchanged votes should skip recomputation
    cache_file = tmp_path / "cache.json"
    with patch("mwahahavote.database.create_engine", fake_create_engine):
        res1 = await compute_scores.async_main(
            scores_dir=scores_dir,
            intermediate_dir=intermediate_dir,
            cache_file=cache_file,
            min_votes_per_system=1,
            num_bootstrap=10,
            tasks=["a-en"],
        )
        assert res1["a-en"] is True
        assert cache_file.exists()

        # Unchanged: should skip
        res2 = await compute_scores.async_main(
            scores_dir=scores_dir,
            intermediate_dir=intermediate_dir,
            cache_file=cache_file,
            min_votes_per_system=1,
            num_bootstrap=10,
            tasks=["a-en"],
        )
        assert res2["a-en"] is False

        # Force: should recompute
        res_force = await compute_scores.async_main(
            scores_dir=scores_dir,
            intermediate_dir=intermediate_dir,
            cache_file=cache_file,
            min_votes_per_system=1,
            num_bootstrap=10,
            tasks=["a-en"],
            force=True,
        )
        assert res_force["a-en"] is True

        # Directly touching the DB: updating a vote in-place automatically invalidates cache
        async with seeded_db_engine.begin() as conn:
            await conn.execute(sqlalchemy.sql.text("UPDATE votes SET vote = 'b' WHERE session_id = 'sess-1'"))

        res_updated = await compute_scores.async_main(
            scores_dir=scores_dir,
            intermediate_dir=intermediate_dir,
            cache_file=cache_file,
            min_votes_per_system=1,
            num_bootstrap=10,
            tasks=["a-en"],
        )
        assert res_updated["a-en"] is True

    # Test an empty/unvoted task writes null ratings
    with patch("mwahahavote.database.create_engine", fake_create_engine):
        await compute_scores.async_main(
            scores_dir=scores_dir,
            intermediate_dir=intermediate_dir,
            min_votes_per_system=1,
            tasks=["a-es"],
        )
    out_es = scores_dir / "a-es.json"
    assert out_es.exists()
    records_es = json.loads(out_es.read_text(encoding="utf-8"))
    assert all(r["rating"] is None for r in records_es)


async def test_compute_scores_input_csv(tmp_path: Path) -> None:
    csv_file = tmp_path / "votes.csv"
    csv_file.write_text(
        "task,prompt_id,system_id_a,system_id_b,vote,session_id\n"
        "a-en,en_1,sys_a,sys_b,a,sess1\n"
        "a-en,en_2,sys_b,sys_a,b,sess2\n",
        encoding="utf-8",
    )
    scores_dir = tmp_path / "scores"
    await compute_scores.async_main(
        input_csv=csv_file,
        scores_dir=scores_dir,
        min_votes_per_system=1,
        num_bootstrap=10,
    )
    out_file = scores_dir / "a-en.json"
    assert out_file.exists()
    records = json.loads(out_file.read_text(encoding="utf-8"))
    assert len(records) == 2


def test_compute_scores_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    csv_file = tmp_path / "votes.csv"
    csv_file.write_text(
        "task,prompt_id,system_id_a,system_id_b,vote,session_id\n"
        "a-en,en_1,sys_a,sys_b,a,sess1\n"
        "a-en,en_2,sys_b,sys_a,b,sess2\n",
        encoding="utf-8",
    )
    scores_dir = tmp_path / "scores"
    monkeypatch.setattr(
        "sys.argv",
        [
            "compute_scores.py",
            "--input-csv",
            str(csv_file),
            "--scores-dir",
            str(scores_dir),
            "--min-votes-per-system",
            "1",
            "--num-bootstrap",
            "10",
            "--cache-file",
            str(tmp_path / "cache.json"),
            "--force",
        ],
    )
    compute_scores.main()
    assert (scores_dir / "a-en.json").exists()
