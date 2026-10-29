import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.asyncio
from pydantic import ValidationError

from ingestion import codabench, submission
from ingestion.codabench import Submission


def test_get_environ_session_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CODABENCH_SESSION_ID", "my-session-123")
    assert codabench.get_environ_session_id() == "my-session-123"


def test_is_session_id_valid() -> None:
    mock_resp_valid = MagicMock()
    mock_resp_valid.text = "<html><div id='user_dropdown'>User</div></html>"
    with patch("httpx2.get", return_value=mock_resp_valid) as mock_get:
        assert codabench.is_session_id_valid("valid-sid") is True
        mock_get.assert_called_once_with("https://www.codabench.org/", cookies={"sessionid": "valid-sid"})
        mock_resp_valid.raise_for_status.assert_called_once()

    mock_resp_invalid = MagicMock()
    mock_resp_invalid.text = "<html>Login</html>"
    with patch("httpx2.get", return_value=mock_resp_invalid):
        assert codabench.is_session_id_valid("invalid-sid") is False


def test_task_id_to_task_and_get_submission_url(monkeypatch: pytest.MonkeyPatch) -> None:
    assert codabench.task_id_to_task(21358) == "t3"

    monkeypatch.setenv("CODABENCH_SESSION_ID", "env-sid")
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"data_file": "https://codabench.org/files/sub42.zip"}
    with patch("httpx2.get", return_value=mock_resp) as mock_get:
        url = codabench.get_submission_url(42)
        assert url == "https://codabench.org/files/sub42.zip"
        mock_get.assert_called_once_with(
            "https://www.codabench.org/api/submissions/42/get_details",
            cookies={"sessionid": "env-sid"},
        )


def test_submission_dataclass() -> None:
    dt = datetime.datetime(2026, 1, 15, tzinfo=datetime.UTC)
    s1 = Submission(id=10, user="alice", date=dt, tasks=["t3"], tests_passed=[True], path_or_url="/tmp/a.tsv")
    s1_dup = Submission(id=10, user="bob", date=dt)
    s2 = Submission(id=20, user="bob", date=dt)

    assert s1.system_id == "alice"
    assert s1 == s1_dup
    assert hash(s1) == hash(s1_dup)
    assert s1 != s2
    assert s1 < s2
    assert s1 != "not-a-submission"
    assert s1.__eq__("not-a-submission") is NotImplemented
    assert s1.compute_path_or_url() == "/tmp/a.tsv"

    with patch("ingestion.codabench.get_submission_url", return_value="https://example.com/20.zip") as mock_url:
        assert s2.compute_path_or_url(session_id="sid") == "https://example.com/20.zip"
        mock_url.assert_called_once_with(20, session_id="sid")


def test_list_submission_dicts_and_list_submissions() -> None:
    with patch("ingestion.codabench.is_session_id_valid", return_value=False):
        with pytest.raises(ValueError, match="The provided session ID is not valid"):
            codabench._list_submission_dicts(session_id="bad")

    sample_dicts: list[dict[str, Any]] = [
        # Single-task submission that passed test
        {
            "id": 100,
            "parent": None,
            "children": [],
            "owner": "alice",
            "created_when": "2026-01-10T12:00:00Z",
            "is_soft_deleted": False,
            "task": {"id": 1},
            "scores": [{"score": "1.0"}],
        },
        # Single-task submission with empty scores
        {
            "id": 101,
            "parent": None,
            "children": [],
            "owner": "bob",
            "created_when": "2026-01-11T12:00:00Z",
            "is_soft_deleted": True,
            "task": {"id": 1},
            "scores": [],
        },
        # Parent submission with a child
        {
            "id": 200,
            "parent": None,
            "children": [201],
            "owner": "charlie",
            "created_when": "2026-01-12T12:00:00Z",
            "is_soft_deleted": False,
        },
        # Child of 200
        {
            "id": 201,
            "parent": 200,
            "children": [],
            "owner": "charlie",
            "created_when": "2026-01-12T12:00:00Z",
            "is_soft_deleted": False,
            "task": {"id": 1},
            "scores": [{"score": "0.0"}],
        },
    ]

    mock_resp = MagicMock()
    mock_resp.json.return_value = sample_dicts

    with (
        patch("ingestion.codabench.is_session_id_valid", return_value=True),
        patch("httpx2.get", return_value=mock_resp) as mock_get,
    ):
        # Test with phase_id=None (uses phase__competition)
        res_comp = codabench._list_submission_dicts(competition_id=9719, phase_id=None, session_id="sid")
        assert res_comp == sample_dicts
        assert mock_get.call_args.kwargs["params"] == {"show_is_soft_deleted": True, "phase__competition": 9719}

        # Test list_submissions (uses phase)
        subs = list(codabench.list_submissions(competition_id=9719, phase_id=15785, session_id="sid"))
        assert len(subs) == 3
        by_id = {s.id: s for s in subs}
        assert by_id[100].user == "alice"
        assert by_id[100].tests_passed == [True]
        assert by_id[101].is_deleted is True
        assert by_id[101].tests_passed == [False]
        assert by_id[200].tasks == ["t3"]
        assert by_id[200].tests_passed == [False]

    # Invalid API schema raises Pydantic ValidationError
    mock_bad_resp = MagicMock()
    mock_bad_resp.json.return_value = [{"id": "not-an-int"}]
    with (
        patch("ingestion.codabench.is_session_id_valid", return_value=True),
        patch("httpx2.get", return_value=mock_bad_resp),
        pytest.raises(ValidationError),
    ):
        codabench.list_submissions(session_id="sid")


def test_print_stats(capsys: pytest.CaptureFixture[str]) -> None:
    dt = datetime.datetime(2026, 1, 15, tzinfo=datetime.UTC)
    subs = [
        Submission(id=1, user="Alice", date=dt, tasks=["t3"], tests_passed=[True], is_deleted=False),
        Submission(id=2, user="bob", date=dt, tasks=["t3"], tests_passed=[False], is_deleted=False),
        Submission(id=3, user="charlie", date=dt, tasks=["t3"], tests_passed=[True], is_deleted=True),
    ]
    submission.print_stats(subs)
    out = capsys.readouterr().out
    assert "3 submissions." in out
    assert "2 submissions that were not deleted." in out
    assert "1 submissions that were not deleted and passed the test" in out
    assert "['Alice']" in out


def test_mysql_insert_on_conflict_update() -> None:
    metadata = sqlalchemy.MetaData()
    tbl = sqlalchemy.Table(
        "outputs",
        metadata,
        sqlalchemy.Column("prompt_id", sqlalchemy.String(10), primary_key=True),
        sqlalchemy.Column("system_id", sqlalchemy.String(100), primary_key=True),
        sqlalchemy.Column("text", sqlalchemy.String(2048)),
    )
    mock_sql_table = MagicMock()
    mock_sql_table.table = tbl
    mock_conn = MagicMock()
    mock_conn.execute.return_value.rowcount = 2

    count = submission._mysql_insert_on_conflict_update(
        mock_sql_table,
        mock_conn,
        ["prompt_id", "system_id", "text"],
        [("en_1", "sys_a", "hello"), ("en_2", "sys_a", "world")],
    )
    assert count == 2
    mock_conn.execute.assert_called_once()


async def test_ingest_submission_and_list_systems(
    db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
    tmp_path: Path,
) -> None:
    async with db_engine.begin() as conn:
        await conn.execute(
            sqlalchemy.sql.text("""
                INSERT INTO prompts (prompt_id, word1, word2, headline, url, prompt, task, phase_id) VALUES
                ('en_1', 'cat', 'dog', NULL, NULL, NULL, 't3', 1),
                ('en_2', NULL, NULL, 'Headline', NULL, NULL, 't3', 1)
            """)
        )

    tsv_file = tmp_path / "sub.tsv"
    tsv_file.write_text("id\ttext\nen_1\tJoke one\nen_2\t\n", encoding="utf-8")

    dt = datetime.datetime(2026, 1, 15, tzinfo=datetime.UTC)
    sub = Submission(id=1, user="new_system", date=dt, tasks=["t3"], tests_passed=[True], path_or_url=str(tsv_file))

    affected = await submission.ingest_submission(db_engine, phase_id=1, submission=sub, accept_null_texts=True)
    assert affected == 2

    ingested_ids = [s async for s in submission.list_ingested_system_ids(db_engine)]
    assert ingested_ids == ["new_system"]

    # Verify null text was replaced with "-"
    async with db_engine.connect() as conn:
        rows = dict(
            iter(
                await conn.execute(
                    sqlalchemy.sql.text("SELECT prompt_id, text FROM outputs WHERE system_id = 'new_system'")
                )
            )
        )
        assert rows == {"en_1": "Joke one", "en_2": "-"}

    # Re-ingesting same system with system_exists_ok=False raises IntegrityError
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        await submission.ingest_submission(db_engine, phase_id=1, submission=sub, system_exists_ok=False)

    # With system_exists_ok=True, it proceeds past system insertion; test missing file error
    sub_missing = Submission(
        id=2, user="new_system", date=dt, tasks=["t3"], tests_passed=[True], path_or_url=str(tmp_path / "missing.tsv")
    )
    with pytest.raises(ValueError, match="doesn't exist"):
        await submission.ingest_submission(db_engine, phase_id=1, submission=sub_missing, system_exists_ok=True)

    # Directory path instead of file raises ValueError
    sub_dir = Submission(id=3, user="new_system", date=dt, tasks=["t3"], tests_passed=[True], path_or_url=str(tmp_path))
    with pytest.raises(ValueError, match="isn't a file"):
        await submission.ingest_submission(db_engine, phase_id=1, submission=sub_dir, system_exists_ok=True)

    # Mismatched prompt IDs raises ValueError
    bad_tsv = tmp_path / "bad_ids.tsv"
    bad_tsv.write_text("id\ttext\nen_1\tJoke one\nextra_id\tJoke extra\n", encoding="utf-8")
    sub_bad_ids = Submission(
        id=4, user="new_system", date=dt, tasks=["t3"], tests_passed=[True], path_or_url=str(bad_tsv)
    )
    with pytest.raises(ValueError, match="do not match the reference IDs"):
        await submission.ingest_submission(db_engine, phase_id=1, submission=sub_bad_ids, system_exists_ok=True)
