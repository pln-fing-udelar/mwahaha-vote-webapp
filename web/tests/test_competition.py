import pytest

from ingestion import codabench
from mwahahavote import config, database


def test_competition_configs() -> None:
    haha = config.COMPETITIONS["haha"]
    assert haha.id == "haha"
    assert haha.phase_id == 1
    assert haha.default_task == "t3"
    assert haha.task_ids == ("t3",)
    assert haha.allow_codabench is False

    mwahaha = config.COMPETITIONS["mwahaha"]
    assert mwahaha.id == "mwahaha"
    assert mwahaha.phase_id == 15785
    assert mwahaha.default_task == "a-en"
    assert mwahaha.task_ids == ("a-en", "a-es", "a-zh", "b1", "b2")
    assert mwahaha.allow_codabench is True
    assert mwahaha.codabench_competition_id == 9719
    assert mwahaha.codabench_task_map == {
        21358: "a-en",
        21359: "a-es",
        21360: "a-zh",
        21361: "b1",
        22992: "b2",
    }
    assert mwahaha.task_by_id["a-en"].label == "Task A: Joke in English"
    assert mwahaha.prolific_completion_codes["a-en"] == "CC4WY7K5"


def test_get_active_competition(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COMPETITION", raising=False)
    assert config.get_active_competition().id == "mwahaha"

    monkeypatch.setenv("COMPETITION", "HAHA")
    assert config.get_active_competition().id == "haha"

    monkeypatch.setenv("COMPETITION", "unknown_comp")
    assert config.get_active_competition().id == "mwahaha"


def test_get_competition(monkeypatch: pytest.MonkeyPatch) -> None:
    assert config.get_competition("haha").id == "haha"
    assert config.get_competition("MWAHAHA").id == "mwahaha"

    monkeypatch.setenv("COMPETITION", "haha")
    assert config.get_competition().id == "haha"

    with pytest.raises(ValueError, match="Unknown competition 'nonexistent'"):
        config.get_competition("nonexistent")


def test_prompt_id_to_task_mwahaha(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(database, "ACTIVE_COMPETITION", config.COMPETITIONS["mwahaha"])

    assert database.prompt_id_to_task("en_joke_1") == "a-en"
    assert database.prompt_id_to_task("es_joke_2") == "a-es"
    assert database.prompt_id_to_task("zh_joke_3") == "a-zh"
    assert database.prompt_id_to_task("img_100") == "b1"
    assert database.prompt_id_to_task("img_2_200") == "b2"

    with pytest.raises(ValueError, match="Cannot determine the task"):
        database.prompt_id_to_task("unrecognized_prompt_id")


def test_prompt_id_to_task_haha(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(database, "ACTIVE_COMPETITION", config.COMPETITIONS["haha"])

    assert database.prompt_id_to_task("t3_001") == "t3"
    assert database.prompt_id_to_task("random_id") == "t3"


def test_codabench_task_id_to_task_mwahaha(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(codabench, "ACTIVE_COMPETITION", config.COMPETITIONS["mwahaha"])

    assert codabench.task_id_to_task(21358) == "a-en"
    assert codabench.task_id_to_task(21359) == "a-es"
    assert codabench.task_id_to_task(21360) == "a-zh"
    assert codabench.task_id_to_task(21361) == "b1"
    assert codabench.task_id_to_task(22992) == "b2"

    with pytest.raises(ValueError, match="Unknown task ID"):
        codabench.task_id_to_task(99999)


def test_codabench_task_id_to_task_haha(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(codabench, "ACTIVE_COMPETITION", config.COMPETITIONS["haha"])

    assert codabench.task_id_to_task(1) == "t3"


def test_is_only_latest_submission_per_user(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ONLY_LATEST_SUBMISSION_PER_USER", raising=False)
    monkeypatch.setattr(config, "ACTIVE_COMPETITION", config.COMPETITIONS["haha"])
    assert config.is_only_latest_submission_per_user() is True

    monkeypatch.setattr(config, "ACTIVE_COMPETITION", config.COMPETITIONS["mwahaha-dev"])
    assert config.is_only_latest_submission_per_user() is False

    # Overridden via env var
    monkeypatch.setenv("ONLY_LATEST_SUBMISSION_PER_USER", "false")
    assert config.is_only_latest_submission_per_user() is False

    monkeypatch.setenv("ONLY_LATEST_SUBMISSION_PER_USER", "1")
    assert config.is_only_latest_submission_per_user() is True


def test_submission_system_id_parameterization(monkeypatch: pytest.MonkeyPatch) -> None:
    import datetime

    from ingestion.codabench import Submission

    dt = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    sub = Submission(id=42, user="alice", date=dt)

    # When taking only the latest, system_id is user
    monkeypatch.setattr(config, "ACTIVE_COMPETITION", config.COMPETITIONS["haha"])
    monkeypatch.delenv("ONLY_LATEST_SUBMISSION_PER_USER", raising=False)
    assert sub.system_id == "alice"

    # When taking each submission, system_id includes the submission id
    monkeypatch.setenv("ONLY_LATEST_SUBMISSION_PER_USER", "false")
    assert sub.system_id == "alice-42"

    # With override, system_id is always the override
    sub_override = Submission(id=42, user="alice", date=dt, system_id_override="team_x")
    assert sub_override.system_id == "team_x"


def test_filter_submissions_to_ingest() -> None:
    import datetime

    import ingest_submissions

    from ingestion.codabench import Submission

    dt1 = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    dt2 = datetime.datetime(2026, 1, 2, tzinfo=datetime.UTC)
    s1 = Submission(id=1, user="alice", date=dt1)
    s2 = Submission(id=2, user="alice", date=dt2)
    s3 = Submission(id=3, user="bob", date=dt1)

    # only_latest_per_user=True keeps latest per user
    latest = ingest_submissions._filter_submissions_to_ingest([s1, s2, s3], only_latest_per_user=True)
    assert len(latest) == 2
    assert s2 in latest
    assert s1 not in latest
    assert s3 in latest

    # only_latest_per_user=False keeps all submissions
    all_subs = ingest_submissions._filter_submissions_to_ingest([s1, s2, s3], only_latest_per_user=False)
    assert len(all_subs) == 3
    assert all_subs == [s1, s2, s3]


def test_ingest_submissions_main_argparse(monkeypatch: pytest.MonkeyPatch) -> None:
    from unittest.mock import AsyncMock, patch

    import ingest_submissions

    with patch.object(ingest_submissions, "async_main", AsyncMock()) as mock_async:
        monkeypatch.setattr("sys.argv", ["ingest_submissions.py", "--all-submissions"])
        ingest_submissions.main()
        mock_async.assert_awaited_once_with(only_latest_per_user=False, competition=None)

    with patch.object(ingest_submissions, "async_main", AsyncMock()) as mock_async:
        monkeypatch.setattr("sys.argv", ["ingest_submissions.py", "--only-latest"])
        ingest_submissions.main()
        mock_async.assert_awaited_once_with(only_latest_per_user=True, competition=None)
