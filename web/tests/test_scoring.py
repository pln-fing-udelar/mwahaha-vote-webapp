"""Tests for mwahahavote.scoring and scripts/elo_analysis."""

import pickle
from pathlib import Path

import elo_analysis
import numpy as np
import pandas as pd
import pytest

from mwahahavote.scoring import (
    _always_true,
    bt_loss_and_grad,
    cells_from_battles,
    compute_bootstrap_bt,
    compute_bt,
    compute_elo,
    get_matchups_models,
    preprocess_for_bt,
    preprocess_for_elo,
    pretty_print_elo_rating,
    report_elo_analysis_results,
    report_elo_analysis_results_from_cells,
    scale_and_offset,
)


def _sample_battles() -> pd.DataFrame:
    rows = [
        {"model_a": "sys_a", "model_b": "sys_b", "winner": "model_a", "tstamp": 100, "anony": True, "language": "en"},
        {"model_a": "sys_b", "model_b": "sys_c", "winner": "model_b", "tstamp": 200, "anony": True, "language": "en"},
        {"model_a": "sys_a", "model_b": "sys_c", "winner": "tie", "tstamp": 300, "anony": True, "language": "es"},
        {"model_a": "sys_c", "model_b": "sys_a", "winner": "model_b", "tstamp": 400, "anony": True, "language": "en"},
        {"model_a": "sys_b", "model_b": "sys_a", "winner": "model_a", "tstamp": 500, "anony": True, "language": "en"},
        {
            "model_a": "sys_b",
            "model_b": "sys_c",
            "winner": "model_a",
            "tstamp": 600,
            "anony": False,
            "language": "unknown",
        },
    ]
    return pd.DataFrame(rows)


def test_always_true() -> None:
    assert _always_true(pd.Series()) is True


def test_get_matchups_models() -> None:
    df = _sample_battles()
    matchups, models = get_matchups_models(df)
    assert len(models) == 3
    assert set(models) == {"sys_a", "sys_b", "sys_c"}
    assert matchups.shape == (len(df), 2)


def test_preprocess_for_bt() -> None:
    df = _sample_battles()
    matchups, outcomes, models, weights = preprocess_for_bt(df)
    assert len(models) == 3
    assert len(matchups) == len(outcomes) == len(weights)
    assert all(o in {0.0, 0.5, 1.0} for o in outcomes)
    assert weights.sum() == len(df)


def test_cells_from_battles() -> None:
    df = pd.DataFrame(
        {
            "model_a": ["sys_a", "sys_a", "sys_b", "sys_a", "sys_a"],
            "model_b": ["sys_b", "sys_b", "sys_a", "sys_b", "sys_b"],
            "winner": ["model_a", "model_a", "model_b", "tie", "tie (bothbad)"],
        }
    )
    assert cells_from_battles(df).to_dict("records") == [
        {"model_a": "sys_a", "model_b": "sys_b", "winner": "model_a", "count": 2},
        {"model_a": "sys_a", "model_b": "sys_b", "winner": "tie", "count": 2},
        {"model_a": "sys_b", "model_b": "sys_a", "winner": "model_b", "count": 1},
    ]


def test_preprocess_for_bt_does_not_depend_on_the_order_of_the_battles() -> None:
    df = _sample_battles()
    shuffled = df.sample(frac=1, random_state=1).reset_index(drop=True)
    for original, from_shuffled in zip(preprocess_for_bt(df), preprocess_for_bt(shuffled), strict=True):
        assert np.array_equal(original, from_shuffled)


def test_report_elo_analysis_results_from_cells_matches_the_one_from_battles() -> None:
    battles = _sample_battles()
    battles = battles[battles["anony"]]
    res = report_elo_analysis_results(battles, num_bootstrap=20)
    res_cells = report_elo_analysis_results_from_cells(
        cells_from_battles(battles), last_updated_tstamp=int(battles["tstamp"].max()), num_bootstrap=20
    )

    pd.testing.assert_frame_equal(res["leaderboard_table_df"], res_cells["leaderboard_table_df"], check_dtype=False)
    pd.testing.assert_frame_equal(res["bootstrap_df"], res_cells["bootstrap_df"])
    assert res["last_updated_tstamp"] == res_cells["last_updated_tstamp"]
    assert res_cells["elo_rating_online"] is None


def test_preprocess_for_elo() -> None:
    df = _sample_battles()
    matchups, outcomes, models = preprocess_for_elo(df)
    assert len(models) == 3
    assert len(matchups) == len(outcomes) == len(df)
    assert outcomes[0] == 1.0  # model_a win
    assert outcomes[2] == 0.5  # tie


def test_compute_elo() -> None:
    df = _sample_battles()
    ratings = compute_elo(df)
    assert set(ratings.keys()) == {"sys_a", "sys_b", "sys_c"}
    assert all(isinstance(v, float) for v in ratings.values())


def test_bt_loss_and_grad() -> None:
    df = _sample_battles()
    matchups, outcomes, models, weights = preprocess_for_bt(df)
    ratings = np.zeros(len(models), dtype=np.float64)
    loss, grad = bt_loss_and_grad(ratings, matchups, outcomes, weights)
    assert isinstance(loss, float)
    assert grad.shape == (len(models),)
    assert np.isfinite(loss)
    assert np.all(np.isfinite(grad))


def test_scale_and_offset_with_and_without_baseline() -> None:
    ratings = np.array([1.0, 0.5, -0.5])
    models = ["model_1", "model_2", "model_3"]
    scaled = scale_and_offset(ratings, models, scale=400.0, init_rating=1000.0)
    assert scaled[0] == 1400.0
    assert scaled[1] == 1200.0
    assert scaled[2] == 800.0

    # With baseline model
    models_with_base = ["model_1", "mixtral-8x7b-instruct-v0.1", "model_3"]
    scaled_base = scale_and_offset(ratings, models_with_base, scale=400.0, init_rating=1000.0, baseline_rating=1114.0)
    # The baseline index is 1, original scaled rating is 1200. Offset = 1114 - 1200 = -86.
    assert scaled_base[1] == 1114.0
    assert scaled_base[0] == 1400.0 - 86.0


def test_compute_bt() -> None:
    df = _sample_battles()
    bt_series = compute_bt(df)
    assert isinstance(bt_series, pd.Series)
    assert set(bt_series.index) == {"sys_a", "sys_b", "sys_c"}
    assert bt_series.is_monotonic_decreasing


def test_compute_bootstrap_bt() -> None:
    df = _sample_battles()
    # Test single-core
    boot_df_seq = compute_bootstrap_bt(df, num_round=10, num_cpu=1)
    assert boot_df_seq.shape == (10, 3)
    assert set(boot_df_seq.columns) == {"sys_a", "sys_b", "sys_c"}

    # Test multi-core
    boot_df_mp = compute_bootstrap_bt(df, num_round=10, num_cpu=2)
    assert boot_df_mp.shape == (10, 3)
    assert set(boot_df_mp.columns) == {"sys_a", "sys_b", "sys_c"}


def test_report_elo_analysis_results() -> None:
    df = _sample_battles()
    res = report_elo_analysis_results(
        df,
        rating_system="bt",
        num_bootstrap=20,
        langs=["en"],
        exclude_models=["sys_c"],
        exclude_tie=True,
        exclude_unknown_lang=True,
        num_cpu=1,
    )
    assert res["rating_system"] == "bt"
    assert "sys_c" not in res["elo_rating_final"]
    assert "elo_rating_median" in res
    assert "sys_a" in res["elo_rating_median"]
    table_df = res["leaderboard_table_df"]
    assert "rating" in table_df.columns
    assert "variance" in table_df.columns
    assert "final_ranking" in table_df.columns


def test_report_elo_analysis_results_with_custom_filter() -> None:
    df = _sample_battles()
    res = report_elo_analysis_results(
        df,
        rating_system="bt",
        num_bootstrap=10,
        filter_func=lambda row: bool(row["tstamp"] < 500),
        num_cpu=1,
    )
    assert res["last_updated_tstamp"] == 400


def test_report_elo_analysis_results_with_custom_tol() -> None:
    df = _sample_battles()
    res = report_elo_analysis_results(
        df,
        rating_system="bt",
        num_bootstrap=10,
        tol=1e-4,
        num_cpu=1,
    )
    assert "leaderboard_table_df" in res
    assert res["rating_system"] == "bt"


def test_report_elo_analysis_results_unsupported_system() -> None:
    df = _sample_battles()
    with pytest.raises(NotImplementedError):
        report_elo_analysis_results(df, rating_system="invalid")


def test_pretty_print_elo_rating(capsys: pytest.CaptureFixture[str]) -> None:
    ratings = {"model_b": 950.2, "model_a": 1280.8, "model_c": 1100.0}
    pretty_print_elo_rating(ratings)
    out = capsys.readouterr().out
    lines = [line.strip() for line in out.strip().splitlines()]
    assert len(lines) == 3
    assert lines[0].startswith("1, model_a") and lines[0].endswith("1281")
    assert lines[1].startswith("2, model_c") and lines[1].endswith("1100")
    assert lines[2].startswith("3, model_b") and lines[2].endswith("950")


def test_elo_analysis_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    df = _sample_battles()
    input_json = tmp_path / "battles.json"
    output_pkl = tmp_path / "results.pkl"
    df.to_json(input_json, orient="records")

    monkeypatch.setattr(
        "sys.argv",
        [
            "elo_analysis.py",
            "--clean-battle-file",
            str(input_json),
            "--output",
            str(output_pkl),
            "--num-bootstrap",
            "10",
            "--num-cpu",
            "1",
            "--tol",
            "1e-4",
        ],
    )
    elo_analysis.main()

    out = capsys.readouterr().out
    assert "# Results for full conversations" in out
    assert "# Online Elo" in out
    assert "# Median" in out
    assert "last update :" in out

    assert output_pkl.is_file()
    with open(output_pkl, "rb") as f:
        data = pickle.load(f)
    assert "full" in data
    assert "leaderboard_table_df" in data["full"]

    # Test --quiet suppresses output
    monkeypatch.setattr(
        "sys.argv",
        [
            "elo_analysis.py",
            "--clean-battle-file",
            str(input_json),
            "--output",
            str(output_pkl),
            "--num-bootstrap",
            "10",
            "--num-cpu",
            "1",
            "--tol",
            "1e-4",
            "--quiet",
        ],
    )
    elo_analysis.main()
    assert capsys.readouterr().out == ""
