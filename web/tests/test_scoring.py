"""Tests for mwahahavote.scoring and scripts/elo_analysis."""

import pickle
from pathlib import Path

import elo_analysis
import numpy as np
import pandas as pd
import pytest

from mwahahavote.scoring import (
    bt_loss_and_grad,
    compute_bootstrap_bt,
    compute_bt,
    compute_elo,
    get_matchups_models,
    preprocess_for_bt,
    preprocess_for_elo,
    report_elo_analysis_results,
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
    table_df = res["leaderboard_table_df"]
    assert "rating" in table_df.columns
    assert "variance" in table_df.columns
    assert "final_ranking" in table_df.columns


def test_report_elo_analysis_results_unsupported_system() -> None:
    df = _sample_battles()
    with pytest.raises(NotImplementedError):
        report_elo_analysis_results(df, rating_system="invalid")


def test_elo_analysis_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
        ],
    )
    elo_analysis.main()

    assert output_pkl.is_file()
    with open(output_pkl, "rb") as f:
        data = pickle.load(f)
    assert "full" in data
    assert "leaderboard_table_df" in data["full"]
