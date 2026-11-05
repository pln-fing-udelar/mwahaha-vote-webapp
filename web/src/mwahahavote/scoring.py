# Based on LMSYS FastChat's Bradley-Terry implementation (Apache License 2.0).
# https://github.com/lm-sys/FastChat
"""Bradley-Terry and Elo scoring algorithms with bootstrap confidence intervals."""

from collections.abc import Callable
from functools import partial
from typing import Any
from zoneinfo import ZoneInfo
lazy import datetime
lazy import math
lazy import multiprocessing as mp

lazy import numpy as np
lazy import pandas as pd
lazy from scipy.optimize import minimize
lazy from scipy.special import expit, log_expit


def _always_true(_: pd.Series) -> bool:
    return True


def get_matchups_models(df: pd.DataFrame) -> tuple[np.ndarray, list[str]]:
    """Extract matchup indices and distinct model IDs from a battles dataframe."""
    n_rows = len(df)
    model_indices, models = pd.factorize(pd.concat([df["model_a"], df["model_b"]]))
    matchups = np.column_stack([model_indices[:n_rows], model_indices[n_rows:]])
    return matchups, models.to_list()


def cells_from_battles(df: pd.DataFrame) -> pd.DataFrame:
    """Group battles into (model_a, model_b, winner) cells with the number of battles in each one."""
    winner = np.where(df["winner"] == "model_a", "model_a", np.where(df["winner"] == "model_b", "model_b", "tie"))
    return (
        pd.DataFrame({"model_a": df["model_a"].to_numpy(), "model_b": df["model_b"].to_numpy(), "winner": winner})
        .groupby(["model_a", "model_b", "winner"], sort=True)
        .size()
        .rename("count")
        .reset_index()
    )


def preprocess_cells_for_bt(cells: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, list[str], np.ndarray]:
    """Preprocess (model_a, model_b, winner, count) cells into matchups, outcomes, models, and observation weights.

    The models are sorted by ID, so the results don't depend on the order the battles were in.
    """
    cells = cells.groupby(["model_a", "model_b", "winner"], sort=True, as_index=False).agg(count=("count", "sum"))
    models = sorted(set(cells["model_a"]) | set(cells["model_b"]))
    model_to_index = {model: i for i, model in enumerate(models)}
    model_a_indices = cells["model_a"].map(model_to_index).to_numpy(dtype=np.int32)
    model_b_indices = cells["model_b"].map(model_to_index).to_numpy(dtype=np.int32)
    # model_a win -> 2, tie -> 1, model_b win -> 0
    outcome_ids = cells["winner"].map({"model_b": 0, "tie": 1, "model_a": 2}).to_numpy(dtype=np.int32)
    order = np.lexsort((outcome_ids, model_b_indices, model_a_indices))
    matchups = np.column_stack([model_a_indices[order], model_b_indices[order]])
    outcomes = outcome_ids[order].astype(np.float64) / 2.0
    weights = cells["count"].to_numpy(dtype=np.float64)[order]
    return matchups, outcomes, models, weights


def preprocess_for_bt(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, list[str], np.ndarray]:
    """Preprocess battles into unique (matchup, outcome) pairs with corresponding observation weights."""
    return preprocess_cells_for_bt(cells_from_battles(df))


def preprocess_for_elo(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """Preprocess battles into matchup arrays and float outcome labels for online Elo calculation."""
    matchups, models = get_matchups_models(df)
    outcomes = np.full(len(df), 0.5)
    outcomes[df["winner"] == "model_a"] = 1.0
    outcomes[df["winner"] == "model_b"] = 0.0
    return matchups, outcomes, models


def compute_elo(
    df: pd.DataFrame,
    k: float = 4.0,
    base: float = 10.0,
    init_rating: float = 1000.0,
    scale: float = 400.0,
) -> dict[str, float]:
    """Compute sequential online Elo ratings for all models."""
    matchups, outcomes, models = preprocess_for_elo(df)
    alpha = math.log(base) / scale
    ratings = np.full(shape=(len(models),), fill_value=init_rating)
    for (model_a_idx, model_b_idx), outcome in zip(matchups, outcomes, strict=False):
        prob = 1.0 / (1.0 + math.exp(alpha * (ratings[model_b_idx] - ratings[model_a_idx])))
        update = k * (outcome - prob)
        ratings[model_a_idx] += update
        ratings[model_b_idx] -= update
    return {model: float(ratings[idx]) for idx, model in enumerate(models)}


def pretty_print_elo_rating(rating: dict[str, float]) -> None:
    """Print Elo ratings formatted in descending order."""
    model_order = sorted(rating.keys(), key=lambda k: -rating[k])
    for i, model in enumerate(model_order):
        print(f"{i + 1:2d}, {model:25s}, {rating[model]:.0f}")


def bt_loss_and_grad(
    ratings: np.ndarray,
    matchups: np.ndarray,
    outcomes: np.ndarray,
    weights: np.ndarray,
    alpha: float = 1.0,
) -> tuple[float, np.ndarray]:
    """Compute negative log-likelihood loss and gradients for the Bradley-Terry model."""
    diff = ratings[matchups[:, 0]] - ratings[matchups[:, 1]]
    logits = alpha * diff
    probs = expit(logits)
    loss = -float(((log_expit(logits) * outcomes + log_expit(-logits) * (1.0 - outcomes)) * weights).sum())
    m_grads = alpha * (probs - outcomes) * weights
    n_models = len(ratings)
    model_grad = np.bincount(matchups[:, 0], weights=m_grads, minlength=n_models) - np.bincount(
        matchups[:, 1], weights=m_grads, minlength=n_models
    )
    return loss, model_grad


def fit_bt(
    matchups: np.ndarray,
    outcomes: np.ndarray,
    weights: np.ndarray,
    n_models: int,
    alpha: float,
    tol: float = 1e-6,
) -> np.ndarray:
    """Fit Bradley-Terry model parameters using L-BFGS-B optimization."""
    initial_ratings = np.zeros(n_models, dtype=np.float64)
    result = minimize(
        fun=bt_loss_and_grad,
        x0=initial_ratings,
        args=(matchups, outcomes, weights, alpha),
        jac=True,
        method="L-BFGS-B",
        options={"maxiter": 100, "gtol": tol},
    )
    return result["x"]


def scale_and_offset(
    ratings: np.ndarray,
    models: list[str],
    scale: float = 400.0,
    init_rating: float = 1000.0,
    baseline_model: str = "mixtral-8x7b-instruct-v0.1",
    baseline_rating: float = 1114.0,
) -> np.ndarray:
    """Convert natural scale ratings to Elo scale, optionally anchoring to a baseline model."""
    scaled_ratings = (ratings * scale) + init_rating
    if baseline_model in models:
        baseline_idx = models.index(baseline_model)
        scaled_ratings += baseline_rating - scaled_ratings[..., [baseline_idx]]
    return scaled_ratings


def compute_bt_from_data(
    bt_data: tuple[np.ndarray, np.ndarray, list[str], np.ndarray],
    base: float = 10.0,
    scale: float = 400.0,
    init_rating: float = 1000.0,
    tol: float = 1e-6,
) -> pd.Series:
    """Compute Bradley-Terry maximum likelihood ratings from preprocessed battles."""
    matchups, outcomes, models, weights = bt_data
    ratings = fit_bt(matchups, outcomes, weights, len(models), math.log(base), tol)
    scaled_ratings = scale_and_offset(ratings, models, scale, init_rating=init_rating)
    return pd.Series(scaled_ratings, index=models).sort_values(ascending=False)


def compute_bt(
    df: pd.DataFrame,
    base: float = 10.0,
    scale: float = 400.0,
    init_rating: float = 1000.0,
    tol: float = 1e-6,
) -> pd.Series:
    """Compute Bradley-Terry maximum likelihood ratings for models in the dataset."""
    return compute_bt_from_data(preprocess_for_bt(df), base, scale, init_rating, tol)


def compute_bootstrap_bt_from_data(
    bt_data: tuple[np.ndarray, np.ndarray, list[str], np.ndarray],
    num_round: int = 100,
    base: float = 10.0,
    scale: float = 400.0,
    init_rating: float = 1000.0,
    tol: float = 1e-6,
    num_cpu: int | None = None,
) -> pd.DataFrame:
    """Compute bootstrap sample distributions of Bradley-Terry ratings from preprocessed battles."""
    matchups, outcomes, models, weights = bt_data
    n_battles = int(weights.sum())
    rng = np.random.default_rng(seed=0)
    idxs = rng.multinomial(n=n_battles, pvals=weights / weights.sum(), size=num_round)
    boot_weights = idxs.astype(np.float64) / n_battles

    bt_fn = partial(fit_bt, matchups, outcomes, n_models=len(models), alpha=math.log(base), tol=tol)

    if num_cpu == 1:
        results = [bt_fn(w) for w in boot_weights]
    else:
        with mp.Pool(num_cpu) as pool:
            results = list(pool.imap(bt_fn, boot_weights))

    ratings = np.array(results)
    scaled_ratings = scale_and_offset(ratings, models, scale, init_rating)
    df = pd.DataFrame(scaled_ratings, columns=models)
    return df[df.median().sort_values(ascending=False).index]


def compute_bootstrap_bt(
    battles: pd.DataFrame,
    num_round: int = 100,
    base: float = 10.0,
    scale: float = 400.0,
    init_rating: float = 1000.0,
    tol: float = 1e-6,
    num_cpu: int | None = None,
) -> pd.DataFrame:
    """Compute bootstrap sample distributions of Bradley-Terry ratings."""
    return compute_bootstrap_bt_from_data(preprocess_for_bt(battles), num_round, base, scale, init_rating, tol, num_cpu)


def _filter_battles(
    battles: pd.DataFrame,
    filter_func: Callable[[pd.Series], bool],
    langs: list[str],
    exclude_unknown_lang: bool,
    exclude_models: list[str],
    exclude_tie: bool,
) -> pd.DataFrame:
    if filter_func is not _always_true:
        filtered_indices = battles.apply(filter_func, axis=1)
        battles = battles[filtered_indices]
    battles = battles.sort_values(ascending=True, by=["tstamp"])

    if len(langs) > 0:
        battles = battles[battles["language"].isin(langs)]
    if exclude_unknown_lang:
        battles = battles[~battles["language"].str.contains("unknown")]

    battles = battles[~(battles["model_a"].isin(exclude_models) | battles["model_b"].isin(exclude_models))]

    battles = battles[battles["anony"]].reset_index(drop=True)
    if exclude_tie:
        battles = battles[~battles["winner"].str.contains("tie")]
    return battles


def _compute_ranking(
    model_order: list[str],
    model_rating_q025: pd.Series,
    model_rating_q975: pd.Series,
) -> dict[str, int]:
    q025_arr = model_rating_q025[model_order].to_numpy()
    q975_arr = model_rating_q975[model_order].to_numpy()
    strictly_better = q025_arr[None, :] > q975_arr[:, None]
    np.fill_diagonal(strictly_better, False)
    ranks = 1 + strictly_better.sum(axis=1)
    return dict(zip(model_order, ranks.tolist(), strict=True))


def _build_elo_analysis_results(
    bt_data: tuple[np.ndarray, np.ndarray, list[str], np.ndarray],
    num_battles: pd.Series,
    last_updated_tstamp: int,
    elo_rating_online: dict[str, float] | None,
    rating_system: str,
    num_bootstrap: int,
    num_cpu: int | None,
    tol: float,
) -> dict[str, Any]:
    if rating_system != "bt":
        raise NotImplementedError(f"Unsupported rating system: {rating_system}")

    bootstrap_df = compute_bootstrap_bt_from_data(bt_data, num_round=num_bootstrap, num_cpu=num_cpu, tol=tol)
    elo_rating_final = compute_bt_from_data(bt_data, tol=tol)

    model_order = list(elo_rating_final.keys())
    model_rating_q025 = bootstrap_df.quantile(0.025)
    model_rating_q975 = bootstrap_df.quantile(0.975)
    ranking = _compute_ranking(model_order, model_rating_q025, model_rating_q975)

    leaderboard_table_df = pd.DataFrame(
        {
            "rating": elo_rating_final,
            "variance": bootstrap_df.var(),
            "rating_q975": bootstrap_df.quantile(0.975),
            "rating_q025": bootstrap_df.quantile(0.025),
            "num_battles": num_battles,
            "final_ranking": pd.Series(ranking),
        }
    )

    last_updated_datetime = datetime.datetime.fromtimestamp(last_updated_tstamp, tz=ZoneInfo("US/Pacific")).strftime(
        "%Y-%m-%d %H:%M:%S %Z"
    )

    return {
        "rating_system": rating_system,
        "elo_rating_online": elo_rating_online,
        "elo_rating_final": elo_rating_final,
        "elo_rating_median": bootstrap_df.median().to_dict(),
        "last_updated_datetime": last_updated_datetime,
        "last_updated_tstamp": last_updated_tstamp,
        "bootstrap_df": bootstrap_df,
        "leaderboard_table_df": leaderboard_table_df,
    }


def report_elo_analysis_results(
    battles_json: pd.DataFrame | list[dict[str, Any]] | dict[str, Any],
    rating_system: str = "bt",
    num_bootstrap: int = 100,
    exclude_models: list[str] | None = None,
    langs: list[str] | None = None,
    exclude_tie: bool = False,
    exclude_unknown_lang: bool = False,
    daily_vote_per_user: int | None = None,  # Kept for signature compatibility
    run_outlier_detect: bool = False,  # Kept for signature compatibility
    scale: float = 1.0,  # Kept for signature compatibility
    filter_func: Callable[[pd.Series], bool] | None = None,
    style_control: bool = False,  # Kept for signature compatibility
    num_cpu: int | None = None,
    tol: float = 1e-6,
) -> dict[str, Any]:
    """Compute Bradley-Terry / Elo analysis results matching FastChat format."""
    if exclude_models is None:
        exclude_models = []
    if langs is None:
        langs = []
    if filter_func is None:
        filter_func = _always_true

    battles = pd.DataFrame(battles_json)
    battles = _filter_battles(battles, filter_func, langs, exclude_unknown_lang, exclude_models, exclude_tie)

    elo_rating_online = compute_elo(battles)

    return _build_elo_analysis_results(
        bt_data=preprocess_for_bt(battles),
        num_battles=battles["model_a"].value_counts().add(battles["model_b"].value_counts(), fill_value=0),
        last_updated_tstamp=battles["tstamp"].max(),
        elo_rating_online=elo_rating_online,
        rating_system=rating_system,
        num_bootstrap=num_bootstrap,
        num_cpu=num_cpu,
        tol=tol,
    )


def report_elo_analysis_results_from_cells(
    cells: pd.DataFrame,
    last_updated_tstamp: int,
    rating_system: str = "bt",
    num_bootstrap: int = 100,
    num_cpu: int | None = None,
    tol: float = 1e-6,
) -> dict[str, Any]:
    """Like `report_elo_analysis_results`, but for battles already grouped in (model_a, model_b, winner, count) cells.

    It gives the same results without needing the battles one by one, except for the online Elo (it's `None`), as it
    depends on their order.
    """
    cells = cells.groupby(["model_a", "model_b", "winner"], sort=True, as_index=False).agg(count=("count", "sum"))
    num_battles = cells.groupby("model_a")["count"].sum().add(cells.groupby("model_b")["count"].sum(), fill_value=0)
    return _build_elo_analysis_results(
        bt_data=preprocess_cells_for_bt(cells),
        num_battles=num_battles,
        last_updated_tstamp=last_updated_tstamp,
        elo_rating_online=None,
        rating_system=rating_system,
        num_bootstrap=num_bootstrap,
        num_cpu=num_cpu,
        tol=tol,
    )
