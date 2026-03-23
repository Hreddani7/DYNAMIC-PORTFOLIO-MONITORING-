"""
Layer 6B — Interactive Portfolio Stress Simulator (InteliRisk v4.2)
Monte Carlo + HMM regime-based simulation, historical crisis replay,
Fama-French factor classification & stress testing.
Adapted to work with InteliRisk's JSE/African market data pipeline.
"""
import os
import numpy as np
import pandas as pd
from scipy.optimize import minimize

try:
    from hmmlearn.hmm import GaussianHMM
    HAS_HMM = True
except ImportError:
    HAS_HMM = False

try:
    import statsmodels.api as sm
    HAS_SM = True
except ImportError:
    HAS_SM = False

from app.config import N_ANN, DATA_DIR

# ── Factor data paths ──
_FACTOR_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))), "data")
_FF5_PATH = os.path.join(_FACTOR_DIR, "F-F_Research_Data_5_Factors_2x3_daily.csv")
_MOM_PATH = os.path.join(_FACTOR_DIR, "F-F_Momentum_Factor_daily.csv")

# ── Historical crisis periods (for replay) ──
CRISIS_PERIODS = {
    "COVID-19 Crash":         ("2020-02-14", "2020-04-15"),
    "Global Financial Crisis": ("2007-10-01", "2009-03-01"),
    "Dot-Com Bubble":          ("2000-03-01", "2002-10-01"),
    "2011 Eurozone Crisis":    ("2011-07-01", "2011-12-01"),
    "2022 Inflation Crash":    ("2022-01-01", "2022-10-01"),
    "Taper Tantrum (2013)":    ("2013-05-22", "2013-06-24"),
    "China Slowdown (2015)":   ("2015-08-10", "2015-08-24"),
    "Commodity Crash (2014)":  ("2014-06-01", "2016-01-20"),
}

STRESS_MULTIPLIERS = {"1.0x": 1.0, "1.15x": 1.15, "1.25x": 1.25,
                      "1.35x": 1.35, "1.5x": 1.5}

# ── Factor model maps ──
FACTOR_MAP = {
    "FF3": ["Mkt-RF", "SMB", "HML"],
    "FF5": ["Mkt-RF", "SMB", "HML", "RMW", "CMA"],
}
ALL_FACTORS = ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "Mom"]


# ═══════════════════════════════════════════════════════════════════
# PORTFOLIO WEIGHT OPTIMIZATION
# ═══════════════════════════════════════════════════════════════════

def compute_weights(prices_df, method="eq", custom_weights=None,
                    lower_bound=0.0, upper_bound=0.5):
    """Compute portfolio weights from price data.
    method: 'eq' (equal), 'opt' (Sharpe-optimized), 'custom'
    """
    n = len(prices_df.columns)
    if method == "eq":
        return [round(1.0 / n, 4)] * n
    elif method == "custom" and custom_weights is not None:
        return custom_weights
    elif method == "opt":
        log_ret = np.log(prices_df / prices_df.shift(1)).dropna()
        exp_ret = log_ret.mean() * N_ANN
        cov = log_ret.cov() * N_ANN
        rf = 0.045
        w0 = np.repeat(1.0 / n, n)
        bounds = [(lower_bound, upper_bound)] * n
        cons = {"type": "eq", "fun": lambda w: np.sum(w) - 1}
        def neg_sharpe(w):
            pr = w.T @ exp_ret
            ps = np.sqrt(w.T @ cov @ w)
            return -((pr - rf) / ps) if ps > 0 else 0
        res = minimize(neg_sharpe, w0, method="SLSQP",
                       bounds=bounds, constraints=cons)
        return [round(float(x), 4) for x in res.x]
    return [round(1.0 / n, 4)] * n


# ═══════════════════════════════════════════════════════════════════
# RISK METRICS
# ═══════════════════════════════════════════════════════════════════

def calculate_metrics(weights, returns_df):
    """Calculate portfolio risk metrics from returns (log returns).
    Returns dict with metrics + PCR dict + cumulative returns list.
    """
    w = np.array(weights)
    exp_ret = returns_df.mean() * N_ANN
    cov = returns_df.cov() * N_ANN
    rf = 0.045

    port_ret = float(w.T @ exp_ret)
    port_vol = float(np.sqrt(w.T @ cov @ w))
    sharpe = (port_ret - rf) / port_vol if port_vol > 0 else 0

    port_ret_series = (returns_df.values @ w)
    var_95 = float(np.percentile(port_ret_series, 5))
    cvar_95 = float(np.mean(port_ret_series[port_ret_series <= var_95])) if np.any(port_ret_series <= var_95) else var_95

    cum = np.cumprod(1 + port_ret_series)
    peak = np.maximum.accumulate(cum)
    dd = cum / peak - 1
    max_dd = float(np.min(dd))

    # PCR
    pcr = {}
    tickers = list(returns_df.columns)
    for i, t in enumerate(tickers):
        col_vals = returns_df.iloc[:, i].values  # use iloc to avoid duplicate column issues
        t_vol = float(np.std(col_vals) * np.sqrt(N_ANN))
        if len(col_vals) == len(port_ret_series) and len(col_vals) > 1:
            t_corr = float(np.corrcoef(col_vals, port_ret_series)[0, 1])
        else:
            t_corr = 1.0
        if np.isnan(t_corr):
            t_corr = 1.0
        mrc = t_vol * t_corr
        pcr[t] = round(float(w[i] * mrc / port_vol * 100), 2) if port_vol > 0 else 0

    return {
        "annual_volatility": round(port_vol, 4),
        "expected_return": round(port_ret, 4),
        "sharpe_ratio": round(sharpe, 4),
        "var_95": round(var_95, 4),
        "cvar_95": round(cvar_95, 4),
        "max_drawdown": round(max_dd, 4),
        "pcr": pcr,
        "cumulative_returns": [round(float(x), 4) for x in cum.tolist()],
    }


# ═══════════════════════════════════════════════════════════════════
# MONTE CARLO + HMM SIMULATION
# ═══════════════════════════════════════════════════════════════════

def run_monte_carlo(prices_df, weights, n_sims=500, n_days=250,
                    regime="Medium", stress_level="1.0x",
                    factor_stress_means=None):
    """
    Monte Carlo simulation with HMM volatility regime modeling.
    Uses InteliRisk's price data directly (no yfinance).

    Returns dict with simulation results ready for JSON.
    """
    tickers = list(prices_df.columns)
    w = np.array(weights)
    log_ret = np.log(prices_df / prices_df.shift(1)).dropna()

    scaling = STRESS_MULTIPLIERS.get(stress_level, 1.0)

    # Mean matrix
    if factor_stress_means is not None:
        mean_vec = np.array(factor_stress_means[:len(tickers)])
        mean_matrix = np.full((n_days, len(tickers)), mean_vec)
    else:
        mean_vec = log_ret.mean().values
        mean_matrix = np.full((n_days, len(tickers)), mean_vec)

    # HMM for volatility regimes
    port_ret = (log_ret.values @ w).reshape(-1, 1)
    hist_vol = float(np.std(port_ret))

    vol_dict = {"Low": hist_vol * 0.6, "Medium": hist_vol, "High": hist_vol * 1.8}

    if HAS_HMM and len(port_ret) >= 50:
        try:
            model = GaussianHMM(n_components=3, covariance_type="diag",
                                n_iter=1000, random_state=42)
            model.fit(port_ret)
            vol_regimes = np.sort(np.sqrt([v[0][0] for v in model.covars_]))
            vol_dict = {"Low": vol_regimes[0], "Medium": vol_regimes[1],
                        "High": vol_regimes[2]}
        except Exception:
            pass  # fallback to simple scaling

    desired_vol = vol_dict.get(regime, vol_dict["Medium"]) * scaling
    vol_scale = min(desired_vol / hist_vol, 3.0) if hist_vol > 0 else 1.0
    cov_matrix = log_ret.cov().values * (vol_scale ** 2)

    # Ensure PSD
    try:
        L = np.linalg.cholesky(cov_matrix)
    except np.linalg.LinAlgError:
        cov_matrix += np.eye(len(tickers)) * 1e-8
        L = np.linalg.cholesky(cov_matrix)

    # Generate paths
    all_cum_returns = np.zeros((n_sims, n_days))
    all_sim_returns = np.zeros((n_sims, n_days, len(tickers)))

    for m in range(n_sims):
        Z = np.random.normal(size=(n_days, len(tickers)))
        daily_ret = mean_matrix + Z @ L.T
        all_sim_returns[m] = daily_ret
        port_daily = daily_ret @ w
        all_cum_returns[m] = np.cumprod(1 + port_daily)

    # Compute per-path metrics
    path_metrics = []
    for m in range(n_sims):
        sim_df = pd.DataFrame(all_sim_returns[m], columns=tickers)
        pm = calculate_metrics(weights, sim_df)
        path_metrics.append(pm)

    # Find representative, best, worst paths
    sharpes = [pm["sharpe_ratio"] for pm in path_metrics]
    best_idx = int(np.argmax(sharpes))
    worst_idx = int(np.argmin(sharpes))

    # Representative via density (simplified: use median Sharpe)
    median_sharpe = np.median(sharpes)
    rep_idx = int(np.argmin(np.abs(np.array(sharpes) - median_sharpe)))

    # If sklearn available, use KDE for representative
    try:
        from sklearn.neighbors import KernelDensity
        X = np.array([[pm["annual_volatility"], pm["expected_return"],
                        pm["sharpe_ratio"], pm["var_95"], pm["max_drawdown"]]
                       for pm in path_metrics])
        kde = KernelDensity(kernel="gaussian", bandwidth=0.1).fit(X)
        scores = kde.score_samples(X)
        rep_idx = int(np.argmax(scores))
    except Exception:
        pass

    def _path_summary(idx, label):
        pm = path_metrics[idx]
        return {
            "label": label,
            "path_index": idx,
            "metrics": {k: v for k, v in pm.items()
                        if k not in ("pcr", "cumulative_returns")},
            "pcr": pm["pcr"],
            "cumulative_returns": pm["cumulative_returns"],
        }

    # Average PCR across all paths
    avg_pcr = {}
    for t in tickers:
        avg_pcr[t] = round(float(np.mean([pm["pcr"].get(t, 0) for pm in path_metrics])), 2)

    # Portfolio value projections
    final_values = all_cum_returns[:, -1]

    return {
        "type": "Monte Carlo",
        "n_sims": n_sims,
        "n_days": n_days,
        "regime": regime,
        "stress_level": stress_level,
        "hmm_available": HAS_HMM,
        "vol_regimes": {k: round(float(v), 6) for k, v in vol_dict.items()},
        "paths": {
            "representative": _path_summary(rep_idx, "Representative"),
            "best": _path_summary(best_idx, "Best"),
            "worst": _path_summary(worst_idx, "Worst"),
        },
        "avg_pcr": avg_pcr,
        "final_value_stats": {
            "mean": round(float(np.mean(final_values)), 4),
            "median": round(float(np.median(final_values)), 4),
            "p5": round(float(np.percentile(final_values, 5)), 4),
            "p95": round(float(np.percentile(final_values, 95)), 4),
        },
        "all_cumulative": [
            [round(float(x), 4) for x in all_cum_returns[rep_idx]],
            [round(float(x), 4) for x in all_cum_returns[best_idx]],
            [round(float(x), 4) for x in all_cum_returns[worst_idx]],
        ],
        "sharpe_distribution": {
            "mean": round(float(np.mean(sharpes)), 4),
            "std": round(float(np.std(sharpes)), 4),
            "min": round(float(np.min(sharpes)), 4),
            "max": round(float(np.max(sharpes)), 4),
        },
    }


# ═══════════════════════════════════════════════════════════════════
# HISTORICAL CRISIS REPLAY
# ═══════════════════════════════════════════════════════════════════

def run_historical_replay(prices_df, weights, crisis_name):
    """
    Replay a historical crisis using InteliRisk's own data.
    Applies the returns pattern from the crisis period to current prices.
    """
    if crisis_name not in CRISIS_PERIODS:
        return {"error": f"Unknown crisis: {crisis_name}"}

    start_str, end_str = CRISIS_PERIODS[crisis_name]
    start = pd.to_datetime(start_str)
    end = pd.to_datetime(end_str)

    tickers = list(prices_df.columns)
    w = np.array(weights)

    # Try to find crisis period in the data
    crisis_data = prices_df.loc[
        (prices_df.index >= start) & (prices_df.index <= end)
    ].dropna(how="all")

    if len(crisis_data) < 5:
        # Not enough data in crisis period — use synthetic shock
        # based on known drawdowns
        known_dd = {
            "COVID-19 Crash": -0.35,
            "Global Financial Crisis": -0.45,
            "Dot-Com Bubble": -0.30,
            "2011 Eurozone Crisis": -0.15,
            "2022 Inflation Crash": -0.20,
            "Taper Tantrum (2013)": -0.12,
            "China Slowdown (2015)": -0.13,
            "Commodity Crash (2014)": -0.22,
        }
        dd = known_dd.get(crisis_name, -0.20)
        n_days = max(20, int(abs((end - start).days) * 0.7))

        # Generate a drawdown path
        daily_ret = dd / n_days
        noise = np.random.normal(0, abs(daily_ret) * 0.5, (n_days, len(tickers)))
        returns_matrix = np.full((n_days, len(tickers)), daily_ret) + noise
        sim_df = pd.DataFrame(returns_matrix, columns=tickers)
        metrics = calculate_metrics(weights, sim_df)
        cum = np.cumprod(1 + returns_matrix @ w)

        return {
            "type": "Historical Replay",
            "crisis": crisis_name,
            "period": f"{start_str} / {end_str}",
            "data_source": "synthetic_approximation",
            "n_days": n_days,
            "metrics": {k: v for k, v in metrics.items()
                        if k not in ("pcr", "cumulative_returns")},
            "pcr": metrics["pcr"],
            "cumulative_returns": [round(float(x), 4) for x in cum.tolist()],
            "final_value_multiplier": round(float(cum[-1]), 4),
        }

    # Use actual crisis returns
    crisis_ret = np.log(crisis_data / crisis_data.shift(1)).dropna()
    n_days = len(crisis_ret)
    metrics = calculate_metrics(weights, crisis_ret)
    cum = np.cumprod(1 + (crisis_ret.values @ w))

    return {
        "type": "Historical Replay",
        "crisis": crisis_name,
        "period": f"{start_str} / {end_str}",
        "data_source": "historical",
        "n_days": n_days,
        "metrics": {k: v for k, v in metrics.items()
                    if k not in ("pcr", "cumulative_returns")},
        "pcr": metrics["pcr"],
        "cumulative_returns": [round(float(x), 4) for x in cum.tolist()],
        "final_value_multiplier": round(float(cum[-1]), 4),
    }


# ═══════════════════════════════════════════════════════════════════
# FACTOR CLASSIFICATION & STRESS
# ═══════════════════════════════════════════════════════════════════

def _load_factor_data():
    """Load Fama-French 5-factor + Momentum data."""
    ff5 = pd.read_csv(_FF5_PATH, skiprows=3, index_col=0)
    ff5 = ff5.iloc[:-1]  # drop footer
    ff5.index = pd.to_datetime(ff5.index, format="%Y%m%d", errors="coerce")
    ff5 = ff5.dropna(subset=ff5.columns[:1])

    mom = pd.read_csv(_MOM_PATH, skiprows=13, index_col=0)
    mom = mom.iloc[:-1]
    mom.index = pd.to_datetime(mom.index, format="%Y%m%d", errors="coerce")
    mom = mom.dropna(subset=mom.columns[:1])

    # Align
    common = ff5.index.intersection(mom.index)
    ff5 = ff5.loc[common]
    mom = mom.loc[common]
    full = pd.concat([ff5, mom], axis=1)

    # Convert numeric
    for c in full.columns:
        full[c] = pd.to_numeric(full[c], errors="coerce")
    full = full.dropna()

    return full


def classify_and_stress(prices_df, factors_requested, shocks=None):
    """
    Classify portfolio assets via Fama-French factor regression
    and optionally compute stressed expected returns.

    factors_requested: list like ["FF5", "Mom"] or ["Mkt-RF", "SMB", "HML"]
    shocks: dict like {"SMB": 0.2, "Mom": -0.1} (percentage changes)

    Returns dict with classifications, factor betas, and stressed means.
    """
    if not HAS_SM:
        return {"error": "statsmodels not installed"}

    # Expand factor groups
    final_factors = []
    for f in factors_requested:
        if f in FACTOR_MAP:
            final_factors.extend(FACTOR_MAP[f])
        else:
            final_factors.append(f)
    final_factors = list(dict.fromkeys(final_factors))  # dedupe preserving order

    # Load factor data
    try:
        full_factors = _load_factor_data()
    except Exception as e:
        return {"error": f"Failed to load factor data: {e}"}

    # Compute log returns
    log_ret = np.log(prices_df / prices_df.shift(1)).dropna()

    # Align with factor data
    common_idx = log_ret.index.intersection(full_factors.index)
    if len(common_idx) < 30:
        return {"error": f"Insufficient overlapping data ({len(common_idx)} days). Need at least 30."}

    log_ret = log_ret.loc[common_idx]
    rf = full_factors.loc[common_idx, "RF"] / 100
    factors_df = full_factors.loc[common_idx, final_factors] / 100

    tickers = list(prices_df.columns)
    classifications = {}
    regression_results = {}

    for ticker in tickers:
        excess_ret = log_ret[ticker] - rf
        X = sm.add_constant(factors_df)
        model = sm.OLS(excess_ret.astype(float), X.astype(float)).fit()

        alpha = float(model.params["const"])
        betas = {f: round(float(model.params[f]), 4) for f in final_factors}
        r_sq = round(float(model.rsquared), 4)

        regression_results[ticker] = {
            "alpha": round(alpha, 6),
            "betas": betas,
            "r_squared": r_sq,
        }

        # Classification
        cls = []
        for f in final_factors:
            b = betas[f]
            if f == "Mkt-RF":
                cls.append("High-Beta" if b > 1.1 else "Low-Beta" if b < 0.9 else "Normal-Beta")
            elif f == "SMB":
                cls.append("Small-Cap" if b > 0.3 else "Large-Cap" if b < -0.3 else "Mid-Cap")
            elif f == "HML":
                cls.append("Value" if b > 0.3 else "Growth" if b < -0.3 else "Blend")
            elif f == "RMW":
                cls.append("High-Quality" if b > 0.2 else "Low-Quality" if b < -0.2 else "Normal-Quality")
            elif f == "CMA":
                cls.append("Conservative" if b > 0.2 else "Aggressive" if b < -0.2 else "Moderate")
            elif f == "Mom":
                cls.append("High-Momentum" if b > 0.1 else "Low-Momentum" if b < -0.1 else "Mid-Momentum")
        classifications[ticker] = dict(zip(final_factors, cls))

    result = {
        "factors_used": final_factors,
        "n_observations": len(common_idx),
        "classifications": classifications,
        "regression": regression_results,
    }

    # Compute stressed means if shocks provided
    if shocks:
        # Expand FF3/FF5 shocks
        expanded_shocks = {}
        for f in final_factors:
            expanded_shocks[f] = 0
        for key, val in shocks.items():
            if key in FACTOR_MAP:
                if isinstance(val, list):
                    for i, ff in enumerate(FACTOR_MAP[key]):
                        if i < len(val):
                            expanded_shocks[ff] = val[i]
                else:
                    for ff in FACTOR_MAP[key]:
                        expanded_shocks[ff] = val
            else:
                expanded_shocks[key] = val

        factor_means = factors_df.mean()
        stressed_means = {}
        for ticker in tickers:
            alpha = regression_results[ticker]["alpha"]
            betas = regression_results[ticker]["betas"]
            sm_val = alpha
            for f in final_factors:
                sm_val += betas[f] * float(factor_means[f]) * (1 + expanded_shocks.get(f, 0))
            stressed_means[ticker] = round(float(sm_val), 8)

        result["stressed_means"] = stressed_means
        result["shocks_applied"] = expanded_shocks

    return result


# ═══════════════════════════════════════════════════════════════════
# BASELINE METRICS (from historical data)
# ═══════════════════════════════════════════════════════════════════

def compute_baseline_metrics(prices_df, weights):
    """Compute baseline risk metrics from historical price data."""
    log_ret = np.log(prices_df / prices_df.shift(1)).dropna()
    return calculate_metrics(weights, log_ret)


# ═══════════════════════════════════════════════════════════════════
# KDE DISTRIBUTION DATA (for multi-scenario comparison)
# ═══════════════════════════════════════════════════════════════════

def compute_kde_data(scenarios_metrics, metric_name="annual_volatility"):
    """
    Compute KDE curves for a given metric across multiple scenarios.
    Returns data points for frontend Chart.js rendering.
    """
    try:
        from scipy.stats import gaussian_kde
    except ImportError:
        return {"error": "scipy not installed"}

    all_vals = []
    for data in scenarios_metrics.values():
        if isinstance(data, list):
            all_vals.extend(data)
        else:
            all_vals.append(data)

    if not all_vals:
        return {"error": "No metric data available"}

    x_min = min(all_vals)
    x_max = max(all_vals)
    pad = (x_max - x_min) * 0.1 if x_max != x_min else 0.1
    x_range = np.linspace(x_min - pad, x_max + pad, 100)

    kde_curves = {}
    for name, data in scenarios_metrics.items():
        if isinstance(data, list) and len(data) > 1:
            kde = gaussian_kde(data)
            kde_curves[name] = {
                "type": "curve",
                "x": [round(float(v), 6) for v in x_range],
                "y": [round(float(v), 6) for v in kde(x_range)],
            }
        else:
            val = data[0] if isinstance(data, list) else data
            kde_curves[name] = {"type": "vline", "x": round(float(val), 6)}

    return {"metric": metric_name, "curves": kde_curves}
