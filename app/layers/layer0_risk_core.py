"""
Layer 0 — Portfolio Risk Core (InteliRisk v4 methodology)
- Rolling volatility: 21d/63d/126d windows (Basel III + Schwert 1989)
- HAR-RV: Heterogeneous Autoregressive Realized Volatility
- VaR suite: Historical, Parametric, Cornish-Fisher, EVT/GPD
- CVaR (Expected Shortfall)
- Max drawdown with recovery tracking
- Component VaR decomposition + Herfindahl
"""
import numpy as np
import pandas as pd
from scipy.stats import norm, jarque_bera, genpareto
from scipy.stats import kurtosis as sp_kurt, skew as sp_skew

from app.config import N_ANN, WIN_SHORT, WIN_MED, WIN_LONG, WIN_VAR, MARKETS


def rolling_vol(r, win):
    mp = max(min(win // 2, len(r) // 2), 2)
    return r.rolling(win, min_periods=mp).std() * np.sqrt(N_ANN)


def har_rv(r):
    """HAR-RV: exploits multi-scale memory (daily/weekly/monthly) in squared returns."""
    rv = r.values ** 2
    rvs = pd.Series(rv)
    rw5 = rvs.rolling(5, min_periods=3).mean().values
    rw22 = rvs.rolling(22, min_periods=10).mean().values
    y = rv[1:]
    X = np.c_[np.ones(len(rv) - 1), rv[:-1], rw5[:-1], rw22[:-1]]
    ok = np.isfinite(y) & np.isfinite(X).all(1)
    if ok.sum() < 30:
        return {"r2": 0, "vol_ann": pd.Series(dtype=float), "fc_ann": 0, "b": np.zeros(4)}
    b = np.linalg.lstsq(X[ok], y[ok], rcond=None)[0]
    ss = ((y[ok] - X[ok] @ b) ** 2).sum()
    r2 = 1 - ss / ((y[ok] - y[ok].mean()) ** 2).sum()
    fit = np.maximum(np.r_[rv[0], (X @ b)], 1e-12)
    fc = max(b[0] + b[1] * rv[-1] + b[2] * np.nanmean(rv[-5:]) + b[3] * np.nanmean(rv[-22:]), 1e-12)
    return {
        "r2": float(r2),
        "vol_ann": pd.Series(np.sqrt(fit) * np.sqrt(N_ANN), index=r.index),
        "fc_ann": float(np.sqrt(fc) * np.sqrt(N_ANN)),
        "b": b,
    }


def hist_var_cvar(ra, alpha=0.05, win=WIN_VAR):
    """Rolling historical VaR and CVaR (Expected Shortfall)."""
    n = len(ra)
    v = np.full(n, np.nan)
    cv = np.full(n, np.nan)
    # Adapt window to data length
    eff_win = min(win, max(n - 1, 5))
    start = max(eff_win, 5)
    for t in range(start, n):
        w = min(eff_win, t)
        L = -np.sort(ra[t - w:t])[::-1]
        k = max(int(np.ceil(alpha * w)), 1)
        v[t] = L[min(k - 1, len(L) - 1)]
        cv[t] = L[:k].mean()
    # If data too short, compute a single VaR from all available data
    if np.all(np.isnan(v)) and n >= 5:
        L = -np.sort(ra)[::-1]
        k = max(int(np.ceil(alpha * n)), 1)
        v[-1] = L[min(k - 1, len(L) - 1)]
        cv[-1] = L[:k].mean()
    return v, cv


def param_var(r, alpha=0.05, win=WIN_VAR):
    """Parametric (Gaussian) VaR."""
    r = pd.Series(r)
    mp = max(min(win // 2, len(r) // 2), 2)
    mu = r.rolling(win, min_periods=mp).mean()
    sg = r.rolling(win, min_periods=mp).std()
    return -(mu + norm.ppf(alpha) * sg)


def cf_var(r, alpha=0.05, win=WIN_VAR):
    """Cornish-Fisher VaR — adjusts for skewness and kurtosis."""
    r = pd.Series(r)
    n = len(r)
    v = np.full(n, np.nan)
    eff_win = min(win, max(n - 1, 5))
    for t in range(eff_win, n):
        s = r.iloc[max(0, t - eff_win):t].values
        mu, sg = s.mean(), s.std(ddof=1)
        if sg == 0:
            continue
        z = norm.ppf(alpha)
        sk = sp_skew(s)
        ku = sp_kurt(s)
        zcf = z + (z ** 2 - 1) * sk / 6 + (z ** 3 - 3 * z) * ku / 24 - (2 * z ** 3 - 5 * z) * sk ** 2 / 36
        v[t] = -(mu + zcf * sg)
    return v


def rolling_dd(r_pct):
    """Rolling drawdown from log returns (in % units).
    Converts log returns back to cumulative level via exp(cumsum).
    """
    cum = np.exp(np.cumsum(r_pct.values / 100))
    pk = pd.Series(cum).cummax().values
    return (cum / pk - 1) * 100


def evt_gpd(ra, alpha=0.05, tail_q=0.10):
    """EVT/GPD tail model — fits Generalized Pareto to extreme losses."""
    L = -ra[ra < 0]
    if len(L) < 50:
        return np.nan, np.nan
    u = np.quantile(L, 1 - tail_q)
    exc = L[L > u] - u
    if len(exc) < 20:
        return np.nan, np.nan
    try:
        xi, _, beta = genpareto.fit(exc, floc=0)
        pe = len(exc) / len(L)
        v = u + (beta / xi) * ((alpha / pe) ** (-xi) - 1) if xi != 0 else u - beta * np.log(alpha / pe)
        cv = (v + beta - xi * u) / (1 - xi)
        return float(v), float(cv)
    except Exception:
        return np.nan, np.nan


def component_var(rpanel, alpha=0.05, win=WIN_VAR):
    """Component VaR — each market's contribution to portfolio VaR."""
    mkts = rpanel.columns.tolist()
    n = len(mkts)
    w = np.ones(n) / n
    eff_win = min(win, len(rpanel))
    Sg = rpanel.iloc[-eff_win:].cov().values
    pv = np.sqrt(w @ Sg @ w) * norm.ppf(1 - alpha) * np.sqrt(N_ANN)
    mg = (Sg @ w) / (pv / (norm.ppf(1 - alpha) * np.sqrt(N_ANN)) + 1e-12)
    comp = w * mg
    pct = comp / (comp.sum() + 1e-12) * 100
    return {mkts[i]: round(float(pct[i]), 2) for i in range(n)}


def compute_layer0(prices, holdings):
    """Full Layer 0 computation with InteliRisk v4 methods."""
    # Build per-market returns
    ret_all = (np.log(prices / prices.shift(1)) * 100).dropna(how="all")
    # Adaptive: use markets with at least 10 data points (not 100)
    min_pts = min(100, max(10, len(ret_all) // 2))
    markets_use = [c for c in ret_all.columns if ret_all[c].dropna().shape[0] >= min_pts]
    # Fallback: if nothing qualifies, use all columns with any data
    if not markets_use:
        markets_use = [c for c in ret_all.columns if ret_all[c].dropna().shape[0] >= 5]
    ret = ret_all[markets_use].copy()

    # Per-market metrics
    roll_v = {}
    har_results = {}
    risk_metrics = {}

    for mkt in markets_use:
        r = ret[mkt].dropna()
        rv = {
            "21d": rolling_vol(r, WIN_SHORT),
            "63d": rolling_vol(r, WIN_MED),
            "126d": rolling_vol(r, WIN_LONG),
        }
        roll_v[mkt] = rv
        hr = har_rv(r)
        har_results[mkt] = hr
        ra = r.values
        vh, cvh = hist_var_cvar(ra)
        vp = param_var(r).values
        vcf = cf_var(r)
        dd = rolling_dd(r)
        ev_v, ev_cv = evt_gpd(ra)

        def cur(s):
            s2 = pd.Series(s).dropna()
            return float(s2.iloc[-1]) if len(s2) > 0 else np.nan

        # Jarque-Bera normality test
        jb_stat, jb_p = jarque_bera(r.dropna())

        risk_metrics[mkt] = {
            "var_hist": vh, "cvar_hist": cvh,
            "var_param": vp, "var_cf": vcf,
            "drawdown": dd, "max_dd": float(dd.min()),
            "evt_var": ev_v, "evt_cvar": ev_cv,
            "idx": r.index,
            "vol_21d": cur(rv["21d"]),
            "vol_63d": cur(rv["63d"]),
            "vol_126d": cur(rv["126d"]),
            "har_fc": hr["fc_ann"],
            "har_r2": hr["r2"],
            "current_var": cur(vh),
            "current_cvar": cur(cvh),
            "jb_p": float(jb_p),
            "skewness": float(sp_skew(r.dropna())),
            "kurtosis": float(sp_kurt(r.dropna())),
            "mean_ret": float(r.mean()),
            "std_ret": float(r.std()),
        }

    # Component VaR (uses actual portfolio weights if available)
    ret_panel = ret[markets_use].dropna(how="all")
    comp = component_var(ret_panel)
    herf = sum(v ** 2 for v in comp.values()) / (sum(comp.values()) ** 2 + 1e-12) * (len(markets_use) ** 2)

    # Build portfolio-level time series using ACTUAL weights from holdings
    total_pf_val = sum(abs(h.get("market_value", 0)) for h in holdings) or 1
    # Map holdings to their column weights
    weight_map = {}
    for h in holdings:
        ticker = h.get("asset_id", "").split(".")[0].upper()
        if ticker in markets_use:
            weight_map[ticker] = abs(h.get("market_value", 0)) / total_pf_val
    # If we have weights, use them; otherwise equal weight
    if weight_map and any(k in markets_use for k in weight_map):
        weights = np.array([weight_map.get(m, 1.0 / len(markets_use)) for m in markets_use])
        weights = weights / weights.sum()  # normalize
    else:
        weights = np.ones(len(markets_use)) / len(markets_use)

    port_ret = (ret[markets_use].fillna(0) * weights).sum(axis=1)
    cumulative = (1 + port_ret / 100).cumprod()
    dd_port = rolling_dd(port_ret)

    n_ts = min(500, len(port_ret))
    ts_dates = [str(d.date()) if hasattr(d, 'date') else str(d) for d in port_ret.tail(n_ts).index]

    # Per-stock/market summary for frontend
    market_summary = {}
    for mkt in markets_use:
        rm = risk_metrics[mkt]
        # Try to get a display name — for stock tickers, show ticker; for market keys, use MARKETS
        display_name = MARKETS.get(mkt, {}).get("name", mkt)
        display_short = MARKETS.get(mkt, {}).get("short", mkt[:6])
        market_summary[mkt] = {
            "name": display_name,
            "short": display_short,
            "vol_21d": round(rm["vol_21d"], 2) if not np.isnan(rm["vol_21d"]) else 0,
            "vol_63d": round(rm["vol_63d"], 2) if not np.isnan(rm["vol_63d"]) else 0,
            "vol_126d": round(rm["vol_126d"], 2) if not np.isnan(rm["vol_126d"]) else 0,
            "har_forecast": round(rm["har_fc"], 2),
            "har_r2": round(rm["har_r2"], 3),
            "max_dd": round(rm["max_dd"], 2),
            "current_var": round(rm["current_var"], 3) if not np.isnan(rm["current_var"]) else 0,
            "current_cvar": round(rm["current_cvar"], 3) if not np.isnan(rm["current_cvar"]) else 0,
            "evt_var": round(rm["evt_var"], 3) if not np.isnan(rm["evt_var"]) else None,
            "evt_cvar": round(rm["evt_cvar"], 3) if not np.isnan(rm["evt_cvar"]) else None,
            "jb_p": round(rm["jb_p"], 5),
            "non_normal": rm["jb_p"] < 0.05,
            "skewness": round(rm["skewness"], 3),
            "kurtosis": round(rm["kurtosis"], 3),
            "mean_ret": round(rm["mean_ret"], 4),
            "component_var_pct": comp.get(mkt, 0),
        }

    # Rolling vol time series for charts
    vol_ts = {}
    for mkt in markets_use:
        v21 = roll_v[mkt]["21d"].dropna()
        vol_ts[mkt] = {
            "dates": [str(d.date()) if hasattr(d, 'date') else str(d) for d in v21.tail(n_ts).index],
            "vol_21d": [round(float(v), 2) for v in roll_v[mkt]["21d"].tail(n_ts).fillna(0).values],
            "vol_63d": [round(float(v), 2) for v in roll_v[mkt]["63d"].tail(n_ts).fillna(0).values],
            "vol_126d": [round(float(v), 2) for v in roll_v[mkt]["126d"].tail(n_ts).fillna(0).values],
        }

    # VaR time series
    var_ts = {}
    for mkt in markets_use:
        idx = risk_metrics[mkt]["idx"]
        n2 = min(n_ts, len(idx))
        var_ts[mkt] = {
            "dates": [str(d.date()) if hasattr(d, 'date') else str(d) for d in idx[-n2:]],
            "var_hist": [round(float(v), 3) if not np.isnan(v) else None for v in risk_metrics[mkt]["var_hist"][-n2:]],
            "cvar_hist": [round(float(v), 3) if not np.isnan(v) else None for v in risk_metrics[mkt]["cvar_hist"][-n2:]],
            "var_cf": [round(float(v), 3) if not np.isnan(v) else None for v in risk_metrics[mkt]["var_cf"][-n2:]],
        }

    # ── Per-holding risk metrics (when stock price data available) ─────
    holding_risk = []
    total_pf_value = sum(abs(h.get("market_value", 0)) for h in holdings) or 1
    for h in holdings:
        ps = h.get("_price_series")
        ticker = h.get("asset_id", "")
        mv = h.get("market_value", 0)
        weight = abs(mv) / total_pf_value

        if ps is not None and len(ps) >= 20:
            r_stk = (np.log(ps / ps.shift(1)) * 100).dropna()
            v21 = rolling_vol(r_stk, min(WIN_SHORT, len(r_stk) - 1))
            cur_vol = float(v21.dropna().iloc[-1]) if len(v21.dropna()) > 0 else 0
            ra_stk = r_stk.values
            vh_s, cvh_s = hist_var_cvar(ra_stk, win=min(WIN_VAR, len(ra_stk) - 1))
            dd_s = rolling_dd(r_stk)
            cur_var = float(pd.Series(vh_s).dropna().iloc[-1]) if pd.Series(vh_s).dropna().shape[0] > 0 else 0
            cur_cvar = float(pd.Series(cvh_s).dropna().iloc[-1]) if pd.Series(cvh_s).dropna().shape[0] > 0 else 0
            holding_risk.append({
                "asset_id": ticker, "market": h.get("market", ""),
                "sector": h.get("sector", ""), "market_value": round(mv, 2),
                "weight": round(weight, 4), "data_matched": True,
                "vol_21d": round(cur_vol, 2),
                "var_5pct": round(cur_var, 3),
                "cvar_5pct": round(cur_cvar, 3),
                "max_dd": round(float(dd_s.min()), 2),
                "data_points": len(r_stk),
            })
        else:
            holding_risk.append({
                "asset_id": ticker, "market": h.get("market", ""),
                "sector": h.get("sector", ""), "market_value": round(mv, 2),
                "weight": round(weight, 4), "data_matched": False,
                "vol_21d": 0, "var_5pct": 0, "cvar_5pct": 0, "max_dd": 0,
                "data_points": 0,
            })

    # ── Portfolio-level risk from weighted returns ──────────────
    pf_vol = rolling_vol(port_ret, min(WIN_SHORT, max(len(port_ret) - 1, 2)))
    pf_vol_cur = float(pf_vol.dropna().iloc[-1]) if len(pf_vol.dropna()) > 0 else 0
    pf_har = har_rv(port_ret)
    pf_ra = port_ret.values
    pf_vh, pf_cvh = hist_var_cvar(pf_ra)
    pf_var_cur = float(pd.Series(pf_vh).dropna().iloc[-1]) if pd.Series(pf_vh).dropna().shape[0] > 0 else 0
    pf_cvar_cur = float(pd.Series(pf_cvh).dropna().iloc[-1]) if pd.Series(pf_cvh).dropna().shape[0] > 0 else 0

    return {
        "markets_used": markets_use,
        "market_summary": market_summary,
        "holding_risk": holding_risk,
        "component_var": comp,
        "herfindahl": round(herf, 4),
        "herfindahl_level": "HIGH" if herf > 0.30 else "MODERATE" if herf > 0.22 else "LOW",
        "portfolio": {
            "mean_vol_21d": round(pf_vol_cur, 2),
            "mean_var": round(pf_var_cur, 3),
            "mean_cvar": round(pf_cvar_cur, 3),
            "har_forecast": round(pf_har["fc_ann"], 2),
            "worst_dd": round(float(dd_port.min()), 2),
            "n_markets": len(markets_use),
            "n_holdings": len(holdings),
        },
        "time_series": {
            "dates": ts_dates,
            "returns": [round(float(v), 4) for v in port_ret.tail(n_ts).values],
            "cumulative": [round(float(v), 6) for v in cumulative.tail(n_ts).values],
            "drawdown": [round(float(v), 4) for v in dd_port[-n_ts:]],
        },
        "vol_time_series": vol_ts,
        "var_time_series": var_ts,
        "_internal": {"ret": ret, "roll_v": roll_v, "risk_metrics": risk_metrics, "markets_use": markets_use},
    }
