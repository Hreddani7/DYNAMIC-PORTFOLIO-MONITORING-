"""
Layer 3 — African Factor Model (InteliRisk v4.1)
6 factors computed from multi-market data:
  F1 Global Risk     — PCA PC1 across 4 markets + VIX signal
  F2 USD Liquidity   — DXY + all available African FX pairs (USDZAR, USDMAD, ...)
  F3 Commodity Shock — Equal-weight Brent + Gold + Copper + Platinum
  F4 Sovereign Stress— Mean daily yield-spread change (country 10Y - US 10Y) × 4 markets
  F5 Domestic Sector — Bank basket return minus mining/commodity basket per market
  F6 Herding (CSAD)  — Cross-Sectional Absolute Deviation per market on all stocks

All factors are converted to 63-day rolling z-scores before use.
"""
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression

from app.config import FACTOR_META, FACTOR_COLS, HERD_WIN, H_MOD, H_STRONG, MARKETS


def _aln(s, idx):
    """Align a Series to idx with forward/back fill."""
    if s is None or len(s) == 0:
        return pd.Series(0.0, index=idx)
    if s.index.duplicated().any():
        s = s.groupby(s.index).mean()
    return s.reindex(idx).ffill().bfill().fillna(0.0)


def _safe_ret(s, idx):
    """Compute log % returns from a price series, aligned to idx."""
    s2 = _aln(s, idx)
    r = np.log(s2.replace(0, np.nan) / s2.replace(0, np.nan).shift(1)) * 100
    return r.fillna(0.0)


def _yield_diff(s, idx):
    """Daily change in a yield series (already in % units), aligned to idx."""
    if s is None or len(s) == 0:
        return pd.Series(0.0, index=idx)
    if s.index.duplicated().any():
        s = s.groupby(s.index).mean()
    return s.reindex(idx).ffill().bfill().diff().fillna(0.0)


def compute_csad(sdict, min_stk=3):
    """
    CSAD herding test (Chang, Cheng & Khorana 2000).
    sdict = {symbol: price_Series} — price LEVELS, not returns.
    gamma2 < 0 (and significant) indicates herding.
    """
    null = {
        "H": np.nan, "gamma_2": np.nan, "t_g2": np.nan,
        "sig": False, "level": "N/A", "n_stocks": 0, "roll_H": [],
    }
    # Filter to stocks with enough data for meaningful CSAD computation
    good_stocks = {k: v for k, v in sdict.items() if len(v) >= 100}
    if len(good_stocks) < min_stk:
        good_stocks = sdict  # Fall back to all if not enough long ones
    if len(good_stocks) < min_stk:
        return pd.Series(dtype=float), null

    panel = pd.DataFrame(good_stocks).replace(0, np.nan).dropna(how="all")
    log_ret = np.log(panel / panel.shift(1)) * 100
    log_ret = log_ret.replace([np.inf, -np.inf], np.nan).dropna(how="any")

    ri = log_ret
    rm = ri.mean(axis=1)
    csad = ri.sub(rm, axis=0).abs().mean(axis=1)
    cmn = rm.dropna().index.intersection(csad.dropna().index)
    if len(cmn) < 30:
        return csad, null

    Rm = rm.loc[cmn].values
    Cs = csad.loc[cmn].values
    ok = np.isfinite(Rm) & np.isfinite(Cs)
    if ok.sum() < 30:
        return csad, null

    Rm, Cs = Rm[ok], Cs[ok]
    X = np.c_[np.ones(len(Rm)), np.abs(Rm), Rm ** 2]
    try:
        b = np.linalg.lstsq(X, Cs, rcond=None)[0]
    except np.linalg.LinAlgError:
        return csad, null

    res = Cs - X @ b
    denom = len(Cs) - 3
    if denom <= 0:
        return csad, null
    cov = (res ** 2).sum() / denom * np.linalg.pinv(X.T @ X) + 1e-12 * np.eye(3)
    se = np.sqrt(np.maximum(np.diag(cov), 0))
    g2, t_g2, H = b[2], b[2] / (se[2] + 1e-12), -b[2]
    level = "Strong" if H >= H_STRONG else "Moderate" if H >= H_MOD else "No Herding"

    roll_H = []
    for t in range(HERD_WIN, len(cmn)):
        wi = cmn[t - HERD_WIN:t]
        Rw = rm.loc[wi].values
        Cw = csad.loc[wi].values
        Xw = np.c_[np.ones(HERD_WIN), np.abs(Rw), Rw ** 2]
        try:
            bw = np.linalg.lstsq(Xw, Cw, rcond=None)[0]
            roll_H.append({
                "date": str(cmn[t].date()) if hasattr(cmn[t], "date") else str(cmn[t]),
                "H": round(float(-bw[2]), 5),
            })
        except Exception:
            pass

    return csad, {
        "H": round(float(H), 5),
        "gamma_2": round(float(g2), 6),
        "t_g2": round(float(t_g2), 3),
        "sig": bool((g2 < 0) and (abs(t_g2) > 1.96)),
        "level": level,
        "n_stocks": len(sdict),
        "roll_H": roll_H,
    }


def classify_shock(brent_r, comm_r, usd_r, win=21):
    """Classify macro shocks: DEMAND / SUPPLY / FINANCIAL / MIXED."""
    labels = []
    idx_s = brent_r.index
    for t in range(win, len(idx_s)):
        b = brent_r.iloc[t - win:t].mean()
        cm = comm_r.iloc[t - win:t].mean()
        ud = usd_r.iloc[t - win:t].mean()
        if abs(ud) > max(abs(b), abs(cm)) * 0.8:
            labels.append("FINANCIAL")
        elif abs(b - cm) > abs(b + cm + 1e-9) * 0.5:
            labels.append("SUPPLY")
        elif abs(b + cm) > 0.1:
            labels.append("DEMAND")
        else:
            labels.append("MIXED")
    return pd.Series(labels, index=idx_s[win:])


def compute_layer3(prices, holdings, l2_internal=None):
    """Full Layer 3 — 6-factor model + CSAD herding across all 4 markets."""
    from app.ingestion import generate_stocks_per_market, generate_macro

    stocks_per_market = generate_stocks_per_market()
    macro = generate_macro()

    # Build clean market returns
    prices_clean = prices.replace(0, np.nan)
    ret_all = (np.log(prices_clean / prices_clean.shift(1)) * 100)
    ret_all = ret_all.replace([np.inf, -np.inf], np.nan).dropna(how="all")
    min_pts = min(100, max(10, len(ret_all) // 2))
    markets_use = [c for c in ret_all.columns if ret_all[c].dropna().shape[0] >= min_pts]
    if not markets_use:
        markets_use = [c for c in ret_all.columns if ret_all[c].dropna().shape[0] >= 5]
    ret_clean = ret_all[markets_use].ffill().dropna(how="all")
    common_idx = ret_clean.index

    fac_df = pd.DataFrame(index=common_idx)

    # ── F1: GLOBAL RISK — PCA PC1 + VIX signal ────────────────
    if l2_internal and "pca_obj" in l2_internal and "ret_clean" in l2_internal:
        # Use the stock-level returns from Layer 2 (matching PCA dimensions)
        l2_ret = l2_internal["ret_clean"]
        l2_aligned = l2_ret.reindex(common_idx).ffill().fillna(0.0)
        sc2 = StandardScaler().fit_transform(l2_aligned.values)
        f1_raw = pd.Series(
            l2_internal["pca_obj"].transform(sc2)[:, 0],
            index=common_idx,
        )
    elif l2_internal and "pca_obj" in l2_internal:
        # Fallback: PCA trained on same-dimension data as prices
        rc_full = ret_clean.reindex(common_idx).ffill().fillna(0.0)
        if rc_full.shape[1] == l2_internal["pca_obj"].n_features_in_:
            sc2 = StandardScaler().fit_transform(rc_full.values)
            f1_raw = pd.Series(
                l2_internal["pca_obj"].transform(sc2)[:, 0],
                index=common_idx,
            )
        else:
            f1_raw = ret_clean.mean(axis=1)
    else:
        f1_raw = ret_clean.mean(axis=1)

    # Augment with VIX: risk-off = VIX up = F1 should spike
    vix_series = macro.get("vix")
    if vix_series is not None:
        vix_r = _safe_ret(vix_series, common_idx)
        fac_df["F1_Global"] = (0.7 * f1_raw.reindex(common_idx).fillna(0.0)
                               + 0.3 * vix_r)
    else:
        fac_df["F1_Global"] = f1_raw.reindex(common_idx).fillna(0.0)

    # ── F2: USD LIQUIDITY — DXY + all available African FX pairs ──
    fx_candidates = ["dxy", "USDZAR", "USDMAD", "USDEGP", "USDNGN"]
    fx_rets = []
    for fn in fx_candidates:
        s = macro.get(fn)
        if s is not None:
            fx_rets.append(_safe_ret(s, common_idx))
    if fx_rets:
        fac_df["F2_USD"] = pd.concat(fx_rets, axis=1).mean(axis=1)
    else:
        fac_df["F2_USD"] = 0.0

    # ── F3: COMMODITY SHOCK — equal-weight Brent/Gold/Copper/Platinum ─
    comm_candidates = ["oil", "gold", "copper", "platinum"]
    comm_rets = []
    brent_r = pd.Series(0.0, index=common_idx)  # keep for shock classification
    for cn in comm_candidates:
        s = macro.get(cn)
        if s is not None:
            cr = _safe_ret(s, common_idx)
            comm_rets.append(cr)
            if cn == "oil":
                brent_r = cr
    if comm_rets:
        fac_df["F3_Commodity"] = pd.concat(comm_rets, axis=1).mean(axis=1)
    else:
        fac_df["F3_Commodity"] = 0.0

    # ── F4: SOVEREIGN STRESS — yield spread changes vs US 10Y ─────
    # Spread = (country yield - US 10Y). Daily change in spread.
    us10y = macro.get("yield_US")
    us10y_chg = _yield_diff(us10y, common_idx) if us10y is not None else pd.Series(0.0, index=common_idx)

    sov_spread_changes = []
    for yn in ["yield_SA", "yield_MA", "yield_EG", "yield_NG"]:
        s = macro.get(yn)
        if s is not None:
            country_chg = _yield_diff(s, common_idx)
            spread_chg = country_chg - us10y_chg
            sov_spread_changes.append(spread_chg)
    if sov_spread_changes:
        fac_df["F4_Sovereign"] = pd.concat(sov_spread_changes, axis=1).mean(axis=1)
    else:
        fac_df["F4_Sovereign"] = 0.0

    # ── F5: DOMESTIC SECTOR — bank vs mining/commodity spread ─────
    # Per market: mean(banking stocks returns) - mean(commodity/mining stocks returns)
    sector_spreads = []

    # JSE: use JSE Banks Index vs JSE Mining Index (most direct)
    jse = stocks_per_market.get("JSE_SA", {})
    jse_banks_syms = ["FirstRand", "StandardBank", "JSEBanks"]
    jse_mining_syms = ["AngloAmerican", "ImpalaPlatinum", "Sasol", "JSEMining"]
    jse_b = [_safe_ret(jse[k], common_idx) for k in jse_banks_syms if k in jse]
    jse_m = [_safe_ret(jse[k], common_idx) for k in jse_mining_syms if k in jse]
    if jse_b and jse_m:
        sector_spreads.append(
            pd.concat(jse_b, axis=1).mean(axis=1)
            - pd.concat(jse_m, axis=1).mean(axis=1)
        )

    # MASI: banking (CFGBank) vs commodity (Managem=phosphate, TaqaMorocco=energy)
    masi = stocks_per_market.get("MASI_MA", {})
    masi_b = [_safe_ret(masi[k], common_idx) for k in ["CFGBank"] if k in masi]
    masi_m = [_safe_ret(masi[k], common_idx) for k in ["Managem", "CimentsDuMaroc", "TaqaMorocco"] if k in masi]
    if masi_b and masi_m:
        sector_spreads.append(
            pd.concat(masi_b, axis=1).mean(axis=1)
            - pd.concat(masi_m, axis=1).mean(axis=1)
        )

    # EGX: banking (CIB) vs industrials/energy (ElSewedy, EasternTobacco)
    egx = stocks_per_market.get("EGX_EG", {})
    egx_b = [_safe_ret(egx[k], common_idx) for k in ["CIB"] if k in egx]
    egx_m = [_safe_ret(egx[k], common_idx) for k in ["ElSewedy", "EasternTobacco", "TelecomEgypt"] if k in egx]
    if egx_b and egx_m:
        sector_spreads.append(
            pd.concat(egx_b, axis=1).mean(axis=1)
            - pd.concat(egx_m, axis=1).mean(axis=1)
        )

    # NGX: banking (GTH, Zenith) vs commodity/telecom (Dangote, Seplat)
    ngx = stocks_per_market.get("NGX_NG", {})
    ngx_b = [_safe_ret(ngx[k], common_idx) for k in ["GTH", "Zenith"] if k in ngx]
    ngx_m = [_safe_ret(ngx[k], common_idx) for k in ["Dangote", "Seplat"] if k in ngx]
    if ngx_b and ngx_m:
        sector_spreads.append(
            pd.concat(ngx_b, axis=1).mean(axis=1)
            - pd.concat(ngx_m, axis=1).mean(axis=1)
        )

    if sector_spreads:
        fac_df["F5_Domestic"] = pd.concat(sector_spreads, axis=1).mean(axis=1)
    else:
        # Fallback: rolling std of FX volatility (original logic)
        fx_cols = [c for c in macro.columns if "USD" in c] if hasattr(macro, "columns") else []
        if fx_cols:
            fac_df["F5_Domestic"] = (
                macro[fx_cols].reindex(common_idx).ffill()
                .pct_change().std(axis=1).rolling(20).mean().fillna(0)
            )
        else:
            fac_df["F5_Domestic"] = 0.0

    # ── F6: HERDING (CSAD) — per market on all individual stocks ──
    # Compute CSAD for each market that has >= 3 stocks,
    # then aggregate by averaging CSAD z-scores.
    csad_all = []
    herd_per_market = {}
    best_herd = {"H": np.nan, "gamma_2": np.nan, "t_g2": np.nan,
                 "sig": False, "level": "N/A", "n_stocks": 0, "roll_H": []}

    for mkt, sdict in stocks_per_market.items():
        if len(sdict) >= 3:
            csad_s, herd_r = compute_csad(sdict, min_stk=3)
            herd_per_market[mkt] = herd_r
            if len(csad_s) > 30:
                csad_all.append(csad_s.rename(mkt))
            # Track most informative (most stocks)
            if herd_r.get("n_stocks", 0) > best_herd.get("n_stocks", 0):
                best_herd = herd_r

    if csad_all:
        combined_csad = pd.concat(csad_all, axis=1).ffill().mean(axis=1)
        csad_z = ((combined_csad - combined_csad.rolling(252).mean())
                  / (combined_csad.rolling(252).std() + 1e-8))
        fac_df["F6_Herding"] = csad_z.reindex(common_idx).ffill().fillna(0.0)
    else:
        # Fallback: use market index prices for CSAD
        price_proxy = {m: prices[m].dropna() for m in markets_use if m in prices.columns}
        csad_series, best_herd = compute_csad(price_proxy, min_stk=2)
        csad_z = ((csad_series - csad_series.rolling(252).mean())
                  / (csad_series.rolling(252).std() + 1e-8))
        fac_df["F6_Herding"] = csad_z.reindex(common_idx).ffill().fillna(0.0)

    # ── Forward-fill → ensure dense factor series ─────────────
    fac_df = fac_df.ffill().bfill().fillna(0.0)

    # ── Standardize (63-day rolling z-score then full-sample scale) ──
    fact_sc = pd.DataFrame(
        StandardScaler().fit_transform(fac_df[FACTOR_COLS].values),
        index=fac_df.index,
        columns=FACTOR_COLS,
    )

    # ── Shock classification ──────────────────────────────────
    shock_cls = classify_shock(brent_r, fac_df["F3_Commodity"], fac_df["F2_USD"])
    current_shock = str(shock_cls.iloc[-1]) if len(shock_cls) > 0 else "MIXED"

    # ── Factor betas per market (weekly regression) ──────────
    ret_w = ret_clean.resample("W-FRI").mean().dropna(how="all")
    fac_w = fact_sc.resample("W-FRI").mean().dropna(how="all")
    cw = ret_w.index.intersection(fac_w.index)
    ret_w, fac_w = ret_w.loc[cw], fac_w.loc[cw]

    betas = {}
    for mkt in markets_use:
        y = ret_w[mkt].values
        X = fac_w[FACTOR_COLS].values
        ok = np.isfinite(y) & np.isfinite(X).all(1)
        if ok.sum() < 10:
            betas[mkt] = {f: 0.0 for f in FACTOR_COLS}
            continue
        lr = LinearRegression().fit(X[ok], y[ok])
        betas[mkt] = {f: round(float(c), 4) for f, c in zip(FACTOR_COLS, lr.coef_)}

    # ── Factor Risk Contribution (RC_j = β_j × Cov(F_j, R_i)) ──
    # Compute portfolio-level factor risk decomposition
    # Uses the formula: Weight_j = β_j × Cov(F_j, R_portfolio) / Var(R_portfolio)
    port_ret_daily = ret_clean[markets_use].mean(axis=1)
    fac_daily = fact_sc.reindex(port_ret_daily.index).ffill().fillna(0)
    common_rc = port_ret_daily.dropna().index.intersection(fac_daily.dropna(how="all").index)

    factor_risk_contrib = {}
    if len(common_rc) > 30:
        pr = port_ret_daily.loc[common_rc].values
        var_port = np.var(pr) + 1e-12
        # Average betas across portfolio assets
        avg_betas = {f: np.mean([betas.get(m, {}).get(f, 0) for m in markets_use]) for f in FACTOR_COLS}
        total_contrib = 0
        for f in FACTOR_COLS:
            fv = fac_daily[f].loc[common_rc].values
            cov_fj_ri = np.cov(fv, pr)[0, 1]
            rc = avg_betas[f] * cov_fj_ri
            factor_risk_contrib[f] = rc
            total_contrib += abs(rc)
        # Normalize to percentage
        for f in FACTOR_COLS:
            raw_rc = factor_risk_contrib[f]
            pct = abs(raw_rc) / total_contrib * 100 if total_contrib > 0 else 0
            factor_risk_contrib[f] = {
                "rc": round(raw_rc, 6),
                "pct": round(pct, 1),
                "beta": round(avg_betas[f], 4),
            }
    else:
        for f in FACTOR_COLS:
            factor_risk_contrib[f] = {"rc": 0, "pct": round(100 / len(FACTOR_COLS), 1), "beta": 0}

    # Sort by absolute contribution for display
    factor_risk_sorted = sorted(factor_risk_contrib.items(), key=lambda x: abs(x[1]["pct"]), reverse=True)

    # ── Current factor z-scores (21-day average) ─────────────
    cur_f = fact_sc.iloc[-21:].mean()
    prev_f = fact_sc.iloc[-42:-21].mean() if len(fact_sc) > 42 else cur_f
    dom_f = cur_f.abs().idxmax()

    # ── Factor strength monitor (High/Medium/Low + trend) ────
    STRENGTH_LABELS = {
        "high_pos": "High",
        "mod_pos": "Elevated",
        "low": "Neutral",
        "mod_neg": "Depressed",
        "high_neg": "Low",
    }
    factor_strength = {}
    for f in FACTOR_COLS:
        z = float(cur_f[f])
        z_prev = float(prev_f[f])
        abs_z = abs(z)
        if abs_z > 2.0:
            strength = "High" if z > 0 else "Low"
        elif abs_z > 1.0:
            strength = "Elevated" if z > 0 else "Depressed"
        else:
            strength = "Neutral"
        # Trend: compare current 21d avg to previous 21d avg
        diff = z - z_prev
        if diff > 0.3:
            trend = "increasing"
        elif diff < -0.3:
            trend = "decreasing"
        else:
            trend = "stable"
        factor_strength[f] = {
            "strength": strength,
            "trend": trend,
            "z_current": round(z, 3),
            "z_change": round(diff, 3),
        }

    # ── Per-asset factor exposure matrix ─────────────────────
    asset_exposure = []
    for mkt in markets_use:
        row = {"asset": mkt}
        for f in FACTOR_COLS:
            row[f] = betas.get(mkt, {}).get(f, 0)
        asset_exposure.append(row)

    # ── Factor time series for frontend charts ────────────────
    fts = fact_sc.tail(300)
    step = max(1, len(fts) // 200)
    factor_ts = []
    for i in range(0, len(fts), step):
        rec = {
            "date": (str(fts.index[i].date())
                     if hasattr(fts.index[i], "date") else str(fts.index[i]))
        }
        for f in FACTOR_COLS:
            rec[f] = round(float(fts.iloc[i][f]), 3)
        factor_ts.append(rec)

    # ── Per-factor individual time series (for individual charts) ──
    per_factor_ts = {}
    for f in FACTOR_COLS:
        fs = fts[f]
        per_factor_ts[f] = {
            "dates": [str(fs.index[i].date()) if hasattr(fs.index[i], "date") else str(fs.index[i])
                      for i in range(0, len(fs), step)],
            "values": [round(float(fs.iloc[i]), 3) for i in range(0, len(fs), step)],
        }

    # ── Current values for frontend display ──────────────────
    current_values = {}
    for f in FACTOR_COLS:
        meta = FACTOR_META[f]
        z = float(cur_f[f])
        current_values[f] = {
            "label": meta["label"],
            "cls": meta["cls"],
            "color": meta["color"],
            "zscore": round(z, 3),
            "strength": factor_strength[f]["strength"],
            "trend": factor_strength[f]["trend"],
            "risk_pct": factor_risk_contrib[f]["pct"],
        }

    # ── Factor PCA — decompose which macro factors drive the most variance ──
    from sklearn.decomposition import PCA as _PCA
    factor_pca_result = {}
    try:
        fpca_data = fact_sc[FACTOR_COLS].dropna(how="any")
        if len(fpca_data) >= 30:
            n_fc = len(FACTOR_COLS)
            fpca = _PCA(n_components=n_fc)
            fpca.fit(fpca_data.values)
            f_eigenvalues = fpca.explained_variance_
            f_expl = fpca.explained_variance_ratio_
            f_comps = fpca.components_
            # Label each PC by the factor with highest absolute loading (unique assignment)
            f_scree = []
            f_pc_labels = []
            cumvar = 0.0
            used_factors = set()  # Track which factors are already assigned
            for j in range(n_fc):
                abs_loads = np.abs(f_comps[j])
                # Pick the highest-loading factor that hasn't been used yet
                sorted_idx = np.argsort(abs_loads)[::-1]
                top_idx = int(sorted_idx[0])
                for si in sorted_idx:
                    if int(si) not in used_factors:
                        top_idx = int(si)
                        break
                used_factors.add(top_idx)
                driver = FACTOR_COLS[top_idx]
                driver_label = FACTOR_META[driver]["label"]
                driver_loading = float(f_comps[j, top_idx])
                cumvar += float(f_expl[j]) * 100
                pc_label = f"PC{j+1} ({driver_label})"
                f_pc_labels.append(pc_label)
                # Top 3 loadings
                top3_idx = np.argsort(abs_loads)[::-1][:3]
                top3 = [{"factor": FACTOR_META[FACTOR_COLS[k]]["label"],
                         "loading": round(float(f_comps[j, k]), 3)}
                        for k in top3_idx]
                f_scree.append({
                    "pc": pc_label,
                    "eigenvalue": round(float(f_eigenvalues[j]), 4),
                    "variance_pct": round(float(f_expl[j]) * 100, 1),
                    "cumulative_pct": round(cumvar, 1),
                    "driver": driver_label,
                    "driver_loading": round(driver_loading, 3),
                    "top_loadings": top3,
                })
            # Factor loadings matrix (factors x PCs)
            f_loadings = []
            for fi, fn in enumerate(FACTOR_COLS):
                for j in range(n_fc):
                    f_loadings.append({
                        "factor": FACTOR_META[fn]["label"],
                        "component": f_pc_labels[j],
                        "value": round(float(f_comps[j, fi]), 3),
                    })
            factor_pca_result = {
                "scree": f_scree,
                "labels": f_pc_labels,
                "loadings": f_loadings,
                "n_factors": n_fc,
            }
    except Exception:
        pass

    return {
        "markets_used": markets_use,
        "factor_names": FACTOR_COLS,
        "factor_meta": FACTOR_META,
        "betas": betas,
        "current_zscores": {f: round(float(cur_f[f]), 3) for f in FACTOR_COLS},
        "current_values": current_values,
        "dominant_factor": {
            "id": dom_f,
            "label": FACTOR_META[dom_f]["label"],
            "class": FACTOR_META[dom_f]["cls"],
            "zscore": round(float(cur_f[dom_f]), 3),
        },
        "factor_risk_contribution": {f: factor_risk_contrib[f] for f in FACTOR_COLS},
        "factor_risk_sorted": [{"factor": f, "label": FACTOR_META[f]["label"],
                                 "pct": factor_risk_contrib[f]["pct"],
                                 "color": FACTOR_META[f]["color"]}
                                for f, _ in factor_risk_sorted],
        "factor_strength": factor_strength,
        "asset_exposure": asset_exposure,
        "herding": best_herd,
        "herding_per_market": herd_per_market,
        "shock_classification": {
            "current": current_shock,
            "types": {
                "DEMAND":   "Global recession — reduce equity",
                "SUPPLY":   "Geopolitical — hedge oil",
                "FINANCIAL":"USD crisis — hedge FX",
                "MIXED":    "No dominant driver",
            },
        },
        "factor_time_series": factor_ts,
        "per_factor_ts": per_factor_ts,
        "factor_pca": factor_pca_result,
        "_internal": {
            "fact_sc": fact_sc,
            "fac_df": fac_df,
            "betas": betas,
            "sov_per": {},
        },
    }
