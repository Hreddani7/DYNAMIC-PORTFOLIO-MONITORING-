"""
Layer 5 — Adaptive Continental Risk Score (InteliRisk v4.1)
6-component sigmoid-normalized score with regime adjustment.

Components (weighted):
  market_risk     25%  — Portfolio forward volatility
  structural_risk 20%  — PC1 concentration (fragility)
  usd_liquidity   15%  — F2_USD factor z-score
  commodity_risk  15%  — F3_Commodity factor z-score
  sovereign_risk  15%  — F4_Sovereign factor z-score
  behavioral_risk 10%  — F6_Herding factor z-score

7-level risk scale from LOW to CRITICAL.
Regime adjustment multiplier amplifies score in stress regimes.
"""
import numpy as np
import pandas as pd

from app.config import RISK_LEVELS, REGIME_LABELS, REGIME_COUNT, MARKETS

# ── Internal weights (override config SCORE_WEIGHTS) ─────────────
SCORE_WEIGHTS_6 = {
    "market_risk": 0.25,
    "structural_risk": 0.20,
    "usd_liquidity": 0.15,
    "commodity_risk": 0.15,
    "sovereign_risk": 0.15,
    "behavioral_risk": 0.10,
}

# ── 7-level risk scale ───────────────────────────────────────────
SCORE_LEVELS = [
    (0, 15, "LOW", "#1E6B2E"),
    (15, 30, "GUARDED", "#27AE60"),
    (30, 45, "ELEVATED", "#C9A227"),
    (45, 60, "MODERATE", "#E67E22"),
    (60, 75, "HIGH", "#E74C3C"),
    (75, 90, "SEVERE", "#C0392B"),
    (90, 100, "CRITICAL", "#8B0000"),
]

# ── Regime adjustment multipliers ────────────────────────────────
REGIME_MULT = {0: 0.85, 1: 0.95, 2: 1.10, 3: 1.15, 4: 1.25}


def _get_level(score):
    for lo, hi, lv, cl in SCORE_LEVELS:
        if lo <= score < hi:
            return lv, cl
    return "CRITICAL", "#8B0000"


def compute_layer5(prices, holdings, l0_internal=None, l2_internal=None,
                   l3_internal=None, l4_internal=None):
    """Full Layer 5 — 6-component sigmoid risk score with regime adjustment."""
    ret_all = (np.log(prices / prices.shift(1)) * 100).dropna(how="all")
    min_pts = min(100, max(10, len(ret_all) // 2))
    markets_use = [c for c in ret_all.columns if ret_all[c].dropna().shape[0] >= min_pts]
    if not markets_use:
        markets_use = [c for c in ret_all.columns if ret_all[c].dropna().shape[0] >= 5]
    ret_clean = ret_all[markets_use].ffill().dropna(how="all")

    # Gather internals
    regime_s = l4_internal.get("regime_s", pd.Series(dtype=int)) if l4_internal else pd.Series(dtype=int)
    pca_conc = l2_internal.get("pca_conc", pd.DataFrame()) if l2_internal else pd.DataFrame()
    roll_v = l0_internal.get("roll_v", {}) if l0_internal else {}
    risk_metrics = l0_internal.get("risk_metrics", {}) if l0_internal else {}
    fact_sc = l3_internal.get("fact_sc", pd.DataFrame()) if l3_internal else pd.DataFrame()

    # Common index
    cm = ret_clean.index
    if len(regime_s) > 0:
        cm = cm.intersection(regime_s.index)
    if len(pca_conc) > 0:
        cm = cm.intersection(pca_conc.index)
    if len(fact_sc) > 0:
        cm = cm.intersection(fact_sc.index)

    if len(cm) < 30:
        return _default_score()

    sc = pd.DataFrame(index=cm)

    # ── Component 1: Market Risk (forward vol) ─────────────────
    vm_parts = []
    for m in markets_use:
        if m in roll_v and "21d" in roll_v[m]:
            vm_parts.append(roll_v[m]["21d"].reindex(cm))
    if vm_parts:
        vm = pd.concat(vm_parts, axis=1).mean(axis=1)
    else:
        vm = ret_clean.reindex(cm).rolling(21, min_periods=5).std().mean(axis=1) * np.sqrt(252)
    sc["market_risk"] = _zscore(vm)

    # ── Component 2: Structural Risk (PC1 concentration) ──────
    if len(pca_conc) > 0 and "PC1_frac" in pca_conc.columns:
        pc1 = pca_conc["PC1_frac"].reindex(cm).ffill()
        sc["structural_risk"] = _zscore(pc1)
    else:
        sc["structural_risk"] = 0.0

    # ── Component 3: USD Liquidity (F2_USD) ───────────────────
    if len(fact_sc) > 0 and "F2_USD" in fact_sc.columns:
        f2 = fact_sc["F2_USD"].reindex(cm).ffill()
        sc["usd_liquidity"] = _zscore(f2)
    else:
        sc["usd_liquidity"] = 0.0

    # ── Component 4: Commodity Risk (F3_Commodity) ────────────
    if len(fact_sc) > 0 and "F3_Commodity" in fact_sc.columns:
        # Commodity shock: negative z-score = falling commodities = bad for SA
        f3 = -fact_sc["F3_Commodity"].reindex(cm).ffill()
        sc["commodity_risk"] = _zscore(f3)
    else:
        sc["commodity_risk"] = 0.0

    # ── Component 5: Sovereign Risk (F4_Sovereign) ────────────
    if len(fact_sc) > 0 and "F4_Sovereign" in fact_sc.columns:
        f4 = fact_sc["F4_Sovereign"].reindex(cm).ffill()
        sc["sovereign_risk"] = _zscore(f4)
    else:
        sc["sovereign_risk"] = 0.0

    # ── Component 6: Behavioral Risk (F6_Herding) ─────────────
    if len(fact_sc) > 0 and "F6_Herding" in fact_sc.columns:
        f6 = -fact_sc["F6_Herding"].reindex(cm).ffill()  # negative CSAD = herding = risk
        sc["behavioral_risk"] = _zscore(f6)
    else:
        sc["behavioral_risk"] = 0.0

    sc = sc.ffill().bfill().fillna(0)

    # ── Sigmoid-normalized composite ──────────────────────────
    raw_s = sum(SCORE_WEIGHTS_6[k] * sc[k] for k in SCORE_WEIGHTS_6)
    raw_score = 100 / (1 + np.exp(-0.5 * raw_s))

    # ── Regime adjustment ─────────────────────────────────────
    if len(regime_s) > 0:
        cur_regime_idx = int(regime_s.reindex(cm).iloc[-1]) if len(regime_s.reindex(cm).dropna()) > 0 else 0
    else:
        cur_regime_idx = 0
    regime_mult = REGIME_MULT.get(cur_regime_idx, 1.0)

    # Apply multiplier: shift score towards extremes
    # multiplier > 1 pushes score up, < 1 pushes down
    risk_score = raw_score * regime_mult
    risk_score = risk_score.clip(0, 100)

    cur_raw = round(float(raw_score.iloc[-1]), 2)
    cur_score = round(float(risk_score.iloc[-1]), 2)
    score_lv, score_col = _get_level(cur_score)

    # ── Score trend ───────────────────────────────────────────
    score_1d = round(float(risk_score.iloc[-1] - risk_score.iloc[-2]), 2) if len(risk_score) >= 2 else 0
    score_21d = round(float(risk_score.iloc[-1] - risk_score.iloc[-min(22, len(risk_score))]), 2) if len(risk_score) >= 2 else 0
    if score_21d < -2:
        trend = "improving"
    elif score_21d > 2:
        trend = "deteriorating"
    else:
        trend = "stable"
    trend_strength = round(abs(score_21d) / 21, 3) if len(risk_score) > 21 else 0

    # ── Per-asset risk scores ─────────────────────────────────
    country_scores = {}
    for mkt in markets_use:
        sc_c = pd.DataFrame(index=cm)
        # Vol component
        if mkt in roll_v and "21d" in roll_v[mkt]:
            vc = roll_v[mkt]["21d"].reindex(cm)
            sc_c["vol"] = _zscore(vc)
        else:
            r = ret_clean[mkt].reindex(cm) if mkt in ret_clean.columns else pd.Series(0.0, index=cm)
            vc = r.rolling(min(21, max(len(r) // 3, 5)), min_periods=3).std() * np.sqrt(252)
            sc_c["vol"] = _zscore(vc)
        # Shared components
        sc_c["structural"] = sc["structural_risk"]
        sc_c["sovereign"] = sc["sovereign_risk"]
        # Drawdown
        if mkt in risk_metrics and "drawdown" in risk_metrics[mkt]:
            dc = pd.Series(risk_metrics[mkt]["drawdown"], index=risk_metrics[mkt]["idx"]).reindex(cm).ffill().fillna(0)
            sc_c["dd"] = -_zscore(dc)
        else:
            sc_c["dd"] = 0.0
        sc_c = sc_c.ffill().bfill().fillna(0)
        cs = 100 / (1 + np.exp(-0.4 * (0.30 * sc_c["vol"] + 0.25 * sc_c["structural"]
                                         + 0.25 * sc_c["sovereign"] + 0.20 * sc_c["dd"])))
        cur_cs = round(float(cs.iloc[-1]), 2)
        cs_lv, _ = _get_level(cur_cs)
        display_name = MARKETS.get(mkt, {}).get("name", mkt)
        country_scores[mkt] = {"score": cur_cs, "level": cs_lv, "name": display_name}

    # ── Score history ─────────────────────────────────────────
    score_hist = []
    step = max(1, len(risk_score) // 200)
    for i in range(0, len(risk_score), step):
        d = risk_score.index[i]
        score_hist.append({
            "date": str(d.date()) if hasattr(d, 'date') else str(d),
            "score": round(float(risk_score.iloc[i]), 2),
        })

    # ── Component contributions ───────────────────────────────
    contributions = {}
    for k in SCORE_WEIGHTS_6:
        contributions[k] = {
            "weight": SCORE_WEIGHTS_6[k],
            "z_score": round(float(sc[k].iloc[-1]), 3),
            "contribution": round(float(SCORE_WEIGHTS_6[k] * sc[k].iloc[-1]), 3),
        }

    return {
        "score": cur_score,
        "raw_score": cur_raw,
        "level": score_lv,
        "color": score_col,
        "regime_adjustment": round(regime_mult, 2),
        "trend": trend,
        "trend_strength": trend_strength,
        "score_1d_change": score_1d,
        "score_21d_change": score_21d,
        "country_scores": country_scores,
        "contributions": contributions,
        "score_weights": SCORE_WEIGHTS_6,
        "risk_levels": [{"lo": lo, "hi": hi, "level": lv, "color": cl} for lo, hi, lv, cl in SCORE_LEVELS],
        "history": score_hist,
    }


def _zscore(s):
    """Z-score a series."""
    return (s - s.mean()) / (s.std() + 1e-8)


def _default_score():
    return {
        "score": 50.0, "raw_score": 50.0, "level": "MODERATE", "color": "#E67E22",
        "regime_adjustment": 1.0, "trend": "stable", "trend_strength": 0,
        "score_1d_change": 0, "score_21d_change": 0,
        "country_scores": {}, "contributions": {},
        "score_weights": SCORE_WEIGHTS_6,
        "risk_levels": [{"lo": lo, "hi": hi, "level": lv, "color": cl} for lo, hi, lv, cl in SCORE_LEVELS],
        "history": [],
    }
