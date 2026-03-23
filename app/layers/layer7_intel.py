"""
Layer 7 — AI Portfolio Intelligence (InteliRisk v4)
Full JSON report: headline, alerts, herding, shock class, posture, actions.
"""
from datetime import datetime
from app.config import RISK_LEVELS, REGIME_LABELS, FACTOR_META, FACTOR_COLS


def compute_layer7(l0_data, l2_data, l3_data, l4_data, l5_data, l6_data):
    score = l5_data.get("score", 50)
    level = l5_data.get("level", "MODERATE")
    regime = l4_data.get("active_regime", "Unknown")
    crisis_p = l4_data.get("crisis_probability", 0)

    # Headline
    headlines = {
        "LOW": f"Continental risk score at {score}/100 — low risk. {regime} regime supports stable conditions. Favorable for strategic positioning.",
        "MODERATE": f"Risk score {score}/100 — moderate. Under {regime} regime, maintain vigilance and monitor factor z-scores.",
        "HIGH": f"WARNING: Risk score {score}/100 — elevated. {regime} regime amplifies vulnerabilities. Reduce concentrated exposures.",
        "CRITICAL": f"CRITICAL: Risk score {score}/100. {regime} regime signals severe stress. Immediate defensive action required.",
    }

    # Alerts
    alerts = []
    eigen = l2_data.get("eigen_concentration", {})
    if eigen.get("fragile"):
        alerts.append({"severity": "HIGH", "type": "FRAGILITY", "msg": f"PC1 concentration at {eigen.get('current', 0)*100:.1f}% — diversification has collapsed (>{eigen.get('threshold', 0.6)*100:.0f}% threshold)."})
    elif eigen.get("current", 0) > 0.45:
        alerts.append({"severity": "MEDIUM", "type": "FRAGILITY", "msg": f"PC1 at {eigen.get('current', 0)*100:.1f}% — approaching fragility threshold."})

    if crisis_p > 0.3:
        alerts.append({"severity": "HIGH", "type": "REGIME", "msg": f"Crisis probability at {crisis_p*100:.1f}% — Sovereign Stress + Systemic Crisis regimes elevated."})

    herding = l3_data.get("herding", {})
    if herding.get("sig"):
        alerts.append({"severity": "HIGH", "type": "HERDING", "msg": f"Statistically significant herding detected (H={herding.get('H', 0):.4f}, t={herding.get('t_g2', 0):.2f}). Market following behavior amplifies crash risk."})

    for f in FACTOR_COLS:
        z = l3_data.get("current_zscores", {}).get(f, 0)
        if abs(z) > 2:
            alerts.append({"severity": "HIGH", "type": "FACTOR", "msg": f"{FACTOR_META[f]['label']} z-score at {z:+.2f} — tail stress active."})
        elif abs(z) > 1.5:
            alerts.append({"severity": "MEDIUM", "type": "FACTOR", "msg": f"{FACTOR_META[f]['label']} z-score at {z:+.2f} — elevated."})

    shock = l3_data.get("shock_classification", {}).get("current", "MIXED")
    if shock in ("DEMAND", "FINANCIAL"):
        alerts.append({"severity": "MEDIUM", "type": "SHOCK", "msg": f"Commodity shock classified as {shock} — {l3_data.get('shock_classification', {}).get('types', {}).get(shock, '')}."})

    worst = l6_data.get("worst_scenario", "N/A")
    worst_impact = l6_data.get("worst_total", 0)
    vulnerable = l6_data.get("most_vulnerable", "N/A")

    alerts.sort(key=lambda x: {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(x["severity"], 3))

    # Posture
    posture_map = {"LOW": "OPPORTUNISTIC", "MODERATE": "BALANCED", "HIGH": "CAUTIOUS", "CRITICAL": "DEFENSIVE"}
    posture = posture_map.get(level, "BALANCED")

    actions = {
        "DEFENSIVE": [
            "Reduce all high-beta and systemically connected positions",
            "Rotate to sovereign-backed or hard-currency instruments",
            "Implement FX hedges across all African currencies",
            "Review all concentration limits immediately",
            "Set stop-loss at -2 sigma on every position",
        ],
        "CAUTIOUS": [
            "Trim overweight positions in stressed sectors",
            "Hedge commodity exposure if shock = SUPPLY/DEMAND",
            "Diversify across less correlated markets",
            "Monitor regime forecast — prepare for possible escalation",
            "Increase review frequency to daily",
        ],
        "BALANCED": [
            "Maintain strategic allocation with regular monitoring",
            "Seek selective value opportunities",
            "Rebalance to target weights quarterly",
            "Monitor regime transitions and factor z-scores weekly",
            "Prepare contingency plan for HIGH/CRITICAL scenarios",
        ],
        "OPPORTUNISTIC": [
            "Current conditions support selective risk-taking",
            "Increase allocation to high-conviction positions",
            "Exploit cross-market dispersion for relative value",
            "Build watchlist for entry on any regime shift",
            "Consider leveraged positions in low-vol markets",
        ],
    }

    # Diversification
    hhi = l0_data.get("herfindahl", 0)
    div_level = l0_data.get("herfindahl_level", "MODERATE")

    forecast = l4_data.get("forecast", {})

    return {
        "engine": "InteliRisk v4.0",
        "generated_at": datetime.now().isoformat(),
        "headline": headlines.get(level, headlines["MODERATE"]),
        "score": score,
        "level": level,
        "color": l5_data.get("color", "#C9A227"),
        "active_regime": regime,
        "crisis_probability": crisis_p,
        "regime_forecast": forecast,
        "dominant_factor": l3_data.get("dominant_factor", {}),
        "shock_classification": l3_data.get("shock_classification", {}).get("current", "N/A"),
        "herding": {
            "detected": herding.get("sig", False),
            "level": herding.get("level", "N/A"),
            "H": herding.get("H", None),
        },
        "diversification": {"hhi": hhi, "level": div_level},
        "stress_summary": {
            "worst_scenario": worst,
            "worst_impact": worst_impact,
            "most_vulnerable": vulnerable,
            "n_scenarios": l6_data.get("n_scenarios", 0),
        },
        "alerts": alerts,
        "recommendation": {"posture": posture, "actions": actions.get(posture, [])},
        "country_scores": l5_data.get("country_scores", {}),
        "narrative_sections": [
            {"title": "Market Overview", "content": headlines.get(level, "")},
            {"title": "Regime", "content": f"Active: {regime}. Crisis prob: {crisis_p:.1%}. Forecast +21d: {forecast.get('+21d', {}).get('regime', 'N/A')}."},
            {"title": "Factors", "content": f"Dominant: {l3_data.get('dominant_factor', {}).get('label', 'N/A')} (z={l3_data.get('dominant_factor', {}).get('zscore', 0):+.2f}). Shock: {shock}."},
            {"title": "Stress", "content": f"Worst: {worst} (impact: {worst_impact:.3f}). Most vulnerable: {vulnerable}."},
            {"title": "Posture", "content": f"{posture}. " + " ".join(actions.get(posture, [])[:2]) + "."},
        ],
    }
