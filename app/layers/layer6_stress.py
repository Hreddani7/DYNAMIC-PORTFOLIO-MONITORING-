"""
Layer 6 — Historical Crash Stress Testing (InteliRisk v4.1)
Client selects a real historical crash scenario to simulate their portfolio against.
Each scenario applies z-score factor shocks derived from actual market movements
during the crisis, propagated through factor betas and correlation contagion.
"""
import numpy as np
import pandas as pd

from app.config import STRESS_SCENARIOS, FACTOR_COLS, MARKETS


def run_stress(shocks, betas, corr, mkts, rounds=3):
    """Propagate factor shocks through correlation network.

    Returns impact as PERCENTAGE (e.g. -15.0 means -15%).
    Factor betas × z-score shocks yield % log-return impacts.
    Contagion multiplier scales inversely with number of assets.
    """
    direct = {m: sum(betas.get(m, {}).get(f, 0) * v for f, v in shocks.items() if f in FACTOR_COLS) for m in mkts}
    iv = np.array([direct[m] for m in mkts])
    cr = corr.values.copy()
    np.fill_diagonal(cr, 0)
    # Scale contagion: 0.20 for <=4 assets, lower for more (avoid feedback explosion)
    n = len(mkts)
    contagion_mult = 0.20 / max(1, n / 4)
    for _ in range(rounds):
        iv += contagion_mult * cr @ iv
    # iv is already in % units (beta * z-score = % impact)
    # Cap at reasonable levels
    iv = np.clip(iv, -60.0, 60.0)
    return {m: round(float(iv[i]), 4) for i, m in enumerate(mkts)}


def simulate_portfolio_stress(holdings, scenario_shocks, betas, corr, mkts):
    """
    Simulate a portfolio's estimated loss under a stress scenario.
    Returns per-holding impact and total portfolio impact.

    mkts can be stock tickers (NPN, SBK) or market indices (JSE_SA).
    When stock-level, each holding's impact comes from its ticker in mkts.
    """
    # Get impacts per asset in mkts (could be stock tickers or market indices)
    asset_impacts = run_stress(scenario_shocks, betas, corr, mkts)

    total_value = sum(abs(h.get("market_value", 0)) for h in holdings) or 1
    portfolio_loss = 0.0
    holding_impacts = []

    for h in holdings:
        ticker = h.get("asset_id", "").split(".")[0].upper()
        mkt = h.get("market", "")
        mv = h.get("market_value", 0)
        weight = abs(mv) / total_value if total_value > 0 else 0

        # Try ticker first (stock-level), then market (index-level)
        # run_stress already returns % values (e.g. -15.0 = -15%)
        mkt_impact_pct = asset_impacts.get(ticker, asset_impacts.get(mkt, 0))
        holding_loss = mv * (mkt_impact_pct / 100)

        holding_impacts.append({
            "asset_id": h.get("asset_id", ""),
            "market": mkt,
            "sector": h.get("sector", ""),
            "market_value": round(mv, 2),
            "weight": round(weight, 4),
            "impact_pct": round(mkt_impact_pct, 2),
            "estimated_loss": round(holding_loss, 2),
        })
        portfolio_loss += holding_loss

    return {
        "holding_impacts": holding_impacts,
        "portfolio_loss": round(portfolio_loss, 2),
        "portfolio_loss_pct": round(portfolio_loss / total_value * 100, 2) if total_value > 0 else 0,
        "market_impacts": {m: round(v, 2) for m, v in asset_impacts.items()},
    }


def compute_layer6(prices, holdings, l2_internal=None, l3_internal=None):
    """
    Layer 6 — Historical crash simulation.
    Returns all available scenarios + pre-computed results so the client
    can select any scenario to see their portfolio's estimated impact.

    When prices contains stock tickers (NPN, SBK, etc.), we build stock-level
    correlation for contagion propagation and map market-level factor betas
    to individual holdings through their market membership.
    """
    ret_all = (np.log(prices / prices.shift(1)) * 100).dropna(how="all")
    min_pts = min(100, max(10, len(ret_all) // 2))
    markets_use = [c for c in ret_all.columns if ret_all[c].dropna().shape[0] >= min_pts]
    if not markets_use:
        markets_use = [c for c in ret_all.columns if ret_all[c].dropna().shape[0] >= 5]

    # Get factor betas (keyed by market index e.g. JSE_SA)
    raw_betas = l3_internal.get("betas", {}) if l3_internal else {}

    # Detect if markets_use are stock tickers (not in MARKETS dict) vs market indices
    is_stock_level = any(m not in MARKETS for m in markets_use)

    # Build stock-to-market mapping from holdings
    stock_market_map = {}
    for h in holdings:
        ticker = h.get("asset_id", "").split(".")[0].upper()
        mkt = h.get("market", "")
        if ticker and mkt:
            stock_market_map[ticker] = mkt

    # Map market-level betas to stock tickers
    # Each stock inherits the betas of its parent market
    if is_stock_level:
        betas = {}
        for m in markets_use:
            parent_mkt = stock_market_map.get(m, "")
            if parent_mkt and parent_mkt in raw_betas:
                betas[m] = raw_betas[parent_mkt]
            else:
                # Try all available market betas (fallback to first available)
                for mk, mb in raw_betas.items():
                    betas[m] = mb
                    break
                else:
                    betas[m] = {}
    else:
        betas = raw_betas

    # Build correlation matrix from stock returns
    l2_corr = l2_internal.get("corr_mat") if l2_internal else None
    if l2_corr is not None and set(markets_use).issubset(set(l2_corr.index)):
        corr_mat = l2_corr.loc[markets_use, markets_use]
    else:
        cw = min(252, len(ret_all))
        corr_mat = ret_all[markets_use].iloc[-cw:].corr().fillna(0)
        np.fill_diagonal(corr_mat.values, 1.0)

    total_value = sum(abs(h.get("market_value", 0)) for h in holdings) or 1

    # Pre-compute all scenarios
    scenarios = {}
    for name, scenario in STRESS_SCENARIOS.items():
        sim = simulate_portfolio_stress(holdings, scenario["shocks"], betas, corr_mat, markets_use)
        scenarios[name] = {
            "type": scenario["type"],
            "period": scenario["period"],
            "jse_actual_dd": scenario.get("jse_dd", 0),
            "shocks": scenario["shocks"],
            "market_impacts": sim["market_impacts"],
            "portfolio_loss": sim["portfolio_loss"],
            "portfolio_loss_pct": sim["portfolio_loss_pct"],
            "holding_impacts": sim["holding_impacts"],
        }

    # Find worst scenario
    worst_name = min(scenarios, key=lambda s: scenarios[s]["portfolio_loss_pct"]) if scenarios else "N/A"

    # Summary by type
    type_summary = {}
    for sname, sdata in scenarios.items():
        t = sdata["type"]
        if t not in type_summary:
            type_summary[t] = {"scenarios": 0, "avg_loss_pct": 0}
        type_summary[t]["scenarios"] += 1
        type_summary[t]["avg_loss_pct"] += sdata["portfolio_loss_pct"]
    for t in type_summary:
        type_summary[t]["avg_loss_pct"] = round(
            type_summary[t]["avg_loss_pct"] / type_summary[t]["scenarios"], 2
        )

    return {
        "scenarios": scenarios,
        "n_scenarios": len(scenarios),
        "worst_scenario": worst_name,
        "worst_loss_pct": scenarios[worst_name]["portfolio_loss_pct"] if worst_name in scenarios else 0,
        "portfolio_value": round(total_value, 2),
        "type_summary": type_summary,
        "markets_used": markets_use,
        "methodology": "Factor-beta shock × 3-round correlation contagion, based on real historical z-score movements",
        "available_scenarios": [
            {
                "name": name,
                "type": s["type"],
                "period": s["period"],
                "jse_actual_dd": s["jse_actual_dd"],
                "portfolio_loss_pct": s["portfolio_loss_pct"],
            }
            for name, s in scenarios.items()
        ],
    }
