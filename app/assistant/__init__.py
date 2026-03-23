"""
AI Portfolio Assistant — Context-aware conversational interface.
Uses computed layer data to answer portfolio questions.
"""


class PortfolioAssistant:
    """Rule-based assistant that reads all 8 layers to respond contextually."""

    KNOWLEDGE = {
        "var": "Value at Risk (VaR) quantifies maximum expected loss at a confidence level. We compute historical (direct percentile), parametric (Gaussian), and Cornish-Fisher (skew/kurtosis adjusted) VaR at 95% and 99%.",
        "cvar": "CVaR (Expected Shortfall) = average loss given we exceed VaR. More conservative than VaR, captures tail risk. Preferred by risk managers.",
        "regime": "GMM detects 5 regimes: Calm, USD Tightening, Commodity Shock, Sovereign Stress, Crisis. Bayesian smoothing (70/30 current/prior) prevents flickering. Each regime adjusts score weights.",
        "contagion": "Contagion network from thresholded correlations (≥0.45). Degree centrality = shock amplifiers. Betweenness = bridge assets between market clusters. Systemic nodes listed.",
        "pca": "PCA decomposes variance into orthogonal factors. PC1 > 40% means markets are a single block — diversification fails. We track eigenvalue concentration over time.",
        "stress": "5 scenarios: Oil Collapse, USD Surge, Rate Spike, Sovereign Blow, China Slowdown. Shocks propagate through network — systemically central assets amplify impact by up to 50%.",
        "hhi": "HHI = sum of squared weights. <0.08 excellent, 0.08-0.12 good, 0.12-0.18 fair, >0.18 dangerous concentration.",
        "factor": "5 African factors: Global Risk (VIX), USD Liquidity (DXY), Commodity (Oil+Gold), Sovereign (yield changes), Domestic (FX vol). Z-scores >2 = unusual stress.",
        "score": "0-100 composite: 6 components × regime-adaptive weights. Crisis raises volatility weight +10%. Calm reduces it -5%. Dynamic by design.",
        "drawdown": "Max drawdown = largest peak-to-trough decline. Recovery time = days to reclaim prior peak. Key worst-case metric.",
        "bloomberg": "AfriSK supports Bloomberg B-PIPE for real-time data. Priority: Bloomberg > yfinance > CSV. Auto-detects and degrades gracefully.",
    }

    def respond(self, message, context=None):
        msg = message.lower().strip()

        # ── Greeting ─────────────────────────────────────────
        if any(w in msg for w in ["hello", "hi", "hey", "help", "start", "what can"]):
            return {"response": (
                "Welcome to AfriSK Intelligence. I can help with:\n\n"
                "• Risk Analysis — VaR, volatility, drawdown, concentration\n"
                "• Market Regime — Current state and implications\n"
                "• Factor Exposure — What macro forces drive your portfolio\n"
                "• Contagion — Network interconnectedness and systemic risk\n"
                "• Stress Testing — Scenario analysis and shock propagation\n"
                "• Recommendations — Posture and allocation guidance\n"
                "• Education — Explain any concept (VaR, PCA, regimes...)\n\n"
                "What would you like to explore?"
            ), "intent": "greeting"}

        # ── Education ────────────────────────────────────────
        if any(w in msg for w in ["what is", "explain", "define", "how does", "tell me about", "meaning"]):
            for key, explanation in self.KNOWLEDGE.items():
                if key in msg:
                    return {"response": explanation, "intent": "education"}
            return {"response": "I can explain: VaR, CVaR, drawdown, regimes, contagion, PCA, stress testing, factors, HHI, score, Bloomberg. Which one?", "intent": "education"}

        # ── Context-aware ────────────────────────────────────
        if context:
            if any(w in msg for w in ["risk", "var", "vol", "volatility", "drawdown"]):
                p = context.get("risk_core", {}).get("portfolio", {})
                if p:
                    return {"response": (
                        f"Portfolio Risk Summary:\n\n"
                        f"• Annualized Vol: {p.get('annualized_vol',0)*100:.1f}%\n"
                        f"• VaR 95%: {p.get('var_95',0)*100:.2f}%\n"
                        f"• VaR 99%: {p.get('var_99',0)*100:.2f}%\n"
                        f"• CVaR 95%: {p.get('cvar_95',0)*100:.2f}%\n"
                        f"• Max Drawdown: {p.get('max_drawdown',0)*100:.1f}%\n"
                        f"• HHI: {p.get('hhi',0):.4f}\n"
                        f"• Skew: {p.get('skewness',0):.2f} | Kurt: {p.get('kurtosis',0):.2f}\n\n"
                        f"{'⚠️ Elevated volatility — review high-beta positions.' if p.get('annualized_vol',0)>0.30 else 'Volatility within normal ranges.'}"
                    ), "intent": "risk"}

            if any(w in msg for w in ["regime", "state", "market condition"]):
                r = context.get("regime", {})
                if r:
                    probs = "\n".join(f"  {n}: {p*100:.1f}%" for n, p in sorted(r.get("regime_probs", {}).items(), key=lambda x: -x[1]))
                    guidance = {
                        "Calm": "Stable — favorable for risk-taking.",
                        "USD Tightening": "Dollar strengthening — monitor FX.",
                        "Commodity Shock": "Resource stress — consider hedging.",
                        "Sovereign Stress": "Credit deteriorating — review bonds.",
                        "Crisis": "Multiple stresses — preserve capital.",
                    }
                    return {"response": f"Active Regime: {r.get('active_regime','?')}\n\nProbabilities:\n{probs}\n\n{guidance.get(r.get('active_regime',''), '')}", "intent": "regime"}

            if any(w in msg for w in ["score", "rating", "overall"]):
                s = context.get("score", {})
                if s:
                    comps = "\n".join(f"  {k.replace('_',' ').title()}: {v:.0f} (w: {s.get('weights',{}).get(k,0)*100:.0f}%)" for k, v in s.get("components", {}).items())
                    return {"response": f"Risk Score: {s.get('score',0)}/100 ({s.get('tier','?')})\n\nComponents:\n{comps}\n\nRegime: {s.get('active_regime','?')} — weights adapt dynamically.", "intent": "score"}

            if any(w in msg for w in ["recommend", "advice", "should", "action", "posture"]):
                intel = context.get("intelligence", {})
                if intel:
                    rec = intel.get("recommendation", {})
                    acts = "\n".join(f"  {i+1}. {a}" for i, a in enumerate(rec.get("actions", [])))
                    n_alerts = len(intel.get("alerts", []))
                    return {"response": f"Posture: {rec.get('posture','BALANCED')}\n\nActions:\n{acts}\n\n{'⚠️ '+str(n_alerts)+' alerts active.' if n_alerts else 'No critical alerts.'}", "intent": "recommendation"}

            if any(w in msg for w in ["stress", "scenario", "shock", "what if"]):
                st = context.get("stress", {}).get("summary", {})
                scenarios = context.get("stress", {}).get("scenarios", {})
                if st:
                    sc_str = "\n".join(f"  {s['name']}: {s['loss_pct']:.2f}% | Vol +{s['vol_increase']:.0f}%" for s in scenarios.values())
                    return {"response": f"Stress Results:\n\nWorst: {st.get('worst','?')} ({st.get('worst_loss',0):.2f}%)\nAvg: {st.get('avg_loss',0):.2f}%\n\n{sc_str}", "intent": "stress"}

            if any(w in msg for w in ["factor", "beta", "exposure", "macro"]):
                f = context.get("factors", {})
                if f:
                    zs = "\n".join(f"  {k.replace('_',' ').title()}: {v:.2f}" for k, v in f.get("current_zscores", {}).items())
                    return {"response": f"Factor Model (R²={f.get('r_squared',0)*100:.1f}%):\n\nZ-Scores:\n{zs}\n\nAlpha: {f.get('alpha',0)*10000:.1f} bps/day", "intent": "factors"}

            if any(w in msg for w in ["contagion", "network", "correlation", "systemic"]):
                net = context.get("structural", {}).get("network", {})
                corr = context.get("structural", {}).get("correlation", {})
                eigen = context.get("structural", {}).get("eigen_concentration", {})
                if net:
                    return {"response": (
                        f"Contagion Network:\n\n"
                        f"• Density: {net.get('density',0):.2f}\n"
                        f"• Edges: {net.get('n_edges',0)}\n"
                        f"• Clustering: {net.get('clustering',0):.2f}\n"
                        f"• Systemic: {', '.join(net.get('systemic_nodes',[]))}\n"
                        f"• Avg corr: {corr.get('avg',0)*100:.0f}%\n"
                        f"• Eigen conc: {eigen.get('current',0)*100:.0f}%"
                    ), "intent": "contagion"}

            if any(w in msg for w in ["holding", "portfolio", "asset", "allocation", "weight"]):
                assets = context.get("risk_core", {}).get("assets", {})
                if assets:
                    top = sorted(assets.items(), key=lambda x: -x[1]["weight"])[:8]
                    lines = "\n".join(f"  {a['name']}: {a['weight']*100:.1f}% ({a.get('market','')}/{a.get('sector','')})" for _, a in top)
                    return {"response": f"Top Holdings:\n\n{lines}\n\nTotal: {len(assets)} assets", "intent": "holdings"}

        return {"response": "I can help with risk, regimes, factors, contagion, stress tests, holdings, or recommendations. Ask me to explain any concept too.", "intent": "general"}
