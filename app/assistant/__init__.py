"""
AI Portfolio Assistant — LLM-powered conversational interface.
Uses computed layer data + HuggingFace Inference API (Qwen-72B / Llama-3.1-8B)
to answer portfolio-specific questions intelligently.
"""
import os, logging, json

log = logging.getLogger(__name__)

HF_TOKEN = os.environ.get("HF_TOKEN", "")
HF_MODEL_PRIMARY = "Qwen/Qwen2.5-72B-Instruct"
HF_MODEL_FALLBACK = "meta-llama/Llama-3.1-8B-Instruct"

SYSTEM_PROMPT = """You are the AI assistant for AfriSK InteliRisk v4, an African-focused portfolio risk intelligence platform.
You have access to the user's live portfolio risk data computed by the 8-layer risk engine:
- Layer 0: HAR-RV volatility, VaR/CVaR, drawdowns
- Layer 2: PCA structural analysis, correlation network, contagion
- Layer 3: 6-factor African model (Global, USD, Commodity, Sovereign, Domestic, Herding)
- Layer 4: 5-state HMM regime detection (25 features across 7 macro categories)
- Layer 5: Composite risk score (0-100)
- Layer 6: Stress testing (10 historical scenarios)
- Layer 7: AI intelligence report

Answer the user's question using ONLY the portfolio data provided. Be specific — cite numbers.
Be concise but thorough. If the data doesn't cover the question, say so.
Speak as a senior risk analyst would to a portfolio manager."""


def _build_context_summary(context):
    """Build a concise data summary from the computed context for the LLM."""
    if not context:
        return "No portfolio data available. The user needs to upload a portfolio and run compute-all first."

    parts = []

    # Score
    sc = context.get("score", {})
    if sc:
        parts.append(f"RISK SCORE: {sc.get('score', '?')}/100, Level: {sc.get('level', '?')}")

    # Regime
    r = context.get("regime", {})
    if r:
        probs = r.get("regime_probs", {})
        probs_str = ", ".join(f"{k}: {v*100:.0f}%" for k, v in sorted(probs.items(), key=lambda x: -x[1]) if isinstance(v, (int, float)))
        parts.append(
            f"REGIME: {r.get('active_regime', '?')} (duration: {r.get('regime_duration', '?')}d, "
            f"crisis prob: {r.get('crisis_probability', 0):.0%})\n"
            f"  Probabilities: {probs_str}\n"
            f"  HMM features: {r.get('model_info', {}).get('n_features', '?')}, "
            f"categories: {', '.join(r.get('model_info', {}).get('feature_categories', {}).keys())}"
        )
        forecast = r.get("forecast", {})
        if forecast:
            f21 = forecast.get("+21d", {})
            if f21:
                parts.append(f"  21d forecast: {f21.get('regime', '?')}")

    # Risk core
    rc = context.get("risk_core", {})
    p = rc.get("portfolio", {})
    if p:
        parts.append(
            f"PORTFOLIO RISK: Vol21d={p.get('mean_vol_21d', '?')}%, "
            f"VaR={p.get('mean_var', '?')}%, CVaR={p.get('mean_cvar', '?')}%, "
            f"MaxDD={p.get('worst_dd', '?')}%"
        )
    ms = rc.get("market_summary", {})
    if ms:
        for mkt, d in ms.items():
            parts.append(
                f"  {d.get('name', mkt)}: vol21d={d.get('vol_21d', '?')}%, "
                f"HAR={d.get('har_forecast', '?')}%, VaR={d.get('current_var', '?')}, "
                f"MaxDD={d.get('max_dd', '?')}%"
            )

    # Structural
    s = context.get("structural", {})
    eg = s.get("eigen_concentration", {})
    co = s.get("correlation", {})
    nt = s.get("network", {})
    if eg:
        parts.append(
            f"STRUCTURAL: PC1={eg.get('current', 0)*100:.1f}%, fragile={eg.get('fragile', False)}, "
            f"avg_corr={co.get('avg', 0)*100:.1f}%, "
            f"network: {len(nt.get('nodes', []))} nodes, density={nt.get('density', 0):.2f}, "
            f"hub={nt.get('hub', 'N/A')}"
        )

    # Factors
    f = context.get("factors", {})
    if f:
        dom = f.get("dominant_factor", {})
        zs = f.get("current_zscores", {})
        zs_str = ", ".join(f"{k}: {v:+.2f}" for k, v in zs.items()) if zs else "N/A"
        parts.append(
            f"FACTORS: Dominant={dom.get('label', '?')} (z={dom.get('zscore', 0):+.2f})\n"
            f"  Z-scores: {zs_str}\n"
            f"  Shock: {f.get('shock_classification', {}).get('current', '?')}"
        )

    # Stress
    st = context.get("stress", {})
    if st:
        parts.append(
            f"STRESS: worst={st.get('worst_scenario', '?')}, "
            f"impact={st.get('worst_total', 0):.1%}, "
            f"vulnerable={st.get('most_vulnerable', '?')}, "
            f"{st.get('n_scenarios', 0)} scenarios"
        )

    # Intelligence
    it = context.get("intelligence", {})
    if it:
        rec = it.get("recommendation", {})
        parts.append(
            f"INTELLIGENCE: posture={rec.get('posture', '?')}, "
            f"alerts={len(it.get('alerts', []))}, "
            f"herding={it.get('herding', {}).get('level', '?')}"
        )

    # Holdings
    holdings = context.get("portfolio_holdings", [])
    if holdings:
        h_str = ", ".join(f"{h.get('symbol', '?')}({h.get('weight', 0):.1f}%)" for h in sorted(holdings, key=lambda x: -x.get('weight', 0))[:10])
        parts.append(f"HOLDINGS: {len(holdings)} stocks — {h_str}")

    return "\n".join(parts)


def _call_llm_chat(user_message, context_summary):
    """Call HF LLM for chat response."""
    if not HF_TOKEN:
        return None

    try:
        from huggingface_hub import InferenceClient
    except ImportError:
        return None

    client = InferenceClient(token=HF_TOKEN)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"PORTFOLIO DATA:\n{context_summary}\n\nUSER QUESTION: {user_message}"},
    ]

    for model_id in [HF_MODEL_PRIMARY, HF_MODEL_FALLBACK]:
        try:
            response = client.chat_completion(
                messages=messages,
                model=model_id,
                max_tokens=800,
                temperature=0.3,
                top_p=0.9,
            )
            text = response.choices[0].message.content
            if text and len(text.strip()) > 20:
                return text.strip()
        except Exception as e:
            log.info(f"Chat LLM {model_id} failed: {str(e)[:100]}")
            continue

    return None


class PortfolioAssistant:
    """LLM-powered assistant with rule-based fallback."""

    def respond(self, message, context=None):
        msg = message.lower().strip()

        # Greeting — no need for LLM
        if any(w in msg for w in ["hello", "hi ", "hey", "help", "start", "what can"]) and len(msg) < 30:
            return {"response": (
                "Welcome to AfriSK Intelligence. I can help with:\n\n"
                "• Risk Analysis — VaR, volatility, drawdown, concentration\n"
                "• Market Regime — Current state, duration, forecast, probabilities\n"
                "• Factor Exposure — What macro forces drive your portfolio\n"
                "• Structural — PCA, correlation, contagion network\n"
                "• Stress Testing — Scenario analysis and shock propagation\n"
                "• Recommendations — Posture and allocation guidance\n\n"
                "Ask any question about your portfolio!"
            ), "intent": "greeting"}

        # Build context summary for LLM
        context_summary = _build_context_summary(context)

        # Try LLM first
        llm_response = _call_llm_chat(message, context_summary)
        if llm_response:
            return {"response": llm_response, "intent": "ai"}

        # Fallback: rule-based responses
        return self._rule_based(msg, context)

    def _rule_based(self, msg, context):
        """Rule-based fallback when LLM is unavailable."""
        if not context:
            return {"response": "No portfolio data available. Please upload a portfolio and run the analysis first.", "intent": "no_data"}

        if any(w in msg for w in ["regime", "state", "market condition"]):
            r = context.get("regime", {})
            if r:
                probs = "\n".join(f"  {n}: {p*100:.1f}%" for n, p in sorted(r.get("regime_probs", {}).items(), key=lambda x: -x[1]) if isinstance(p, (int, float)))
                return {"response": f"Active Regime: {r.get('active_regime', '?')}\nDuration: {r.get('regime_duration', '?')} days\n\nProbabilities:\n{probs}", "intent": "regime"}

        if any(w in msg for w in ["risk", "var", "vol", "volatility", "drawdown"]):
            p = context.get("risk_core", {}).get("portfolio", {})
            if p:
                return {"response": (
                    f"Portfolio Risk:\n"
                    f"• Vol 21d: {p.get('mean_vol_21d', 0):.2f}%\n"
                    f"• VaR: {p.get('mean_var', 0):.3f}%\n"
                    f"• CVaR: {p.get('mean_cvar', 0):.3f}%\n"
                    f"• Worst DD: {p.get('worst_dd', 0):.1f}%"
                ), "intent": "risk"}

        if any(w in msg for w in ["score", "rating", "overall"]):
            s = context.get("score", {})
            if s:
                return {"response": f"Risk Score: {s.get('score', 0):.1f}/100 ({s.get('level', '?')})", "intent": "score"}

        if any(w in msg for w in ["recommend", "advice", "should", "action", "posture"]):
            intel = context.get("intelligence", {})
            if intel:
                rec = intel.get("recommendation", {})
                acts = "\n".join(f"  {i+1}. {a}" for i, a in enumerate(rec.get("actions", [])))
                return {"response": f"Posture: {rec.get('posture', 'BALANCED')}\n\nActions:\n{acts}", "intent": "recommendation"}

        if any(w in msg for w in ["factor", "beta", "exposure", "macro"]):
            f = context.get("factors", {})
            if f:
                zs = "\n".join(f"  {k}: {v:+.2f}" for k, v in f.get("current_zscores", {}).items())
                return {"response": f"Factor Z-Scores:\n{zs}", "intent": "factors"}

        if any(w in msg for w in ["stress", "scenario", "shock"]):
            st = context.get("stress", {})
            if st:
                return {"response": f"Worst stress: {st.get('worst_scenario', '?')} (impact: {st.get('worst_total', 0):.1%})", "intent": "stress"}

        return {"response": "I can help with risk, regimes, factors, stress tests, scores, or recommendations. What would you like to know?", "intent": "general"}
