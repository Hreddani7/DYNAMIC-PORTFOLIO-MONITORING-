"""
Layer 7 — AI-Powered Portfolio Intelligence (InteliRisk v4)
Architecture: Data Layer → Quant Engine → Risk Score → Structured JSON → Prompt Builder → LLM (Mistral) → AI Insights → Dashboard

Uses HuggingFace Inference API for Mistral-7B-Instruct-v0.1 with local rule-based fallback.
"""
import os, json, logging, threading, time
from datetime import datetime
from app.config import RISK_LEVELS, REGIME_LABELS, FACTOR_META, FACTOR_COLS

log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════
# HuggingFace Inference API config
# ═══════════════════════════════════════════════════════════════════════
HF_MODEL_PRIMARY = "Qwen/Qwen2.5-72B-Instruct"
HF_MODEL_FALLBACK = "meta-llama/Llama-3.1-8B-Instruct"
HF_TOKEN = os.environ.get("HF_TOKEN", "")
LLM_TIMEOUT = 10  # seconds — keep fast for demo, fallback to rule-based
LLM_MAX_TOKENS = 1500
LLM_TEMPERATURE = 0.3  # Low temp for analytical precision

# Cache for LLM responses (avoid re-calling for same risk state)
_llm_cache = {}
_llm_lock = threading.Lock()


# ═══════════════════════════════════════════════════════════════════════
# STEP 1: Structured JSON Output — extract risk metrics from all layers
# ═══════════════════════════════════════════════════════════════════════
def _build_risk_json(l0_data, l2_data, l3_data, l4_data, l5_data, l6_data):
    """Extract and structure ALL computed risk metrics into a single JSON
    that feeds the prompt builder. This is the quant engine → LLM bridge."""

    score = l5_data.get("score", 50)
    level = l5_data.get("level", "MODERATE")
    regime = l4_data.get("active_regime", "Unknown")
    crisis_p = l4_data.get("crisis_probability", 0)
    forecast = l4_data.get("forecast", {})
    herding = l3_data.get("herding", {})
    eigen = l2_data.get("eigen_concentration", {})
    shock = l3_data.get("shock_classification", {}).get("current", "MIXED")
    dom_factor = l3_data.get("dominant_factor", {})

    # Portfolio risk metrics from L0
    portfolio = l0_data.get("portfolio", {})
    market_summary = l0_data.get("market_summary", {})

    # Factor z-scores
    zscores = l3_data.get("current_zscores", {})
    factor_details = {}
    for f in FACTOR_COLS:
        z = zscores.get(f, 0)
        meta = FACTOR_META.get(f, {})
        factor_details[f] = {
            "label": meta.get("label", f),
            "class": meta.get("cls", "UNKNOWN"),
            "zscore": round(z, 4) if z else 0,
            "stress": "TAIL" if abs(z or 0) > 2 else "ELEVATED" if abs(z or 0) > 1.5 else "NORMAL",
        }

    # Regime details
    regime_probs = l4_data.get("regime_probs", l4_data.get("regime_probabilities", {}))
    regime_duration = l4_data.get("regime_duration", l4_data.get("regime_duration_days", 0))
    model_info = l4_data.get("model_info", {})
    macro_categories = model_info.get("feature_categories", l4_data.get("feature_categories", {}))
    hmm_features_used = sum(len(v) for v in macro_categories.values()) if isinstance(macro_categories, dict) else l4_data.get("n_features", 0)

    # Structural metrics from L2
    network = l2_data.get("network", {})
    pairwise = l2_data.get("pairwise_correlation", {})
    mean_corr = pairwise.get("current_mean", 0) if isinstance(pairwise, dict) else 0

    # Stress test from L6
    worst = l6_data.get("worst_scenario", "N/A")
    worst_impact = l6_data.get("worst_total", 0)
    vulnerable = l6_data.get("most_vulnerable", "N/A")
    scenarios = l6_data.get("scenarios", {})

    # Score components
    components = l5_data.get("components", {})

    return {
        "timestamp": datetime.now().isoformat(),
        "composite_score": {
            "value": round(score, 2),
            "level": level,
            "components": {k: round(v, 4) if isinstance(v, (int, float)) else v for k, v in components.items()},
        },
        "regime": {
            "current": regime,
            "duration_days": regime_duration,
            "crisis_probability": round(crisis_p, 4),
            "probabilities": {k: round(v, 4) if isinstance(v, (int, float)) else v for k, v in regime_probs.items()},
            "forecast_21d": forecast.get("+21d", {}).get("regime", "N/A"),
            "hmm_features": hmm_features_used,
            "macro_categories": macro_categories,
        },
        "factors": {
            "dominant": {
                "name": dom_factor.get("label", "N/A"),
                "zscore": round(dom_factor.get("zscore", 0), 4),
            },
            "details": factor_details,
            "shock_classification": shock,
        },
        "structural": {
            "eigen_concentration": round(eigen.get("current", 0), 4),
            "fragile": eigen.get("fragile", False),
            "mean_correlation": round(mean_corr, 4) if mean_corr else 0,
            "network_density": network.get("density", 0),
            "systemic_hub": network.get("hub", "N/A"),
        },
        "herding": {
            "detected": herding.get("sig", False),
            "level": herding.get("level", "N/A"),
            "H_coefficient": round(herding.get("H", 0), 6) if herding.get("H") else 0,
            "n_stocks": herding.get("n_stocks", 0),
        },
        "portfolio_risk": {
            # L0 uses mean_vol_21d / mean_var / mean_cvar / worst_dd
            "vol_21d": round(portfolio.get("mean_vol_21d", portfolio.get("vol_21d", 0)) / 100, 6),
            "vol_63d": round(portfolio.get("mean_vol_63d", portfolio.get("vol_63d", 0)) / 100, 6),
            "har_forecast": round(portfolio.get("har_forecast", 0), 6),
            "max_drawdown": round(portfolio.get("worst_dd", portfolio.get("max_dd", 0)) / 100, 4),
            "var_5pct": round(portfolio.get("mean_var", portfolio.get("current_var", 0)) / 100, 4),
            "cvar_5pct": round(portfolio.get("mean_cvar", portfolio.get("current_cvar", 0)) / 100, 4),
            "skewness": round(portfolio.get("skewness", 0), 4),
            "kurtosis": round(portfolio.get("kurtosis", 0), 4),
            "herfindahl": round(l0_data.get("herfindahl", 0), 4),
            "diversification_level": l0_data.get("herfindahl_level", "MODERATE"),
        },
        "stress_test": {
            "worst_scenario": worst,
            "worst_impact": round(worst_impact, 4) if isinstance(worst_impact, (int, float)) else 0,
            "most_vulnerable": vulnerable,
            "n_scenarios": l6_data.get("n_scenarios", 0),
        },
    }


# ═══════════════════════════════════════════════════════════════════════
# STEP 2: Alert System — AI + rules-based triggers
# ═══════════════════════════════════════════════════════════════════════
def _generate_alerts(risk_json):
    """Generate alerts from structured risk data using rules-based triggers.
    Triggers: score>70, correlation spike, USD factor>threshold, herding, jump detected."""
    alerts = []
    score = risk_json["composite_score"]["value"]
    level = risk_json["composite_score"]["level"]
    regime = risk_json["regime"]["current"]
    crisis_p = risk_json["regime"]["crisis_probability"]

    # 1. Risk Score threshold
    if score > 60:
        sev = "HIGH" if score > 75 else "MEDIUM"
        alerts.append({
            "severity": sev, "type": "SCORE",
            "trigger": f"Risk Score = {score:.1f} > 70",
            "msg": f"Composite risk score at {score:.1f}/100 ({level}). Portfolio is in elevated risk territory under {regime} regime.",
        })

    # 2. Correlation spike — eigen concentration
    eigen_c = risk_json["structural"]["eigen_concentration"]
    fragile = risk_json["structural"]["fragile"]
    if fragile:
        alerts.append({
            "severity": "HIGH", "type": "CORRELATION_SPIKE",
            "trigger": f"PC1 = {eigen_c*100:.1f}% (fragile)",
            "msg": f"PC1 concentration at {eigen_c*100:.1f}% — diversification has collapsed. All assets moving in sync increases tail risk.",
        })
    elif eigen_c > 0.45:
        alerts.append({
            "severity": "MEDIUM", "type": "CORRELATION_SPIKE",
            "trigger": f"PC1 = {eigen_c*100:.1f}%",
            "msg": f"PC1 concentration at {eigen_c*100:.1f}% — approaching fragility threshold. Monitor for further convergence.",
        })

    # 3. USD/Liquidity factor stress
    usd_z = risk_json["factors"]["details"].get("F2_USD", {}).get("zscore", 0)
    if abs(usd_z) > 1.5:
        sev = "HIGH" if abs(usd_z) > 2 else "MEDIUM"
        direction = "tightening" if usd_z > 0 else "easing"
        alerts.append({
            "severity": sev, "type": "USD_FACTOR",
            "trigger": f"F2_USD z = {usd_z:+.2f}",
            "msg": f"USD Liquidity factor z-score at {usd_z:+.2f} — {direction} pressure on African FX. {'Immediate hedging required.' if sev == 'HIGH' else 'Monitor DXY and USDZAR.'}",
        })

    # 4. Herding detected
    herding = risk_json["herding"]
    if herding["detected"]:
        alerts.append({
            "severity": "HIGH", "type": "HERDING",
            "trigger": f"CSAD H = {herding['H_coefficient']:.4f}",
            "msg": f"Statistically significant herding detected across {herding['n_stocks']} JSE stocks (H={herding['H_coefficient']:.4f}). Market-wide behavioral convergence amplifies crash risk.",
        })

    # 5. Crisis probability
    if crisis_p > 0.3:
        alerts.append({
            "severity": "HIGH", "type": "REGIME_CRISIS",
            "trigger": f"Crisis P = {crisis_p:.1%}",
            "msg": f"Crisis probability at {crisis_p:.1%}. HMM regime model assigns elevated weight to Sovereign Stress + Systemic Crisis states.",
        })

    # 6. Sovereign stress factor
    sov_z = risk_json["factors"]["details"].get("F4_Sovereign", {}).get("zscore", 0)
    if abs(sov_z) > 1.5:
        sev = "HIGH" if abs(sov_z) > 2 else "MEDIUM"
        alerts.append({
            "severity": sev, "type": "SOVEREIGN",
            "trigger": f"F4_Sovereign z = {sov_z:+.2f}",
            "msg": f"Sovereign Stress factor at {sov_z:+.2f} — CDS spreads and SA-US yield differential signal {'severe' if sev == 'HIGH' else 'elevated'} sovereign risk.",
        })

    # 7. Commodity shock
    comm_z = risk_json["factors"]["details"].get("F3_Commodity", {}).get("zscore", 0)
    shock_type = risk_json["factors"]["shock_classification"]
    if abs(comm_z) > 1.5:
        sev = "HIGH" if abs(comm_z) > 2 else "MEDIUM"
        alerts.append({
            "severity": sev, "type": "COMMODITY",
            "trigger": f"F3_Commodity z = {comm_z:+.2f}, shock = {shock_type}",
            "msg": f"Commodity factor at {comm_z:+.2f} — {shock_type} shock active. Mining and resource-heavy positions at risk.",
        })

    # 8. Jump / tail risk (high kurtosis + negative skew)
    kurt = risk_json["portfolio_risk"]["kurtosis"]
    skew = risk_json["portfolio_risk"]["skewness"]
    if kurt > 5 and skew < -0.5:
        alerts.append({
            "severity": "HIGH", "type": "JUMP_RISK",
            "trigger": f"Kurtosis = {kurt:.2f}, Skew = {skew:.2f}",
            "msg": f"Jump risk detected: kurtosis={kurt:.2f}, skew={skew:.2f}. Return distribution has fat left tail — sudden drawdowns more likely than normal model predicts.",
        })
    elif kurt > 4:
        alerts.append({
            "severity": "MEDIUM", "type": "JUMP_RISK",
            "trigger": f"Kurtosis = {kurt:.2f}",
            "msg": f"Elevated tail risk: kurtosis={kurt:.2f}. Return distribution deviates from normal — standard VaR may underestimate true risk.",
        })

    # 9. Drawdown severity
    max_dd = risk_json["portfolio_risk"]["max_drawdown"]
    if max_dd < -0.25:
        alerts.append({
            "severity": "HIGH", "type": "DRAWDOWN",
            "trigger": f"Max DD = {max_dd:.1%}",
            "msg": f"Maximum drawdown at {max_dd:.1%} — portfolio in deep drawdown territory. Recovery may require significant time.",
        })
    elif max_dd < -0.15:
        alerts.append({
            "severity": "MEDIUM", "type": "DRAWDOWN",
            "trigger": f"Max DD = {max_dd:.1%}",
            "msg": f"Drawdown at {max_dd:.1%} — portfolio experiencing notable losses from peak.",
        })

    # 10. Global risk factor
    global_z = risk_json["factors"]["details"].get("F1_Global", {}).get("zscore", 0)
    if abs(global_z) > 2:
        alerts.append({
            "severity": "HIGH", "type": "GLOBAL_RISK",
            "trigger": f"F1_Global z = {global_z:+.2f}",
            "msg": f"Global Risk factor at {global_z:+.2f} — VIX/MOVE elevated, MSCI EM under pressure. Systemic contagion risk active.",
        })

    alerts.sort(key=lambda x: {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(x["severity"], 3))
    return alerts


# ═══════════════════════════════════════════════════════════════════════
# STEP 3: Prompt Builder — structured template for Mistral
# ═══════════════════════════════════════════════════════════════════════
SYSTEM_PROMPT = """You are a senior portfolio risk analyst at an African-focused institutional asset manager.
You have deep expertise in: South African markets (JSE), macro-financial linkages, sovereign credit,
commodity cycles, FX dynamics (USDZAR), and behavioral finance (herding, momentum crashes).
You produce concise, actionable risk intelligence for portfolio managers.
Your analysis must be data-driven — reference specific numbers from the risk metrics provided.
Never fabricate data. Only reference metrics that appear in the input."""

def _build_prompt(risk_json, alerts):
    """Build Mistral-compatible instruct prompt from structured risk data."""
    rj = risk_json
    cs = rj["composite_score"]
    reg = rj["regime"]
    fac = rj["factors"]
    stru = rj["structural"]
    herd = rj["herding"]
    pr = rj["portfolio_risk"]
    stress = rj["stress_test"]

    # Format factor table
    factor_lines = []
    for f, d in fac["details"].items():
        factor_lines.append(f"  {d['label']:30s} z={d['zscore']:+.2f}  [{d['stress']}]")
    factor_table = "\n".join(factor_lines)

    # Format alerts summary
    alert_lines = []
    for a in alerts[:8]:
        alert_lines.append(f"  [{a['severity']}] {a['type']}: {a['trigger']}")
    alert_summary = "\n".join(alert_lines) if alert_lines else "  None"

    # Regime probabilities
    reg_prob_lines = []
    for state, prob in reg.get("probabilities", {}).items():
        if isinstance(prob, (int, float)):
            reg_prob_lines.append(f"  {state}: {prob:.1%}")
    reg_probs_str = "\n".join(reg_prob_lines) if reg_prob_lines else "  N/A"

    # Score components
    comp_lines = []
    for k, v in cs.get("components", {}).items():
        if isinstance(v, (int, float)):
            comp_lines.append(f"  {k}: {v:.4f}")
    comp_str = "\n".join(comp_lines) if comp_lines else "  N/A"

    data_block = f"""═══ INTELIRISK v4 — QUANTITATIVE RISK SNAPSHOT ═══
Generated: {rj['timestamp']}

COMPOSITE RISK SCORE: {cs['value']:.1f}/100 [{cs['level']}]
Score Components:
{comp_str}

REGIME (5-State HMM, {reg['hmm_features']} features):
  Active: {reg['current']} (duration: {reg['duration_days']}d)
  Crisis Probability: {reg['crisis_probability']:.1%}
  Forecast +21d: {reg['forecast_21d']}
  State Probabilities:
{reg_probs_str}
  Macro categories monitored: {', '.join(reg.get('macro_categories', {}).keys()) if reg.get('macro_categories') else 'N/A'}

AFRICAN 6-FACTOR MODEL:
  Dominant Factor: {fac['dominant']['name']} (z={fac['dominant']['zscore']:+.2f})
  Shock Classification: {fac['shock_classification']}
{factor_table}

STRUCTURAL (PCA / Network):
  PC1 Concentration: {stru['eigen_concentration']*100:.1f}%  Fragile: {stru['fragile']}
  Mean Pairwise Corr: {stru['mean_correlation']:.4f}
  Network Hub: {stru['systemic_hub']}

HERDING (CSAD — full JSE market):
  Detected: {herd['detected']}  Level: {herd['level']}
  H-coefficient: {herd['H_coefficient']:.6f}  N-stocks: {herd['n_stocks']}

PORTFOLIO RISK METRICS:
  Vol 21d: {pr['vol_21d']:.4f}  Vol 63d: {pr['vol_63d']:.4f}
  HAR Forecast: {pr['har_forecast']:.4f}
  Max Drawdown: {pr['max_drawdown']:.2%}
  VaR 5%: {pr['var_5pct']:.4f}  CVaR 5%: {pr['cvar_5pct']:.4f}
  Skewness: {pr['skewness']:.3f}  Kurtosis: {pr['kurtosis']:.3f}
  HHI: {pr['herfindahl']:.4f}  Diversification: {pr['diversification_level']}

STRESS TEST:
  Worst: {stress['worst_scenario']} (impact: {stress['worst_impact']:.3f})
  Most Vulnerable: {stress['most_vulnerable']}
  Scenarios tested: {stress['n_scenarios']}

ACTIVE ALERTS:
{alert_summary}
═══════════════════════════════════════════════════"""

    user_instruction = """Based on the quantitative risk snapshot above, produce a concise risk intelligence report with these sections:

1. **EXECUTIVE SUMMARY** (2-3 sentences): Current risk posture, dominant regime, headline risk level.
2. **REGIME ANALYSIS**: Interpret the HMM regime state, macro data feeding it, transition risks.
3. **FACTOR DECOMPOSITION**: Which factors drive risk? How do they interact? SA-US yield spread, CDS, commodity, USD implications.
4. **STRUCTURAL VULNERABILITIES**: Correlation structure, concentration, network fragility, herding assessment.
5. **TAIL RISK ASSESSMENT**: VaR/CVaR adequacy, jump risk, worst-case stress scenarios.
6. **ACTIONABLE RECOMMENDATIONS**: Specific portfolio actions ranked by urgency.

Be precise. Reference exact numbers. No filler. Think like a risk manager presenting to a CIO."""

    # Mistral instruct format: [INST] ... [/INST]
    prompt = f"<s>[INST] {SYSTEM_PROMPT}\n\n{data_block}\n\n{user_instruction} [/INST]"
    return prompt, data_block


# ═══════════════════════════════════════════════════════════════════════
# STEP 4: LLM Inference — HuggingFace API with fallback
# ═══════════════════════════════════════════════════════════════════════
def _call_hf_llm(prompt):
    """Call LLM via HuggingFace Inference API using InferenceClient.
    Tries Qwen-72B first, then Llama-3.1-8B, then gives up."""
    if not HF_TOKEN:
        return None, "No HF_TOKEN environment variable set"

    try:
        from huggingface_hub import InferenceClient
    except ImportError:
        return None, "huggingface_hub not installed"

    # Extract user content from the [INST] format
    content = prompt.replace("<s>[INST]", "").replace("[/INST]", "").strip()
    # Remove the system prompt from content (it's sent separately)
    if SYSTEM_PROMPT in content:
        content = content.replace(SYSTEM_PROMPT, "").strip()

    client = InferenceClient(token=HF_TOKEN)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": content},
    ]

    for model_id in [HF_MODEL_PRIMARY, HF_MODEL_FALLBACK]:
        try:
            response = client.chat_completion(
                messages=messages,
                model=model_id,
                max_tokens=LLM_MAX_TOKENS,
                temperature=LLM_TEMPERATURE,
                top_p=0.9,
            )
            text = response.choices[0].message.content
            if text and len(text.strip()) > 50:
                model_short = model_id.split("/")[-1]
                return text.strip(), f"{model_short} (HF Inference)"
        except Exception as e:
            err = str(e)
            log.info(f"HF model {model_id} failed: {err[:100]}")
            continue

    return None, "All HF models unavailable"


def _try_local_transformers(prompt):
    """Try local Mistral inference via transformers (if installed + enough resources)."""
    try:
        import torch
        from transformers import AutoTokenizer, AutoModelForCausalLM

        # Check if we have enough memory (need ~4GB min for quantized)
        if not torch.cuda.is_available():
            # CPU-only: try 4-bit quantized if bitsandbytes available
            try:
                from transformers import BitsAndBytesConfig
                quantization_config = BitsAndBytesConfig(load_in_4bit=True)
                model = AutoModelForCausalLM.from_pretrained(
                    HF_MODEL_FALLBACK, quantization_config=quantization_config,
                    device_map="auto", torch_dtype=torch.float32
                )
            except ImportError:
                # No quantization available, too heavy for CPU
                return None, "No GPU and no quantization library — using rule-based engine"
        else:
            model = AutoModelForCausalLM.from_pretrained(
                HF_MODEL_FALLBACK, torch_dtype=torch.float16, device_map="auto"
            )

        tokenizer = AutoTokenizer.from_pretrained(HF_MODEL_FALLBACK)
        inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=4096)
        inputs = {k: v.to(model.device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model.generate(
                **inputs, max_new_tokens=LLM_MAX_TOKENS,
                temperature=LLM_TEMPERATURE, top_p=0.9, do_sample=True,
            )
        text = tokenizer.decode(outputs[0][inputs["input_ids"].shape[1]:], skip_special_tokens=True)
        return text.strip(), None
    except ImportError:
        return None, "transformers not installed"
    except Exception as e:
        return None, f"Local inference failed: {str(e)}"


def _generate_llm_narrative(risk_json, alerts):
    """Generate LLM narrative. Try: 1) HF API, 2) Local transformers, 3) Rule-based fallback."""
    prompt, data_block = _build_prompt(risk_json, alerts)

    # Create a cache key from score + regime + level
    cache_key = f"{risk_json['composite_score']['value']:.0f}_{risk_json['regime']['current']}_{risk_json['composite_score']['level']}"

    with _llm_lock:
        if cache_key in _llm_cache:
            cached = _llm_cache[cache_key]
            if time.time() - cached["ts"] < 300:  # 5 min cache
                return cached["text"], cached["source"]

    # Try HF Inference API first (Qwen-72B → Llama-3.1-8B)
    text, source = _call_hf_llm(prompt)
    if text:
        with _llm_lock:
            _llm_cache[cache_key] = {"text": text, "source": source, "ts": time.time()}
        return text, source

    log.info(f"HF API unavailable: {source}. Trying local transformers...")

    # Try local inference
    text, err2 = _try_local_transformers(prompt)
    if text:
        with _llm_lock:
            _llm_cache[cache_key] = {"text": text, "source": "mistral-7b-instruct (local)", "ts": time.time()}
        return text, "mistral-7b-instruct (local)"

    log.info(f"Local LLM unavailable: {err2}. Using rule-based engine.")

    # Fall back to rule-based narrative
    narrative = _generate_rule_based_narrative(risk_json, alerts)
    return narrative, "InteliRisk v4 Rule Engine"


# ═══════════════════════════════════════════════════════════════════════
# STEP 5: Rule-Based Fallback — comprehensive analytical narrative
# ═══════════════════════════════════════════════════════════════════════
def _generate_rule_based_narrative(rj, alerts):
    """Generate detailed analytical narrative using rule-based engine.
    This mirrors what a tuned Mistral would produce."""
    cs = rj["composite_score"]
    reg = rj["regime"]
    fac = rj["factors"]
    stru = rj["structural"]
    herd = rj["herding"]
    pr = rj["portfolio_risk"]
    stress = rj["stress_test"]

    score = cs["value"]
    level = cs["level"]
    regime = reg["current"]
    crisis_p = reg["crisis_probability"]

    sections = []

    # 1. Executive Summary
    posture = {
        "LOW": "opportunistic", "GUARDED": "opportunistic",
        "ELEVATED": "balanced", "MODERATE": "balanced",
        "HIGH": "cautious", "SEVERE": "defensive", "CRITICAL": "defensive",
    }
    stance = posture.get(level, "balanced")

    exec_lines = [
        f"**EXECUTIVE SUMMARY**",
        f"",
        f"Portfolio risk score stands at {score:.1f}/100 ({level}), warranting a {stance} posture. "
        f"The 5-state HMM regime model, monitoring {reg['hmm_features']} features across macro, factor, and structural data, "
        f"identifies the current environment as **{regime}** (active for {reg['duration_days']} days). "
        f"Crisis probability (Sovereign Stress + Systemic Crisis combined) is {crisis_p:.1%}.",
    ]
    sections.append("\n".join(exec_lines))

    # 2. Regime Analysis
    reg_lines = [
        f"**REGIME ANALYSIS**",
        f"",
        f"The Hidden Markov Model operates on {reg['hmm_features']} features spanning portfolio volatility, "
        f"factor z-scores, and critical macro indicators (VIX, CDS spreads, SA-US yield differential, "
        f"DXY, USDZAR, oil, gold, copper, MOVE index, JIBAR, repo rate). ",
    ]
    if regime == "Sovereign Stress":
        reg_lines.append(
            f"The Sovereign Stress regime reflects elevated CDS spreads, widening SA-US yield differential, "
            f"and fiscal pressure. This regime historically precedes either stabilization (if policy response is adequate) "
            f"or escalation to Systemic Crisis (if external shocks compound domestic fragility)."
        )
    elif regime == "Systemic Crisis":
        reg_lines.append(
            f"Systemic Crisis is the most severe regime — characterized by VIX spikes, CDS blowout, "
            f"correlation convergence, and capital flight from EM. Immediate defensive positioning is critical."
        )
    elif regime == "USD Tightening":
        reg_lines.append(
            f"USD Tightening regime reflects DXY strength and USDZAR depreciation pressuring ZAR assets. "
            f"Fed policy and global dollar liquidity are the key drivers."
        )
    elif regime == "Commodity Expansion":
        reg_lines.append(
            f"Commodity Expansion benefits resource-heavy JSE sectors (mining, energy). "
            f"Upside in AGL, BHP, IMP, but watch for reversal signals in oil/gold."
        )
    else:
        reg_lines.append(
            f"Stable Growth is the most benign regime — low volatility, contained spreads, "
            f"positive EM flows. Suitable for selective risk-taking."
        )
    reg_lines.append(f"21-day regime forecast: **{reg['forecast_21d']}**.")
    sections.append("\n".join(reg_lines))

    # 3. Factor Decomposition
    dom = fac["dominant"]
    fac_lines = [
        f"**FACTOR DECOMPOSITION**",
        f"",
        f"The dominant risk driver is **{dom['name']}** (z={dom['zscore']:+.2f}). "
        f"Shock classification: **{fac['shock_classification']}**.",
        f"",
    ]
    stressed = [(f, d) for f, d in fac["details"].items() if d["stress"] != "NORMAL"]
    if stressed:
        fac_lines.append("Factors under stress:")
        for f, d in stressed:
            fac_lines.append(f"  • {d['label']}: z={d['zscore']:+.2f} [{d['stress']}] — {_factor_interpretation(f, d['zscore'])}")
    else:
        fac_lines.append("All factors within normal bounds — no tail stress active.")
    sections.append("\n".join(fac_lines))

    # 4. Structural Vulnerabilities
    struct_lines = [
        f"**STRUCTURAL VULNERABILITIES**",
        f"",
        f"PC1 explains {stru['eigen_concentration']*100:.1f}% of portfolio variance" +
        (" — **FRAGILE**: diversification has effectively collapsed." if stru["fragile"] else "."),
        f"Mean pairwise correlation: {stru['mean_correlation']:.4f}. "
        f"Network hub (most systemically connected): {stru['systemic_hub']}.",
    ]
    if herd["detected"]:
        struct_lines.append(
            f"**HERDING ACTIVE**: CSAD analysis across {herd['n_stocks']} JSE stocks shows statistically significant "
            f"herding behavior (H={herd['H_coefficient']:.4f}). This amplifies crash risk as market participants "
            f"converge on similar positions."
        )
    else:
        struct_lines.append(
            f"Herding: {herd['level']} (H={herd['H_coefficient']:.4f}, {herd['n_stocks']} stocks). "
            f"No significant behavioral convergence detected."
        )
    sections.append("\n".join(struct_lines))

    # 5. Tail Risk Assessment
    tail_lines = [
        f"**TAIL RISK ASSESSMENT**",
        f"",
        f"Portfolio VaR(5%): {pr['var_5pct']:.4f}, CVaR(5%): {pr['cvar_5pct']:.4f}. "
        f"CVaR/VaR ratio: {abs(pr['cvar_5pct']/pr['var_5pct']):.2f}x." if pr['var_5pct'] != 0 else f"VaR/CVaR: insufficient data.",
        f"Distribution: skewness={pr['skewness']:.3f}, kurtosis={pr['kurtosis']:.3f}. "
        + ("Fat-tailed and negatively skewed — standard VaR underestimates true risk." if pr['kurtosis'] > 4 and pr['skewness'] < -0.5
           else "Distribution within normal range." if pr['kurtosis'] < 4 else "Elevated kurtosis suggests fat tails."),
        f"HAR volatility forecast: {pr['har_forecast']:.4f} (vs 21d realized: {pr['vol_21d']:.4f}).",
        f"Max drawdown: {pr['max_drawdown']:.2%}.",
        f"Worst stress scenario: **{stress['worst_scenario']}** (impact: {stress['worst_impact']:.3f}). "
        f"Most vulnerable sector/asset: {stress['most_vulnerable']} across {stress['n_scenarios']} historical scenarios.",
    ]
    sections.append("\n".join(tail_lines))

    # 6. Recommendations
    rec_lines = [
        f"**ACTIONABLE RECOMMENDATIONS**",
        f"",
    ]
    rec_lines.extend(_generate_recommendations(rj, alerts))
    sections.append("\n".join(rec_lines))

    return "\n\n".join(sections)


def _factor_interpretation(factor_name, z):
    """Context-aware interpretation of factor z-scores."""
    interp = {
        "F1_Global": "VIX/MOVE elevated, MSCI EM under pressure — global risk-off" if z > 0 else "Global risk-on, supportive for EM flows",
        "F2_USD": "Dollar strengthening, USDZAR under pressure — FX hedging urgency" if z > 0 else "Dollar weakening, ZAR-supportive environment",
        "F3_Commodity": "Commodity stress (supply-side or demand collapse)" if z > 0 else "Commodity weakness weighing on resource exporters",
        "F4_Sovereign": "Sovereign credit stress — CDS widening, SA-US yield spread expanding" if z > 0 else "Sovereign improvement, spread compression",
        "F5_Domestic": "Domestic concentration risk elevated" if z > 0 else "Domestic factors easing, idiosyncratic risk contained",
        "F6_Herding": "Behavioral convergence increasing crash risk" if z > 0 else "Low herding, healthy market microstructure",
    }
    return interp.get(factor_name, f"z-score at {z:+.2f}")


def _generate_recommendations(rj, alerts):
    """Generate prioritized recommendations based on current risk state."""
    recs = []
    level = rj["composite_score"]["level"]
    regime = rj["regime"]["current"]
    herd = rj["herding"]
    score = rj["composite_score"]["value"]

    if level in ("CRITICAL", "SEVERE"):
        recs.extend([
            "1. **IMMEDIATE**: Reduce all high-beta and systemically connected positions by 30-50%.",
            "2. **URGENT**: Implement FX hedges across all ZAR-denominated exposures (DXY/USDZAR protection).",
            "3. **URGENT**: Rotate to sovereign-backed or hard-currency instruments (USD money market, SA government bonds).",
            "4. **TODAY**: Review all concentration limits — enforce max 10% single-name exposure.",
            "5. **TODAY**: Set stop-loss at -2σ on every remaining position.",
            "6. Monitor regime transition probabilities daily — escalation to Systemic Crisis requires emergency protocol.",
        ])
    elif level == "HIGH":
        recs.extend([
            "1. Trim overweight positions in stressed sectors — prioritize mining if commodity factor elevated.",
            "2. Hedge commodity exposure if shock classification = SUPPLY or DEMAND.",
            "3. Diversify across less correlated markets (Morocco MASI, Egypt EGX offer low-beta alternatives).",
            "4. Monitor regime forecast — prepare contingency for possible CRITICAL escalation.",
            "5. Increase review frequency to daily. Set VaR breach alerts.",
        ])
        if regime == "Sovereign Stress":
            recs.append("6. Watch SA-US yield spread and CDS closely — widening above 300bps triggers defensive rotation.")
    elif level == "MODERATE":
        recs.extend([
            "1. Maintain strategic allocation with regular weekly monitoring.",
            "2. Seek selective value in undervalued sectors (check factor z-scores for entry signals).",
            "3. Rebalance to target weights if drift exceeds 5%.",
            "4. Monitor regime transitions and factor z-scores — prepare for HIGH scenario.",
        ])
    else:  # LOW
        recs.extend([
            "1. Current conditions support selective risk-taking — consider increasing equity allocation.",
            "2. Exploit cross-market dispersion for relative value trades.",
            "3. Build watchlist for tactical entries on any regime shift signals.",
            "4. Review portfolio for strategic rebalancing opportunities.",
        ])

    if herd["detected"]:
        recs.append(f"⚠ HERDING ALERT: With significant herding detected, avoid adding to crowded positions. Consider contrarian positions in under-owned names.")

    return recs


# ═══════════════════════════════════════════════════════════════════════
# STEP 6: Posture & Actions (deterministic, always computed)
# ═══════════════════════════════════════════════════════════════════════
def _compute_posture(level):
    posture_map = {
        "LOW": "OPPORTUNISTIC", "GUARDED": "OPPORTUNISTIC",
        "ELEVATED": "BALANCED", "MODERATE": "BALANCED",
        "HIGH": "CAUTIOUS", "SEVERE": "DEFENSIVE", "CRITICAL": "DEFENSIVE",
    }
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
    return posture, actions.get(posture, [])


# ═══════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════
def compute_layer7(l0_data, l2_data, l3_data, l4_data, l5_data, l6_data):
    """Compute Layer 7 — AI-Powered Intelligence Report.

    Pipeline: L0-L6 data → Structured JSON → Alert Engine → Prompt Builder →
              LLM (Mistral-7B) → AI Narrative → Merged with deterministic output → Dashboard
    """
    # Step 1: Build structured risk JSON from all layers
    risk_json = _build_risk_json(l0_data, l2_data, l3_data, l4_data, l5_data, l6_data)

    # Step 2: Generate alerts (rules-based + AI triggers)
    alerts = _generate_alerts(risk_json)

    # Step 3-4: Generate LLM narrative (HF API → local → rule-based fallback)
    ai_narrative, ai_source = _generate_llm_narrative(risk_json, alerts)

    # Step 5: Deterministic posture & actions
    level = risk_json["composite_score"]["level"]
    score = risk_json["composite_score"]["value"]
    regime = risk_json["regime"]["current"]
    crisis_p = risk_json["regime"]["crisis_probability"]
    forecast = risk_json["regime"]
    herding = risk_json["herding"]
    posture, actions = _compute_posture(level)

    # Headlines (7 risk levels)
    headlines = {
        "LOW": f"Continental risk score at {score:.0f}/100 — low risk. {regime} regime supports stable conditions. Favorable for strategic positioning.",
        "GUARDED": f"Risk score {score:.0f}/100 — guarded. {regime} regime is largely benign. Selective opportunities available.",
        "ELEVATED": f"Risk score {score:.0f}/100 — elevated. Under {regime} regime, monitor factor z-scores and regime transitions.",
        "MODERATE": f"Risk score {score:.0f}/100 — moderate. Under {regime} regime, maintain vigilance and monitor factor z-scores.",
        "HIGH": f"WARNING: Risk score {score:.0f}/100 — high. {regime} regime amplifies vulnerabilities. Reduce concentrated exposures.",
        "SEVERE": f"SEVERE: Risk score {score:.0f}/100. {regime} regime indicates significant stress. Defensive positioning required.",
        "CRITICAL": f"CRITICAL: Risk score {score:.0f}/100. {regime} regime signals extreme stress. Immediate emergency action required.",
    }

    return {
        "engine": "InteliRisk v4.0 + AI Intelligence",
        "ai_source": ai_source,
        "generated_at": datetime.now().isoformat(),
        "headline": headlines.get(level, headlines["MODERATE"]),
        "score": score,
        "level": level,
        "color": l5_data.get("color", "#C9A227"),
        "active_regime": regime,
        "crisis_probability": crisis_p,
        "regime_forecast": l4_data.get("forecast", {}),
        "regime_duration": risk_json["regime"]["duration_days"],
        "hmm_features": risk_json["regime"]["hmm_features"],
        "dominant_factor": l3_data.get("dominant_factor", {}),
        "shock_classification": risk_json["factors"]["shock_classification"],
        "herding": {
            "detected": herding["detected"],
            "level": herding["level"],
            "H": herding["H_coefficient"],
            "n_stocks": herding["n_stocks"],
        },
        "diversification": {
            "hhi": risk_json["portfolio_risk"]["herfindahl"],
            "level": risk_json["portfolio_risk"]["diversification_level"],
        },
        "stress_summary": {
            "worst_scenario": risk_json["stress_test"]["worst_scenario"],
            "worst_impact": risk_json["stress_test"]["worst_impact"],
            "most_vulnerable": risk_json["stress_test"]["most_vulnerable"],
            "n_scenarios": risk_json["stress_test"]["n_scenarios"],
        },
        "alerts": alerts,
        "recommendation": {"posture": posture, "actions": actions},
        "country_scores": l5_data.get("country_scores", {}),
        # AI Intelligence — the new LLM-powered section
        "ai_narrative": ai_narrative,
        "risk_snapshot": risk_json,
        # Legacy narrative_sections (kept for backward compat)
        "narrative_sections": _parse_narrative_sections(ai_narrative, risk_json),
    }


def _parse_narrative_sections(narrative, risk_json):
    """Parse AI narrative into structured sections for frontend display."""
    sections = []
    if not narrative:
        return sections

    # Try to split by markdown headers
    current_title = "Analysis"
    current_content = []

    for line in narrative.split("\n"):
        line_stripped = line.strip()
        # Match ### headers (from Qwen/LLM) or **BOLD** section headers
        is_header = False
        header_text = ""
        if line_stripped.startswith("###"):
            is_header = True
            header_text = line_stripped.lstrip("#").strip()
        elif line_stripped.startswith("**") and line_stripped.endswith("**"):
            is_header = True
            header_text = line_stripped.strip("*").strip()

        if is_header:
            if current_content:
                sections.append({"title": current_title, "content": "\n".join(current_content).strip()})
                current_content = []
            current_title = header_text
        else:
            current_content.append(line)

    if current_content:
        sections.append({"title": current_title, "content": "\n".join(current_content).strip()})

    # Ensure we always have at least basic sections
    if not sections:
        cs = risk_json["composite_score"]
        reg = risk_json["regime"]
        sections = [
            {"title": "Market Overview", "content": f"Risk score: {cs['value']:.0f}/100 ({cs['level']}). Regime: {reg['current']}."},
            {"title": "Analysis", "content": narrative[:500] if narrative else "Awaiting data."},
        ]

    return sections
