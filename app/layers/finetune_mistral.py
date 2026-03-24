"""
LoRA Fine-Tuning Pipeline for Mistral-7B — InteliRisk v4 Domain Adaptation

Fine-tunes Mistral-7B-Instruct-v0.1 on system-generated risk analysis data
using LoRA (Low-Rank Adaptation) for parameter-efficient training.

Requirements: pip install transformers peft datasets trl torch accelerate bitsandbytes

Usage:
    python -m app.layers.finetune_mistral --generate-data    # Generate training dataset
    python -m app.layers.finetune_mistral --train             # Run LoRA fine-tuning
    python -m app.layers.finetune_mistral --merge             # Merge adapter into base model
"""
import os, json, argparse, logging
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "finetune"
MODEL_DIR = BASE_DIR / "models"
ADAPTER_DIR = MODEL_DIR / "mistral-afrisk-lora"
MERGED_DIR = MODEL_DIR / "mistral-afrisk-merged"
TRAINING_DATA = DATA_DIR / "risk_analysis_train.jsonl"

# Model config
BASE_MODEL = "mistralai/Mistral-7B-Instruct-v0.1"

# LoRA config
LORA_R = 8
LORA_ALPHA = 16
LORA_DROPOUT = 0.05
LORA_TARGET_MODULES = ["q_proj", "v_proj"]

# Training config
TRAIN_EPOCHS = 3
TRAIN_BATCH_SIZE = 2
TRAIN_LR = 2e-4
TRAIN_MAX_SEQ_LEN = 2048
GRADIENT_ACCUMULATION_STEPS = 4


# ═══════════════════════════════════════════════════════════════════════
# Training Data Generation — from system outputs
# ═══════════════════════════════════════════════════════════════════════
# Training examples that teach Mistral about African risk analysis
TRAINING_EXAMPLES = [
    {
        "scenario": "Sovereign Stress with high score",
        "score": 76, "level": "CRITICAL", "regime": "Sovereign Stress",
        "crisis_p": 0.85, "duration": 20, "hmm_features": 25,
        "factors": {"F1_Global": 1.2, "F2_USD": 0.8, "F3_Commodity": -0.3, "F4_Sovereign": 2.1, "F5_Domestic": 0.5, "F6_Herding": -0.01},
        "vol_21d": 0.22, "max_dd": -0.36, "var_5": -0.032, "cvar_5": -0.048,
        "herding": False, "h_coeff": -0.01, "pc1": 0.55, "shock": "MIXED",
        "response": """**EXECUTIVE SUMMARY**

Portfolio risk at 76/100 (CRITICAL) demands defensive positioning. The 5-state HMM identifies Sovereign Stress regime (20 days duration) driven by CDS widening and SA-US yield spread expansion. Crisis probability at 85% signals potential escalation.

**REGIME ANALYSIS**

The HMM processes 25 features including VIX, CDS spreads, SA-US yield differential, DXY, USDZAR, and commodity prices. Sovereign Stress is characterized by fiscal pressure, rating agency scrutiny, and capital outflow from SA bonds. The SA-US yield spread is the critical monitored variable — widening indicates foreign investor repricing of SA sovereign risk.

**FACTOR DECOMPOSITION**

Sovereign Stress factor dominates (z=+2.10) — CDS spreads widening, SA-US yield differential expanding. Global Risk elevated (z=+1.20) adds external pressure. Commodity factor neutral (-0.30) provides no buffer. USD Liquidity (z=+0.80) approaching stress territory.

**STRUCTURAL VULNERABILITIES**

PC1 at 55% — approaching fragility threshold. No significant herding (H=-0.01). Market microstructure healthy but correlation structure tightening under sovereign pressure.

**TAIL RISK ASSESSMENT**

VaR(5%): -3.2%, CVaR(5%): -4.8%. Max drawdown: -36%. Under sovereign stress, standard VaR may underestimate — CDS-driven selloffs tend to exhibit gap-down behavior not captured in normal distribution assumptions.

**ACTIONABLE RECOMMENDATIONS**

1. IMMEDIATE: Reduce SA sovereign bond exposure and increase hard-currency allocation.
2. URGENT: Hedge USDZAR at current levels — sovereign stress amplifies ZAR depreciation.
3. Trim mining exposure (commodity buffer minimal), maintain banking only if systemically important names.
4. Monitor CDS daily — breach above 350bps triggers full defensive protocol.
5. Set VaR breach alerts at 1.5x current level."""
    },
    {
        "scenario": "Stable Growth with low risk",
        "score": 28, "level": "LOW", "regime": "Stable Growth",
        "crisis_p": 0.05, "duration": 45, "hmm_features": 25,
        "factors": {"F1_Global": -0.5, "F2_USD": -0.3, "F3_Commodity": 0.4, "F4_Sovereign": -0.8, "F5_Domestic": 0.2, "F6_Herding": 0.01},
        "vol_21d": 0.12, "max_dd": -0.08, "var_5": -0.015, "cvar_5": -0.022,
        "herding": False, "h_coeff": 0.01, "pc1": 0.32, "shock": "SUPPLY",
        "response": """**EXECUTIVE SUMMARY**

Risk score at 28/100 (LOW) — conditions are favorable. Stable Growth regime has persisted for 45 days with crisis probability at just 5%. This is the optimal window for strategic positioning and selective risk-taking.

**REGIME ANALYSIS**

HMM confirms Stable Growth — VIX suppressed, CDS tight, SA-US spread compressed. The 25-feature model shows all macro indicators within benign ranges. Regime duration of 45 days suggests sustainability, though all stable regimes eventually transition.

**FACTOR DECOMPOSITION**

All factors within normal bounds. Sovereign factor most supportive (z=-0.80) — SA credit improving. Mild commodity tailwind (z=+0.40, SUPPLY classification) supports mining sector. Global risk benign (z=-0.50).

**STRUCTURAL VULNERABILITIES**

PC1 at 32% — healthy diversification. Low correlation, no herding. Portfolio benefits from genuine diversification across sectors.

**TAIL RISK ASSESSMENT**

VaR(5%): -1.5%, CVaR(5%): -2.2%. Max drawdown only -8%. Tail risk contained. HAR forecast confirms low forward vol.

**ACTIONABLE RECOMMENDATIONS**

1. Increase equity allocation — conditions support higher beta.
2. Consider adding to commodity-exposed names (AGL, BHP) given supply-side support.
3. Rebalance to target weights — drift in calm markets creates opportunity.
4. Build watchlist for tactical entries if regime shifts.
5. Review hedges — current protection may be over-hedged relative to risk environment."""
    },
    {
        "scenario": "Commodity Expansion with moderate risk",
        "score": 52, "level": "MODERATE", "regime": "Commodity Expansion",
        "crisis_p": 0.15, "duration": 30, "hmm_features": 25,
        "factors": {"F1_Global": 0.3, "F2_USD": -0.6, "F3_Commodity": 1.8, "F4_Sovereign": 0.4, "F5_Domestic": 0.7, "F6_Herding": 0.02},
        "vol_21d": 0.16, "max_dd": -0.15, "var_5": -0.021, "cvar_5": -0.031,
        "herding": False, "h_coeff": 0.02, "pc1": 0.42, "shock": "DEMAND",
        "response": """**EXECUTIVE SUMMARY**

Risk score at 52/100 (MODERATE) under Commodity Expansion regime (30 days). Mining-heavy portfolios benefit, but elevated commodity factor (z=+1.80) with DEMAND shock classification warrants sector monitoring.

**REGIME ANALYSIS**

HMM identifies Commodity Expansion — driven by rising oil, gold, and base metals. This regime benefits JSE resource stocks but creates concentration risk in portfolios overweight mining. USD weakness (z=-0.60) amplifies ZAR returns.

**FACTOR DECOMPOSITION**

Commodity factor dominant (z=+1.80, DEMAND shock) — China/global demand driving prices. USD easing (z=-0.60) reinforces commodity strength. Domestic concentration (z=+0.70) slightly elevated due to mining weighting in JSE index.

**STRUCTURAL VULNERABILITIES**

PC1 at 42% — moderate concentration. Mining stocks increasingly correlated during commodity booms. Watch for herding signals in resource names.

**TAIL RISK ASSESSMENT**

VaR(5%): -2.1%, CVaR(5%): -3.1%. Max drawdown: -15%. Commodity cycles can reverse sharply — DEMAND shocks are more persistent than SUPPLY but reversal carries significant downside.

**ACTIONABLE RECOMMENDATIONS**

1. Take profits in extended mining positions (AGL, BHP, IMP approaching cycle highs).
2. Diversify within commodities — gold offers hedge if demand narrative reverses.
3. Monitor China PMI and global demand indicators weekly.
4. Maintain balanced sector allocation — avoid >30% mining concentration.
5. Set trailing stops on commodity-exposed positions at -10% from peak."""
    },
    {
        "scenario": "Systemic Crisis with herding",
        "score": 92, "level": "CRITICAL", "regime": "Systemic Crisis",
        "crisis_p": 0.95, "duration": 5, "hmm_features": 25,
        "factors": {"F1_Global": 3.5, "F2_USD": 2.8, "F3_Commodity": -2.5, "F4_Sovereign": 3.2, "F5_Domestic": -2.0, "F6_Herding": 0.08},
        "vol_21d": 0.42, "max_dd": -0.45, "var_5": -0.058, "cvar_5": -0.089,
        "herding": True, "h_coeff": 0.08, "pc1": 0.78, "shock": "FINANCIAL",
        "response": """**EXECUTIVE SUMMARY**

CRITICAL: Score 92/100. Systemic Crisis regime activated 5 days ago. All correlations converging (PC1=78%), herding significant (H=0.08), VIX extreme. This is a full risk-off environment requiring immediate defensive action.

**REGIME ANALYSIS**

HMM has transitioned to Systemic Crisis — the most severe of 5 states. All 25 features confirm: VIX spike, CDS blowout, SA-US spread at crisis levels, DXY surging, commodity collapse. Duration only 5 days suggests we are in the acute phase.

**FACTOR DECOMPOSITION**

All factors in extreme territory. Global Risk (z=+3.50) — VIX/MOVE at crisis levels. USD (z=+2.80) — dollar shortage, EM capital flight. Sovereign (z=+3.20) — CDS and yields blown out. Commodities (z=-2.50) — FINANCIAL shock (liquidity-driven, not fundamentals). HERDING (z=+0.08) — statistically significant market-wide panic.

**STRUCTURAL VULNERABILITIES**

PC1 at 78% — FRAGILE. Diversification has collapsed entirely. All assets moving in lockstep. Network hub is the most connected stock — contagion propagates instantly. Herding across JSE confirmed — institutional investors are in synchronized sell mode.

**TAIL RISK ASSESSMENT**

VaR(5%): -5.8%, CVaR(5%): -8.9%. Max drawdown: -45%. These metrics likely UNDERESTIMATE true risk in crisis — gap moves, liquidity black holes, and forced selling create non-linear losses beyond model estimates.

**ACTIONABLE RECOMMENDATIONS**

1. EMERGENCY: Cut all equity exposure by 50% minimum. Cash is the primary hedge.
2. IMMEDIATE: Full FX hedge on all ZAR exposure. USDZAR will gap.
3. IMMEDIATE: Exit illiquid small-caps entirely — bid-ask spreads will blow out.
4. Hold only: SA government bonds (short duration), USD money market, gold.
5. Do NOT attempt to catch the bottom — wait for regime transition confirmation.
6. Daily monitoring. Intraday if possible. Crisis duration is typically 10-30 days."""
    },
    {
        "scenario": "USD Tightening with elevated risk",
        "score": 65, "level": "HIGH", "regime": "USD Tightening",
        "crisis_p": 0.35, "duration": 15, "hmm_features": 25,
        "factors": {"F1_Global": 1.0, "F2_USD": 2.3, "F3_Commodity": -0.8, "F4_Sovereign": 1.5, "F5_Domestic": -0.3, "F6_Herding": -0.005},
        "vol_21d": 0.19, "max_dd": -0.22, "var_5": -0.026, "cvar_5": -0.039,
        "herding": False, "h_coeff": -0.005, "pc1": 0.48, "shock": "MIXED",
        "response": """**EXECUTIVE SUMMARY**

Risk score 65/100 (HIGH) under USD Tightening regime (15 days). Dollar strength (F2_USD z=+2.30) is the dominant risk factor, pressuring USDZAR and SA asset values. Crisis probability at 35% warrants cautious positioning.

**REGIME ANALYSIS**

HMM identifies USD Tightening — driven by Fed hawkishness, DXY strength, and EM capital rotation. This regime persists as long as US rate expectations remain elevated. USDZAR is the key transmission mechanism for SA portfolios.

**FACTOR DECOMPOSITION**

USD Liquidity dominates (z=+2.30) — DXY surge and USDZAR depreciation are the primary risk channels. Sovereign stress elevated (z=+1.50) as USD tightening widens SA spreads. Commodities weakening (z=-0.80) under strong dollar pressure.

**STRUCTURAL VULNERABILITIES**

PC1 at 48% — elevated but not fragile. FX-sensitive stocks (dual-listed, USD-earners) becoming decorrelated from ZAR-domestic names, creating a two-speed market.

**TAIL RISK ASSESSMENT**

VaR(5%): -2.6%, CVaR(5%): -3.9%. Max drawdown: -22%. USD tightening cycles are typically 3-6 months — extended pain rather than acute shock. Tail risk is in USDZAR gap moves on surprise Fed actions.

**ACTIONABLE RECOMMENDATIONS**

1. FX hedge: Minimum 50% of ZAR exposure should be hedged at current USDZAR levels.
2. Overweight USD-earners (BHP, Richemont) relative to ZAR-domestic names.
3. Reduce duration on SA bonds — rising US yields compress SA bond prices.
4. Monitor Fed communications and DXY daily — regime persistence depends on US policy.
5. Prepare for potential escalation to Sovereign Stress if USDZAR breaks key resistance."""
    },
]


def _format_training_prompt(example):
    """Format a training example into Mistral instruct format."""
    fac = example["factors"]
    factor_table = "\n".join([
        f"  {k:20s} z={v:+.2f}  [{'TAIL' if abs(v)>2 else 'ELEVATED' if abs(v)>1.5 else 'NORMAL'}]"
        for k, v in fac.items()
    ])

    data_block = f"""═══ INTELIRISK v4 — QUANTITATIVE RISK SNAPSHOT ═══
COMPOSITE RISK SCORE: {example['score']}/100 [{example['level']}]

REGIME (5-State HMM, {example['hmm_features']} features):
  Active: {example['regime']} (duration: {example['duration']}d)
  Crisis Probability: {example['crisis_p']:.1%}

AFRICAN 6-FACTOR MODEL:
  Shock Classification: {example['shock']}
{factor_table}

HERDING (CSAD — full JSE market):
  Detected: {example['herding']}  H-coefficient: {example['h_coeff']:.4f}

PORTFOLIO RISK METRICS:
  Vol 21d: {example['vol_21d']:.4f}  Max Drawdown: {example['max_dd']:.2%}
  VaR 5%: {example['var_5']:.4f}  CVaR 5%: {example['cvar_5']:.4f}

STRUCTURAL:
  PC1 Concentration: {example['pc1']*100:.1f}%
═══════════════════════════════════════════════════"""

    system = """You are a senior portfolio risk analyst at an African-focused institutional asset manager.
You have deep expertise in: South African markets (JSE), macro-financial linkages, sovereign credit,
commodity cycles, FX dynamics (USDZAR), and behavioral finance (herding, momentum crashes).
You produce concise, actionable risk intelligence for portfolio managers.
Your analysis must be data-driven — reference specific numbers from the risk metrics provided."""

    instruction = """Based on the quantitative risk snapshot above, produce a concise risk intelligence report with these sections:
1. EXECUTIVE SUMMARY, 2. REGIME ANALYSIS, 3. FACTOR DECOMPOSITION,
4. STRUCTURAL VULNERABILITIES, 5. TAIL RISK ASSESSMENT, 6. ACTIONABLE RECOMMENDATIONS."""

    prompt = f"<s>[INST] {system}\n\n{data_block}\n\n{instruction} [/INST]"
    return prompt, example["response"]


def generate_training_data():
    """Generate JSONL training dataset from system examples."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    records = []
    for ex in TRAINING_EXAMPLES:
        prompt, response = _format_training_prompt(ex)
        records.append({
            "text": f"{prompt}{response}</s>",
            "scenario": ex["scenario"],
        })

    with open(TRAINING_DATA, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    print(f"[FINETUNE] Generated {len(records)} training examples → {TRAINING_DATA}")
    return TRAINING_DATA


def run_finetuning():
    """Run LoRA fine-tuning on Mistral-7B-Instruct with generated training data."""
    try:
        import torch
        from transformers import AutoTokenizer, AutoModelForCausalLM, TrainingArguments
        from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training
        from datasets import load_dataset
        from trl import SFTTrainer
    except ImportError as e:
        print(f"[FINETUNE] Missing dependencies: {e}")
        print("Install: pip install transformers peft datasets trl torch accelerate bitsandbytes")
        return

    if not TRAINING_DATA.exists():
        print("[FINETUNE] Training data not found. Generating...")
        generate_training_data()

    print(f"[FINETUNE] Loading base model: {BASE_MODEL}")
    print(f"[FINETUNE] LoRA config: r={LORA_R}, alpha={LORA_ALPHA}, targets={LORA_TARGET_MODULES}")

    # Load tokenizer
    tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL)
    tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "right"

    # Load model with quantization for memory efficiency
    model_kwargs = {"torch_dtype": torch.float16, "device_map": "auto"}
    try:
        from transformers import BitsAndBytesConfig
        model_kwargs["quantization_config"] = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=torch.float16,
            bnb_4bit_use_double_quant=True,
        )
        print("[FINETUNE] Using 4-bit quantization (QLoRA)")
    except ImportError:
        print("[FINETUNE] bitsandbytes not available, using float16")

    model = AutoModelForCausalLM.from_pretrained(BASE_MODEL, **model_kwargs)

    # Prepare for k-bit training
    try:
        model = prepare_model_for_kbit_training(model)
    except Exception:
        pass

    # LoRA config
    lora_config = LoraConfig(
        r=LORA_R,
        lora_alpha=LORA_ALPHA,
        lora_dropout=LORA_DROPOUT,
        target_modules=LORA_TARGET_MODULES,
        bias="none",
        task_type="CAUSAL_LM",
    )

    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()

    # Load dataset
    dataset = load_dataset("json", data_files=str(TRAINING_DATA), split="train")
    print(f"[FINETUNE] Training on {len(dataset)} examples")

    # Training arguments
    ADAPTER_DIR.mkdir(parents=True, exist_ok=True)
    training_args = TrainingArguments(
        output_dir=str(ADAPTER_DIR),
        num_train_epochs=TRAIN_EPOCHS,
        per_device_train_batch_size=TRAIN_BATCH_SIZE,
        gradient_accumulation_steps=GRADIENT_ACCUMULATION_STEPS,
        learning_rate=TRAIN_LR,
        fp16=torch.cuda.is_available(),
        logging_steps=1,
        save_strategy="epoch",
        warmup_ratio=0.05,
        optim="paged_adamw_8bit" if torch.cuda.is_available() else "adamw_torch",
        report_to="none",
    )

    # Trainer
    trainer = SFTTrainer(
        model=model,
        tokenizer=tokenizer,
        train_dataset=dataset,
        args=training_args,
        max_seq_length=TRAIN_MAX_SEQ_LEN,
        dataset_text_field="text",
    )

    print("[FINETUNE] Starting training...")
    trainer.train()

    # Save adapter
    model.save_pretrained(str(ADAPTER_DIR))
    tokenizer.save_pretrained(str(ADAPTER_DIR))
    print(f"[FINETUNE] LoRA adapter saved → {ADAPTER_DIR}")


def merge_adapter():
    """Merge LoRA adapter back into base model for faster inference."""
    try:
        import torch
        from transformers import AutoTokenizer, AutoModelForCausalLM
        from peft import PeftModel
    except ImportError as e:
        print(f"[MERGE] Missing dependencies: {e}")
        return

    if not ADAPTER_DIR.exists():
        print(f"[MERGE] Adapter not found at {ADAPTER_DIR}. Run training first.")
        return

    print(f"[MERGE] Loading base model: {BASE_MODEL}")
    model = AutoModelForCausalLM.from_pretrained(
        BASE_MODEL, torch_dtype=torch.float16, device_map="auto"
    )
    tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL)

    print(f"[MERGE] Loading LoRA adapter from {ADAPTER_DIR}")
    model = PeftModel.from_pretrained(model, str(ADAPTER_DIR))

    print("[MERGE] Merging adapter into base model...")
    model = model.merge_and_unload()

    MERGED_DIR.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(str(MERGED_DIR))
    tokenizer.save_pretrained(str(MERGED_DIR))
    print(f"[MERGE] Merged model saved → {MERGED_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="InteliRisk Mistral Fine-Tuning Pipeline")
    parser.add_argument("--generate-data", action="store_true", help="Generate training dataset")
    parser.add_argument("--train", action="store_true", help="Run LoRA fine-tuning")
    parser.add_argument("--merge", action="store_true", help="Merge adapter into base model")
    args = parser.parse_args()

    if args.generate_data:
        generate_training_data()
    elif args.train:
        run_finetuning()
    elif args.merge:
        merge_adapter()
    else:
        parser.print_help()
