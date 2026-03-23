"""AfriSK Configuration — InteliRisk v4 Engine"""
import os

SECRET_KEY = "afrisk-2024-secret"
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 8
UPLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "uploads")
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "intellirisk")

N_ANN = 252
WIN_SHORT = 21
WIN_MED = 63
WIN_LONG = 126
WIN_VAR = 252

MARKETS = {
    "JSE_SA":  {"name": "JSE (South Africa)", "short": "JSE",  "currency": "ZAR", "color": "#1F4E79"},
    "MASI_MA": {"name": "MASI (Morocco)",     "short": "MASI", "currency": "MAD", "color": "#2E75B6"},
    "EGX_EG":  {"name": "EGX (Egypt)",        "short": "EGX",  "currency": "EGP", "color": "#C9A227"},
    "NGX_NG":  {"name": "NGX (Nigeria)",      "short": "NGX",  "currency": "NGN", "color": "#E67E22"},
}

REGIME_COUNT = 5
REGIME_LABELS = {0: "Stable Growth", 1: "Commodity Expansion", 2: "USD Tightening", 3: "Sovereign Stress", 4: "Systemic Crisis"}
REGIME_COLORS = {0: "#1E6B2E", 1: "#2E75B6", 2: "#C9A227", 3: "#E67E22", 4: "#C0392B"}

FACTOR_META = {
    "F1_Global":    {"label": "Global Risk",            "cls": "GLOBAL",     "color": "#1F4E79"},
    "F2_USD":       {"label": "USD Liquidity",          "cls": "GLOBAL",     "color": "#2E75B6"},
    "F3_Commodity": {"label": "Commodity Shock",        "cls": "GLOBAL",     "color": "#E67E22"},
    "F4_Sovereign": {"label": "Sovereign Stress",       "cls": "DOMESTIC",   "color": "#C0392B"},
    "F5_Domestic":  {"label": "Domestic Concentration", "cls": "DOMESTIC",   "color": "#8E44AD"},
    "F6_Herding":   {"label": "Herding (CSAD)",         "cls": "BEHAVIORAL", "color": "#C9A227"},
}
FACTOR_COLS = list(FACTOR_META.keys())
HERD_WIN = 60
H_MOD = 0.02
H_STRONG = 0.05

SCORE_WEIGHTS = {"fwd_vol": 0.30, "pc1_conc": 0.20, "regime_stress": 0.20, "contagion": 0.15, "drawdown": 0.15}
RISK_LEVELS = [(0, 40, "LOW", "#1E6B2E"), (40, 60, "MODERATE", "#C9A227"), (60, 75, "HIGH", "#E67E22"), (75, 100, "CRITICAL", "#C0392B")]

# Historical crash scenarios — client simulates their portfolio against real events
# Shocks are in z-score units derived from actual factor movements during each event
STRESS_SCENARIOS = {
    "COVID-19 Crash (Mar 2020)":       {"type": "PANDEMIC",   "period": "2020-02-20/2020-03-23", "jse_dd": -35.4, "shocks": {"F1_Global": 4.5, "F3_Commodity": -4.0, "F4_Sovereign": 3.5, "F2_USD": 3.8}},
    "Nenegate (Dec 2015)":             {"type": "POLITICAL",  "period": "2015-12-09/2015-12-11", "jse_dd": -9.2,  "shocks": {"F4_Sovereign": 4.5, "F2_USD": 3.5, "F5_Domestic": -3.0}},
    "SA Junk Downgrade (Apr 2017)":    {"type": "SOVEREIGN",  "period": "2017-03-27/2017-04-03", "jse_dd": -4.1,  "shocks": {"F4_Sovereign": 3.8, "F2_USD": 2.5, "F5_Domestic": -2.0}},
    "Eskom Load-Shedding (2019)":      {"type": "DOMESTIC",   "period": "2019-02-01/2019-03-15", "jse_dd": -5.3,  "shocks": {"F5_Domestic": -3.5, "F4_Sovereign": 2.0, "F1_Global": 1.0}},
    "Global Financial Crisis (2008)":  {"type": "SYSTEMIC",   "period": "2008-05-01/2009-03-09", "jse_dd": -45.8, "shocks": {"F1_Global": 5.0, "F3_Commodity": -4.5, "F2_USD": 4.0, "F4_Sovereign": 3.5}},
    "Taper Tantrum (May 2013)":        {"type": "FINANCIAL",  "period": "2013-05-22/2013-06-24", "jse_dd": -11.7, "shocks": {"F2_USD": 3.5, "F4_Sovereign": 2.8, "F1_Global": 2.0}},
    "China Slowdown (Aug 2015)":       {"type": "DEMAND",     "period": "2015-08-10/2015-08-24", "jse_dd": -12.5, "shocks": {"F3_Commodity": -3.5, "F1_Global": 3.0, "F2_USD": 2.0}},
    "Commodity Crash (2014-2016)":      {"type": "COMMODITY",  "period": "2014-06-01/2016-01-20", "jse_dd": -22.3, "shocks": {"F3_Commodity": -4.0, "F4_Sovereign": 2.5, "F2_USD": 2.5}},
    "Zuma State Capture (2017-2018)":  {"type": "POLITICAL",  "period": "2017-10-01/2017-11-30", "jse_dd": -6.8,  "shocks": {"F4_Sovereign": 3.5, "F5_Domestic": -2.5, "F2_USD": 2.0}},
    "Russia-Ukraine War (Feb 2022)":   {"type": "GEOPOLITICAL","period": "2022-02-24/2022-03-08", "jse_dd": -8.5, "shocks": {"F3_Commodity": 3.5, "F1_Global": 3.0, "F2_USD": 2.0}},
}

# Portfolio upload column aliases
UPLOAD_ALIASES = {
    "symbol": ["symbol", "ticker", "asset", "asset_id", "stock", "instrument", "security", "name", "code"],
    "quantity": ["quantity", "shares", "qty", "units", "lots", "position", "amount"],
    "price": ["price", "cost", "avg_cost", "avgcost", "average_cost", "market_price", "last", "close"],
    "market": ["market", "exchange", "bourse", "listing", "exch"],
    "sector": ["sector", "industry", "category", "segment", "gics"],
    "value": ["value", "market_value", "notional", "mv", "mkt_val"],
    "weight": ["weight", "wt", "pct", "allocation", "%"],
    "currency": ["currency", "ccy", "cur"],
}

EXCHANGE_MAP = {
    # South Africa
    "JSE": "JSE_SA", "XJSE": "JSE_SA", "SJ": "JSE_SA", "JOHANNESBURG": "JSE_SA",
    "JSE_SA": "JSE_SA", "SA": "JSE_SA", "ZA": "JSE_SA",
    # Morocco
    "CSE": "MASI_MA", "XCAS": "MASI_MA", "MC": "MASI_MA", "CASABLANCA": "MASI_MA",
    "MASI": "MASI_MA", "MASI_MA": "MASI_MA", "MA": "MASI_MA",
    # Egypt
    "EGX": "EGX_EG", "XCAI": "EGX_EG", "EY": "EGX_EG", "CAIRO": "EGX_EG",
    "EGX_EG": "EGX_EG", "EG": "EGX_EG",
    # Nigeria
    "NGX": "NGX_NG", "XLAG": "NGX_NG", "NL": "NGX_NG", "LAGOS": "NGX_NG", "NIGERIA": "NGX_NG",
    "NGX_NG": "NGX_NG", "NG": "NGX_NG",
}
