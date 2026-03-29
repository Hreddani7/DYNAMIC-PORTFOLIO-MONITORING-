"""API Routes — CSV/Excel upload + 8-layer compute + AI chat + Bloomberg live."""
import os, uuid, time, random, math, io, threading, logging
from datetime import datetime

import numpy as np
import pandas as pd
from flask import Blueprint, request, jsonify

from app.config import (MARKETS, FACTOR_META, STRESS_SCENARIOS, UPLOAD_DIR,
                         UPLOAD_ALIASES, EXCHANGE_MAP, RISK_LEVELS)
from app.auth import authenticate_user, create_token, token_required
from app.db import (get_db, create_portfolio, save_holdings, get_holdings,
                    get_portfolios, create_sample_portfolio)
from app.ingestion import generate_prices, generate_macro, clear_cache, get_data_source, generate_stocks_per_market
from app.bloomberg import get_bloomberg
from app.bloomberg.blp import MARKET_TICKERS, MACRO_TICKERS
from app.layers import (compute_layer0, compute_layer2, compute_layer3,
                        compute_layer4, compute_layer5, compute_layer6, compute_layer7)
from app.layers.layer6_simulator import (
    run_monte_carlo, run_historical_replay, classify_and_stress,
    compute_baseline_metrics, compute_weights, compute_kde_data,
    CRISIS_PERIODS, STRESS_MULTIPLIERS, ALL_FACTORS, FACTOR_MAP,
)
from app.assistant import PortfolioAssistant

log = logging.getLogger(__name__)
api = Blueprint("api", __name__, url_prefix="/api/v1")
assistant = PortfolioAssistant()
computed_cache = {}
_compute_jobs = {}  # {pid: {"status": "running"|"done"|"error", "result": ..., "error": ...}}


def _sanitize(obj):
    """Replace NaN/Inf with None for valid JSON."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, (np.floating, np.integer)):
        v = float(obj)
        return None if (math.isnan(v) or math.isinf(v)) else v
    if isinstance(obj, np.ndarray):
        return _sanitize(obj.tolist())
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    return obj


def _match_column(df_columns, target):
    """Find a column name matching aliases."""
    aliases = UPLOAD_ALIASES.get(target, [])
    for col in df_columns:
        cl = col.lower().strip().replace(" ", "_")
        if cl in aliases or any(a in cl for a in aliases):
            return col
    return None


def _detect_header_row(df):
    """Find the real header row in an Excel file with title/banner rows.
    Looks for a row containing known column aliases (symbol, quantity, price, etc.)."""
    all_aliases = []
    for aliases in UPLOAD_ALIASES.values():
        all_aliases.extend(aliases)
    for i, row in df.iterrows():
        vals = [str(v).lower().strip().replace(" ", "_") for v in row.values if pd.notna(v)]
        matches = sum(1 for v in vals if any(a in v for a in all_aliases))
        if matches >= 2:  # At least 2 recognized column names
            return i
    return None


def _parse_portfolio_file(file_content, filename):
    """Parse CSV/Excel into normalized holdings."""
    try:
        if filename.lower().endswith(('.xlsx', '.xls')):
            # First try reading with data_only to evaluate formulas
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(file_content), data_only=True)
            ws = wb.active
            raw_data = [[cell.value for cell in row] for row in ws.iter_rows()]
            df = pd.DataFrame(raw_data[1:], columns=raw_data[0])
            # Detect if the first row is a title/banner (not column headers)
            header_row = _detect_header_row(df)
            if header_row is not None:
                # Re-read using the detected header row
                real_headers = [str(v).strip() if pd.notna(v) else f"col_{j}"
                                for j, v in enumerate(df.iloc[header_row])]
                df = df.iloc[header_row + 1:].reset_index(drop=True)
                df.columns = real_headers
            else:
                # Maybe the original first row IS the header — try re-reading
                # with default header detection from openpyxl
                pass
        else:
            for enc in ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']:
                try:
                    df = pd.read_csv(io.BytesIO(file_content), encoding=enc, thousands=',')
                    break
                except Exception:
                    continue
            else:
                return None, "Could not parse file encoding"
            # CSV may also have title rows
            header_row = _detect_header_row(df)
            if header_row is not None:
                real_headers = [str(v).strip() if pd.notna(v) else f"col_{j}"
                                for j, v in enumerate(df.iloc[header_row])]
                df = df.iloc[header_row + 1:].reset_index(drop=True)
                df.columns = real_headers
    except Exception as e:
        return None, str(e)

    if len(df) == 0:
        return None, "File is empty"

    # Map columns
    sym_col = _match_column(df.columns, "symbol")
    if not sym_col:
        return None, f"No symbol/ticker column found. Columns: {list(df.columns)}"

    qty_col = _match_column(df.columns, "quantity")
    price_col = _match_column(df.columns, "price")
    mkt_col = _match_column(df.columns, "market")
    sec_col = _match_column(df.columns, "sector")
    val_col = _match_column(df.columns, "value")
    wt_col = _match_column(df.columns, "weight")
    ccy_col = _match_column(df.columns, "currency")

    # Skip summary/total/header rows
    SKIP_SYMBOLS = {"total", "totals", "sum", "subtotal", "grand total",
                    "portfolio summary", "number of holdings", "total portfolio",
                    "avg position", "largest position", "smallest position",
                    "concentration", "source:", "nan", "none", ""}

    holdings = []
    for _, row in df.iterrows():
        symbol = str(row.get(sym_col, "")).strip()
        if not symbol or symbol.lower() in SKIP_SYMBOLS:
            continue
        # Skip rows where symbol starts with a known skip prefix
        sym_lower = symbol.lower()
        if any(sym_lower.startswith(s) for s in SKIP_SYMBOLS if s):
            continue
        # Skip rows where symbol looks like a label (too long or has spaces)
        if len(symbol) > 15 and " " in symbol:
            continue

        try:
            qty = float(row.get(qty_col, 0) or 0) if qty_col else 0
        except (ValueError, TypeError):
            qty = 0
        try:
            price = float(row.get(price_col, 0) or 0) if price_col else 0
        except (ValueError, TypeError):
            price = 0
        market_raw = str(row.get(mkt_col, "") or "").strip().upper() if mkt_col else ""
        sector = str(row.get(sec_col, "") or "").strip() if sec_col else "Unknown"
        currency = str(row.get(ccy_col, "") or "").strip() if ccy_col else ""

        # Map exchange to internal market ID — accept internal IDs directly (e.g. JSE_SA)
        market = ""
        if market_raw in MARKETS:
            market = market_raw  # Already a valid internal market ID
        else:
            market = EXCHANGE_MAP.get(market_raw, "")
        if not market:
            # Try to infer from symbol suffix
            parts = symbol.split(".")
            if len(parts) > 1:
                suffix = parts[-1].upper()
                market = EXCHANGE_MAP.get(suffix, "")
            if not market:
                # Try Bloomberg-style: "NPN SJ Equity" -> SJ -> JSE_SA
                for token in symbol.upper().split():
                    if token in EXCHANGE_MAP:
                        market = EXCHANGE_MAP[token]
                        break

        # Market value — handle formula strings, None, and numeric values
        mv = 0
        if val_col and pd.notna(row.get(val_col)):
            raw_mv = row.get(val_col, 0)
            if isinstance(raw_mv, str) and raw_mv.startswith("="):
                mv = qty * price  # Formula not evaluated, compute manually
            else:
                try:
                    mv = float(raw_mv or 0)
                except (ValueError, TypeError):
                    mv = qty * price
        else:
            mv = qty * price

        holdings.append({
            "asset_id": symbol,
            "market": market,
            "sector": sector,
            "quantity": qty,
            "price": price,
            "market_value": mv,
            "currency": currency,
            "weight": 0,  # Will be computed
        })

    if not holdings:
        return None, "No valid holdings found"

    # Compute weights from market value
    total = sum(h["market_value"] for h in holdings)
    if total > 0:
        for h in holdings:
            h["weight"] = round(h["market_value"] / total, 4)
    elif wt_col:
        for i, (_, row) in enumerate(df.iterrows()):
            if i < len(holdings):
                w = float(row.get(wt_col, 0) or 0)
                holdings[i]["weight"] = w / 100 if w > 1 else w  # Handle % vs decimal

    return holdings, None


# ═══ AUTH ═══
@api.route("/auth/login", methods=["POST", "OPTIONS"])
def login():
    if request.method == "OPTIONS": return "", 204
    data = request.get_json()
    user = authenticate_user(data.get("username", ""), data.get("password", ""))
    if not user: return jsonify({"error": "Invalid credentials"}), 401
    # Check if 2FA is enabled
    if user.get("totp_enabled"):
        totp_code = data.get("totp_code")
        if not totp_code:
            return jsonify({"requires_2fa": True, "username": user["username"]}), 200
        from app.db import verify_totp
        if not verify_totp(user.get("totp_secret", ""), totp_code):
            return jsonify({"error": "Invalid 2FA code"}), 401
    return jsonify({"access_token": create_token(user), "role": user["role"],
                    "username": user["username"], "user_id": user["user_id"]})

@api.route("/auth/register", methods=["POST", "OPTIONS"])
def register():
    if request.method == "OPTIONS": return "", 204
    data = request.get_json()
    username = (data.get("username") or "").strip()
    email = (data.get("email") or "").strip()
    password = data.get("password", "")
    if not username or not email or not password:
        return jsonify({"error": "Username, email, and password are required"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    try:
        from app.db import create_user
        uid = create_user(username, email, password)
        user = authenticate_user(username, password)
        return jsonify({"access_token": create_token(user), "role": user["role"],
                        "username": user["username"], "user_id": user["user_id"]})
    except Exception as e:
        if "UNIQUE" in str(e):
            return jsonify({"error": "Username or email already exists"}), 409
        return jsonify({"error": str(e)}), 500

@api.route("/auth/setup-2fa", methods=["POST"])
@token_required
def setup_2fa():
    """Generate a TOTP secret for the user."""
    from app.db import get_db, generate_totp_secret
    secret = generate_totp_secret()
    conn = get_db()
    conn.execute("UPDATE users SET totp_secret=? WHERE user_id=?",
                 (secret, request.user.get("user_id")))
    conn.commit()
    conn.close()
    return jsonify({"secret": secret,
                    "otpauth_uri": f"otpauth://totp/AfriSK:{request.user.get('sub')}?secret={secret}&issuer=AfriSK"})

@api.route("/auth/enable-2fa", methods=["POST"])
@token_required
def enable_2fa():
    """Verify a TOTP code and enable 2FA."""
    data = request.get_json()
    code = data.get("code", "")
    from app.db import get_db, verify_totp
    conn = get_db()
    user = conn.execute("SELECT totp_secret FROM users WHERE user_id=?",
                        (request.user.get("user_id"),)).fetchone()
    if not user or not user["totp_secret"]:
        conn.close()
        return jsonify({"error": "Run setup-2fa first"}), 400
    if not verify_totp(user["totp_secret"], code):
        conn.close()
        return jsonify({"error": "Invalid code"}), 400
    conn.execute("UPDATE users SET totp_enabled=1 WHERE user_id=?",
                 (request.user.get("user_id"),))
    conn.commit()
    conn.close()
    return jsonify({"enabled": True})

@api.route("/auth/me")
@token_required
def get_me(): return jsonify(request.user)


# ═══ MARKETS ═══
@api.route("/markets")
def get_markets():
    return jsonify({"markets": MARKETS, "factors": FACTOR_META,
                    "risk_levels": [{"lo": lo, "hi": hi, "level": lv, "color": cl} for lo, hi, lv, cl in RISK_LEVELS]})


# ═══ PORTFOLIOS ═══
@api.route("/portfolios")
@token_required
def list_portfolios():
    portfolios = get_portfolios(request.user.get("user_id"), request.user.get("role"))
    # Add holdings count to each portfolio
    for p in portfolios:
        h = get_holdings(p["portfolio_id"])
        p["n_holdings"] = len(h) if h else 0
    return jsonify({"portfolios": portfolios})

@api.route("/portfolios/create-sample", methods=["POST", "OPTIONS"])
@token_required
def api_create_sample():
    if request.method == "OPTIONS": return "", 204
    pid = create_sample_portfolio(request.user["user_id"])
    return jsonify({"portfolio_id": pid, "message": "Sample portfolio created", "holdings": len(get_holdings(pid))})

@api.route("/portfolios/<pid>/holdings")
@token_required
def api_holdings(pid):
    holdings = get_holdings(pid)
    for h in holdings:
        h["market_name"] = MARKETS.get(h["market"], {}).get("name", h["market"])
    return jsonify({"portfolio_id": pid, "holdings": holdings, "count": len(holdings)})


# ═══ CSV/EXCEL UPLOAD ═══
@api.route("/portfolios/upload", methods=["POST", "OPTIONS"])
@token_required
def upload_portfolio():
    """Upload CSV/Excel file → parse → create portfolio → return holdings."""
    if request.method == "OPTIONS": return "", 204

    if "file" not in request.files:
        return jsonify({"error": "No file provided. Send as multipart/form-data with key 'file'"}), 400

    file = request.files["file"]
    if not file.filename:
        return jsonify({"error": "Empty filename"}), 400

    allowed = file.filename.lower().endswith(('.csv', '.xlsx', '.xls', '.tsv'))
    if not allowed:
        return jsonify({"error": "Unsupported format. Use CSV, XLSX, or XLS"}), 400

    content = file.read()
    name = request.form.get("name", f"Upload {file.filename}")

    # Parse
    holdings, error = _parse_portfolio_file(content, file.filename)
    if error:
        return jsonify({"error": f"Parse error: {error}"}), 400

    # Save file
    safe_name = f"{uuid.uuid4().hex[:8]}_{file.filename}"
    filepath = os.path.join(UPLOAD_DIR, safe_name)
    with open(filepath, "wb") as f:
        f.write(content)

    # Create portfolio + save holdings
    pid = create_portfolio(request.user["user_id"], name, f"Uploaded from {file.filename}", "upload", safe_name)
    save_holdings(pid, holdings)

    # Summary
    markets_found = {}
    for h in holdings:
        m = h["market"] or "Unknown"
        markets_found[m] = markets_found.get(m, 0) + 1

    return jsonify({
        "portfolio_id": pid,
        "name": name,
        "holdings_count": len(holdings),
        "markets_found": markets_found,
        "total_value": round(sum(h["market_value"] for h in holdings), 2),
        "holdings": holdings,
    })


# Ticker-to-internal-name map for stock data lookup
TICKER_TO_INTERNAL = {
    # Mega-cap
    "NPN": "Naspers", "PRX": "Prosus", "CFR": "Richemont",
    "BTI": "BAT", "AGL": "AngloAmerican", "BHP": "BHP", "GLN": "Glencore",
    # Banks
    "SBK": "StandardBank", "FSR": "FirstRand", "ABG": "Absa",
    "NED": "Nedbank", "CPI": "Capitec", "INP": "Investec",
    # Insurance / Financial
    "SLM": "Sanlam", "DSY": "Discovery", "OMU": "OldMutual",
    "REM": "Remgro", "RNI": "Reinet",
    # Mining
    "AMS": "AngloAmericanPlat", "IMP": "ImpalaPlatinum",
    "GFI": "GoldFields", "SSW": "Sibanye", "ANG": "AngloGold",
    "KIO": "KumbaIron", "EXX": "Exxaro", "S32": "South32", "HAR": "Harmony",
    # Telecom / Tech
    "MTN": "MTN_SA", "VOD": "Vodacom",
    # Energy
    "SOL": "Sasol",
    # Retail / Consumer
    "SHP": "Shoprite", "MRP": "MrPrice", "WHL": "Woolworths",
    "TBS": "TigerBrands", "PIK": "PicknPay", "TFG": "Foschini",
    # Healthcare
    "APN": "Aspen",
    # Industrials
    "MNP": "Mondi", "BID": "BidCorp",
    # REITs
    "GRT": "Growthpoint", "VKE": "Vukile", "EMI": "Emira",
    # Consumer Staples
    "SPP": "Spar",
    # Media
    "MCG": "MultiChoice",
    # CSE (Morocco)
    "IAM": "Maroc_Telecom",
    # EGX (Egypt)
    "COMI": "CIB",
    # NGX (Nigeria)
    "GTCO": "GTH", "DANGCEM": "Dangote",
}


_pf_price_cache = {}  # {frozenset(tickers): (pf_prices, enriched_holdings, markets)}

def _build_portfolio_prices(holdings):
    """Build a multi-column price DataFrame from portfolio holdings' matched stock data.
    Also enriches holdings with _price_series and _internal_name.
    Returns (portfolio_prices_df, holdings, portfolio_markets).

    portfolio_prices_df has one column per holding ticker (e.g. NPN, SBK, GFI)
    so all layers compute risk on the ACTUAL portfolio stocks.
    """
    # Cache key: set of tickers in this portfolio
    tickers_key = frozenset(h.get("asset_id", "").split(".")[0].upper() for h in holdings)
    if tickers_key in _pf_price_cache:
        cached_prices, _, cached_markets = _pf_price_cache[tickers_key]
        # Re-enrich holdings with cached price series
        for h in holdings:
            tk = h.get("asset_id", "").split(".")[0].upper()
            if tk in cached_prices.columns:
                h["_price_series"] = cached_prices[tk]
                h["_internal_name"] = tk
            mkt = h.get("market", "")
            if mkt:
                cached_markets.add(mkt)
        return cached_prices.copy(), holdings, cached_markets
    from app.ingestion import generate_stocks_per_market
    stocks = generate_stocks_per_market()

    portfolio_markets = set()
    price_dict = {}

    for h in holdings:
        mkt = h.get("market", "")
        if mkt:
            portfolio_markets.add(mkt)
            if mkt not in MARKETS:
                short = mkt.split("_")[0] if "_" in mkt else mkt[:3]
                MARKETS[mkt] = {"name": f"{short} ({mkt})", "short": short,
                                "currency": h.get("currency", ""), "color": "#888"}

        ticker = h.get("asset_id", "").split(".")[0].upper()
        internal = TICKER_TO_INTERNAL.get(ticker, ticker)

        # Look up price data from ingestion stocks
        matched_series = None
        for mkt_key, sdict in stocks.items():
            if internal in sdict:
                matched_series = sdict[internal]
                break
            for sym, series in sdict.items():
                if sym.upper() == internal.upper() or sym.upper() == ticker:
                    matched_series = series
                    break
            if matched_series is not None:
                break

        h["_price_series"] = matched_series
        h["_internal_name"] = internal

        if matched_series is not None and len(matched_series) >= 5:
            price_dict[ticker] = matched_series

    # For unmatched holdings, generate synthetic correlated series from JSE index
    # so all holdings are represented in the analysis
    # Get a reference series (longest available matched stock or JSE index)
    ref = None
    for tk, s in sorted(price_dict.items(), key=lambda x: len(x[1]), reverse=True):
        if len(s) >= 100:
            ref = s
            break
    if ref is None:
        # Use JSE index from market prices as reference
        from app.ingestion import generate_prices
        mp = generate_prices()
        if mp is not None and "JSE_SA" in mp.columns:
            ref = mp["JSE_SA"]

    if ref is not None and len(ref) >= 100:
        np.random.seed(42)
        # Sector-based vol/beta for synthetic generation
        SECTOR_SYNTH = {
            "Mining": {"beta": 1.2, "vol": 0.022},
            "Mining & Resources": {"beta": 1.2, "vol": 0.022},
            "Banking": {"beta": 0.9, "vol": 0.016},
            "Financials": {"beta": 0.85, "vol": 0.015},
            "Technology": {"beta": 1.1, "vol": 0.020},
            "Telecom": {"beta": 0.8, "vol": 0.014},
            "Energy": {"beta": 1.0, "vol": 0.018},
            "Consumer Discretionary": {"beta": 0.75, "vol": 0.013},
            "Consumer Staples": {"beta": 0.6, "vol": 0.011},
            "Healthcare": {"beta": 0.7, "vol": 0.014},
            "Industrials": {"beta": 0.85, "vol": 0.015},
            "Real Estate": {"beta": 0.65, "vol": 0.016},
        }
        INTERNAL_SYNTH = {
            "Richemont": {"beta": 0.9, "vol": 0.018, "base": 2200},
            "GoldFields": {"beta": 1.3, "vol": 0.025, "base": 320},
            "Shoprite": {"beta": 0.7, "vol": 0.014, "base": 310},
            "BAT": {"beta": 0.6, "vol": 0.012, "base": 650},
        }
        ref_ret = np.log(ref / ref.shift(1)).dropna()
        for h in holdings:
            ticker = h.get("asset_id", "").split(".")[0].upper()
            if ticker not in price_dict:
                internal = TICKER_TO_INTERNAL.get(ticker, ticker)
                # Try specific internal name params, else use sector-based params
                if internal in INTERNAL_SYNTH:
                    params = INTERNAL_SYNTH[internal]
                else:
                    sector = h.get("sector", "")
                    sp = SECTOR_SYNTH.get(sector, {"beta": 0.8, "vol": 0.015})
                    price_val = h.get("price", 0) or 500
                    params = {**sp, "base": max(price_val, 10)}
                # Correlated synthetic: beta * ref_return + idio noise
                synth_ret = params["beta"] * ref_ret + params["vol"] * np.random.randn(len(ref_ret))
                synth_price = params["base"] * np.exp(synth_ret.cumsum())
                synth_price.name = ticker
                price_dict[ticker] = synth_price
                h["_price_series"] = synth_price
                h["_synthetic"] = True

    # Build portfolio-level price DataFrame
    if price_dict:
        pf_prices = pd.DataFrame(price_dict).sort_index().dropna(how="all").ffill().bfill()
    else:
        pf_prices = None

    # Cache for reuse
    if pf_prices is not None:
        _pf_price_cache[tickers_key] = (pf_prices.copy(), None, set(portfolio_markets))

    return pf_prices, holdings, portfolio_markets


# ═══ COMPUTE ALL 8 LAYERS ═══
@api.route("/portfolios/<pid>/compute-all")
@token_required
def compute_all(pid):
    holdings = get_holdings(pid)
    if not holdings:
        return jsonify({"error": "Portfolio not found or empty"}), 404

    # If already computed, return cached result immediately (unless refresh requested)
    refresh = request.args.get("refresh", "0") == "1"
    if refresh:
        computed_cache.pop(pid, None)
        _compute_jobs.pop(pid, None)
        _pf_price_cache.clear()  # Clear price cache for re-fetch
    elif pid in computed_cache:
        return jsonify(_sanitize(computed_cache[pid]))

    # Check if async mode requested (default: yes for speed)
    mode = request.args.get("mode", "async")

    if mode == "sync":
        return _compute_all_json(pid, holdings)

    # ── Async mode: kick off background thread, return immediately ──
    if pid in _compute_jobs and _compute_jobs[pid]["status"] == "running":
        # Already computing, return status
        return jsonify({"status": "computing", "portfolio_id": pid})

    # Build quick portfolio info for immediate display
    pf_prices, enriched_holdings, portfolio_markets = _build_portfolio_prices(holdings)
    clean_holdings = []
    for h in enriched_holdings:
        ch = {k: v for k, v in h.items() if not k.startswith("_")}
        ch["data_matched"] = h.get("_price_series") is not None
        clean_holdings.append(ch)

    pf_info = {
        "total_value": sum(h.get("market_value", 0) for h in holdings),
        "n_holdings": len(holdings),
        "markets": list(portfolio_markets),
        "holdings_matched": sum(1 for h in clean_holdings if h.get("data_matched")),
        "holdings_total": len(holdings),
        "stocks_in_prices": list(pf_prices.columns) if pf_prices is not None and hasattr(pf_prices, 'columns') else [],
    }

    # Start background computation
    _compute_jobs[pid] = {"status": "running", "result": None, "error": None}

    def _bg_compute():
        try:
            market_prices = generate_prices()
            # Re-use already-built prices (cached)
            prices = pf_prices if pf_prices is not None and len(pf_prices.columns) >= 1 else market_prices
            l0 = compute_layer0(prices, enriched_holdings); l0i = l0.pop("_internal", {})
            l2 = compute_layer2(prices, enriched_holdings); l2i = l2.pop("_internal", {})
            l3 = compute_layer3(market_prices, enriched_holdings, l2_internal=l2i); l3i = l3.pop("_internal", {})
            l4 = compute_layer4(prices, enriched_holdings, l0_internal=l0i, l2_internal=l2i, l3_internal=l3i); l4i = l4.pop("_internal", {})
            l5 = compute_layer5(prices, enriched_holdings, l0_internal=l0i, l2_internal=l2i, l3_internal=l3i, l4_internal=l4i)
            l6 = compute_layer6(prices, enriched_holdings, l2_internal=l2i, l3_internal=l3i)
            # L7 AI — short timeout
            l7_result = [None]
            def _run_l7():
                try:
                    l7_result[0] = compute_layer7(l0, l2, l3, l4, l5, l6)
                except Exception:
                    l7_result[0] = {"headline": "AI report generation skipped.", "alerts": [], "ai_source": "skipped"}
            t7 = threading.Thread(target=_run_l7, daemon=True)
            t7.start()
            t7.join(timeout=12)
            l7 = l7_result[0] or {"headline": "AI report generating in background...", "alerts": [], "ai_source": "pending"}
            result = _build_result(pid, enriched_holdings, portfolio_markets, prices, l0, l2, l3, l4, l5, l6, l7)
            computed_cache[pid] = result
            _compute_jobs[pid] = {"status": "done", "result": result, "error": None}
            log.info(f"[COMPUTE] Background compute done for {pid}")
        except Exception as e:
            log.error(f"[COMPUTE] Background compute failed for {pid}: {e}")
            _compute_jobs[pid] = {"status": "error", "result": None, "error": str(e)}

    threading.Thread(target=_bg_compute, daemon=True).start()

    # Return immediate response with portfolio info + holdings
    return jsonify({
        "status": "computing",
        "portfolio_id": pid,
        "computed_at": datetime.now().isoformat(),
        "engine": "InteliRisk v4.0",
        "portfolio_info": pf_info,
        "portfolio_holdings": clean_holdings,
    })


@api.route("/portfolios/<pid>/compute-status")
@token_required
def compute_status(pid):
    """Poll for background compute result."""
    # If already in computed_cache, return full result
    if pid in computed_cache:
        return jsonify({"status": "done", "result": _sanitize(computed_cache[pid])})

    job = _compute_jobs.get(pid)
    if not job:
        return jsonify({"status": "not_started"}), 404

    if job["status"] == "running":
        return jsonify({"status": "computing"})

    if job["status"] == "error":
        return jsonify({"status": "error", "error": job["error"]}), 500

    # Done — return full result
    return jsonify({"status": "done", "result": _sanitize(job["result"])})


def _compute_all_json(pid, holdings):
    """Standard blocking compute-all — returns full JSON at once."""
    market_prices = generate_prices()
    pf_prices, holdings, portfolio_markets = _build_portfolio_prices(holdings)
    prices = pf_prices if pf_prices is not None and len(pf_prices.columns) >= 1 else market_prices

    l0 = compute_layer0(prices, holdings); l0i = l0.pop("_internal", {})
    l2 = compute_layer2(prices, holdings); l2i = l2.pop("_internal", {})
    l3 = compute_layer3(market_prices, holdings, l2_internal=l2i); l3i = l3.pop("_internal", {})
    l4 = compute_layer4(prices, holdings, l0_internal=l0i, l2_internal=l2i, l3_internal=l3i); l4i = l4.pop("_internal", {})
    l5 = compute_layer5(prices, holdings, l0_internal=l0i, l2_internal=l2i, l3_internal=l3i, l4_internal=l4i)
    l6 = compute_layer6(prices, holdings, l2_internal=l2i, l3_internal=l3i)

    # L7 AI — run with short timeout to avoid blocking
    l7_result = [None]
    def _run_l7():
        try:
            l7_result[0] = compute_layer7(l0, l2, l3, l4, l5, l6)
        except Exception:
            l7_result[0] = {"headline": "AI report generation skipped.", "alerts": [], "ai_source": "skipped"}
    t7 = threading.Thread(target=_run_l7, daemon=True)
    t7.start()
    t7.join(timeout=12)  # Wait max 12s for AI
    l7 = l7_result[0] or {"headline": "AI report generating in background...", "alerts": [], "ai_source": "pending"}

    result = _build_result(pid, holdings, portfolio_markets, prices, l0, l2, l3, l4, l5, l6, l7)
    computed_cache[pid] = result
    return jsonify(_sanitize(result))


def _compute_all_sse(pid, holdings):
    """SSE streaming — sends layer-done events as each layer completes."""
    from flask import Response
    import json as _json

    def generate():
        market_prices = generate_prices()
        pf_prices, h2, portfolio_markets = _build_portfolio_prices(holdings)
        prices = pf_prices if pf_prices is not None and len(pf_prices.columns) >= 1 else market_prices

        def emit(layer, status="done"):
            return f"data: {_json.dumps({'layer': layer, 'status': status})}\n\n"

        # L0
        l0 = compute_layer0(prices, h2); l0i = l0.pop("_internal", {})
        yield emit("L0")

        # L2
        l2 = compute_layer2(prices, h2); l2i = l2.pop("_internal", {})
        yield emit("L2")

        # L3
        l3 = compute_layer3(market_prices, h2, l2_internal=l2i); l3i = l3.pop("_internal", {})
        yield emit("L3")

        # L4
        l4 = compute_layer4(prices, h2, l0_internal=l0i, l2_internal=l2i, l3_internal=l3i); l4i = l4.pop("_internal", {})
        yield emit("L4")

        # L5
        l5 = compute_layer5(prices, h2, l0_internal=l0i, l2_internal=l2i, l3_internal=l3i, l4_internal=l4i)
        yield emit("L5")

        # L6
        l6 = compute_layer6(prices, h2, l2_internal=l2i, l3_internal=l3i)
        yield emit("L6")

        # L7 — with short timeout
        import threading
        l7_result = [None]
        def _run_l7():
            try:
                l7_result[0] = compute_layer7(l0, l2, l3, l4, l5, l6)
            except Exception:
                l7_result[0] = {"headline": "AI report skipped.", "alerts": [], "ai_source": "skipped"}
        t7 = threading.Thread(target=_run_l7, daemon=True)
        t7.start()
        t7.join(timeout=12)
        l7 = l7_result[0] or {"headline": "AI report generating...", "alerts": [], "ai_source": "pending"}
        yield emit("L7")

        # Build final result
        result = _build_result(pid, h2, portfolio_markets, prices, l0, l2, l3, l4, l5, l6, l7)
        computed_cache[pid] = result
        yield f"data: {_json.dumps({'layer': 'DONE', 'status': 'done', 'result': _sanitize(result)})}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _build_result(pid, holdings, portfolio_markets, prices, l0, l2, l3, l4, l5, l6, l7):
    """Build the final compute-all result dict."""
    pf_info = {"total_value": sum(h.get("market_value", 0) for h in holdings),
               "n_holdings": len(holdings),
               "markets": list(portfolio_markets),
               "holdings_matched": sum(1 for h in holdings if h.get("_price_series") is not None),
               "holdings_total": len(holdings),
               "stocks_in_prices": list(prices.columns) if hasattr(prices, 'columns') else []}

    clean_holdings = []
    for h in holdings:
        ch = {k: v for k, v in h.items() if not k.startswith("_")}
        ch["data_matched"] = h.get("_price_series") is not None
        clean_holdings.append(ch)

    return {
        "portfolio_id": pid, "computed_at": datetime.now().isoformat(), "engine": "InteliRisk v4.0",
        "portfolio_info": pf_info,
        "portfolio_holdings": clean_holdings,
        "risk_core": l0, "structural": l2, "factors": l3, "regime": l4,
        "score": l5, "stress": l6, "intelligence": l7,
    }


# ═══ INDIVIDUAL LAYERS ═══
@api.route("/portfolios/<pid>/risk-core")
@token_required
def api_risk_core(pid):
    if pid in computed_cache: return jsonify(_sanitize(computed_cache[pid]["risk_core"]))
    return jsonify({"error": "Run compute-all first"}), 400

@api.route("/portfolios/<pid>/regime")
@token_required
def api_regime(pid):
    if pid in computed_cache: return jsonify(_sanitize(computed_cache[pid]["regime"]))
    return jsonify({"error": "Run compute-all first"}), 400

@api.route("/portfolios/<pid>/intelligence")
@token_required
def api_intel(pid):
    if pid in computed_cache: return jsonify(_sanitize(computed_cache[pid]["intelligence"]))
    return jsonify({"error": "Run compute-all first"}), 400


# ═══ AI CHAT ═══
@api.route("/chat", methods=["POST", "OPTIONS"])
@token_required
def chat():
    if request.method == "OPTIONS": return "", 204
    data = request.get_json()
    ctx = computed_cache.get(data.get("portfolio_id"), {}) if data.get("portfolio_id") else {}
    return jsonify(assistant.respond(data.get("message", ""), ctx))


# ═══ BLOOMBERG / LIVE DATA ═══

@api.route("/bloomberg/status")
@token_required
def bloomberg_status():
    """Bloomberg Terminal + TWS connection status."""
    blp = get_bloomberg(auto_connect=True)
    st  = blp.status()
    # Report actual data source (bloomberg, tws, csv, synthetic)
    try:
        generate_prices()
        st["data_source"] = get_data_source()
    except Exception:
        st["data_source"] = "unknown"
    # Add TWS connection status
    try:
        from app.ibkr import get_ibkr
        ibkr = get_ibkr(auto_connect=False)
        st["tws_connected"] = ibkr.is_connected()
    except Exception:
        st["tws_connected"] = False
    st["last_checked"] = datetime.now().isoformat()
    return jsonify(st)


@api.route("/bloomberg/live")
@token_required
def bloomberg_live():
    """Live quotes for all 4 market indices + key macro instruments."""
    blp = get_bloomberg(auto_connect=True)

    if blp.is_connected():
        quotes = blp.get_live_quotes()
        source = "bloomberg"
    else:
        quotes = {}
        source = "unavailable"

    # Annotate each quote with the market display name
    for label, q in quotes.items():
        if label in MARKETS:
            q["display_name"] = MARKETS[label]["name"]
            q["short"]        = MARKETS[label]["short"]
            q["currency"]     = MARKETS[label].get("currency", "")
        elif label in MACRO_TICKERS:
            q["display_name"] = label.upper()

    return jsonify({
        "source":    source,
        "connected": blp.is_connected(),
        "quotes":    _sanitize(quotes),
        "timestamp": datetime.now().isoformat(),
    })


@api.route("/bloomberg/macro")
@token_required
def bloomberg_macro():
    """Return current macro data snapshot (latest values)."""
    m = generate_macro()
    if m is None or len(m) == 0:
        return jsonify({"macro": {}, "source": get_data_source()})
    latest = {}
    for col in m.columns:
        val = m[col].dropna()
        if len(val) > 0:
            latest[col] = {"value": round(float(val.iloc[-1]), 4),
                           "date": str(val.index[-1].date()),
                           "count": len(val)}
    return jsonify({"macro": latest, "source": get_data_source(),
                    "total_rows": len(m), "columns": list(m.columns)})


@api.route("/bloomberg/refresh", methods=["POST", "OPTIONS"])
@api.route("/data/refresh", methods=["POST", "OPTIONS"])
@token_required
def bloomberg_refresh():
    """Force-clear the data cache and re-fetch from Bloomberg/TWS/CSV."""
    if request.method == "OPTIONS":
        return "", 204
    clear_cache()
    try:
        p = generate_prices()
        m = generate_macro()
        source = get_data_source()
        stocks = generate_stocks_per_market()
        stock_count = sum(len(sd) for sd in stocks.values()) if stocks else 0
        return jsonify({
            "status":      "refreshed",
            "source":      source,
            "prices_rows": len(p),
            "macro_cols":  list(m.columns),
            "markets":     list(p.columns),
            "stocks_loaded": stock_count,
            "timestamp":   datetime.now().isoformat(),
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ═══ BANK/BLOOMBERG SIM (kept for sample trades) ═══

@api.route("/bank/status")
@token_required
def bank_status():
    """Legacy bank status — proxies to real Bloomberg status."""
    blp = get_bloomberg(auto_connect=False)
    st  = blp.status()
    return jsonify({
        "connected": True,
        "bank": "Pan-African Investment Bank",
        "last_sync": datetime.now().isoformat(),
        "bloomberg": {
            "status":       "connected" if st["connected"] else "disconnected",
            "blpapi":       st["blpapi_available"],
            "subscription": "live" if st["subscription_alive"] else "off",
            "live_tickers": st["live_tickers"],
        },
    })


@api.route("/bank/trades")
@token_required
def bank_trades():
    """Return actual portfolio holdings (not fake trades)."""
    # Get current portfolio holdings
    portfolios = get_portfolios(request.user.get("user_id"), request.user.get("role"))
    holdings = []
    if portfolios:
        pid = portfolios[0]["portfolio_id"]
        holdings = get_holdings(pid)
    return jsonify({"holdings": holdings, "count": len(holdings)})


# ═══ IBKR TWS ═══

@api.route("/ibkr/status")
@token_required
def ibkr_status():
    """IBKR TWS connection status."""
    from app.ibkr import get_ibkr
    ibkr = get_ibkr(auto_connect=True)
    return jsonify(_sanitize(ibkr.status()))


@api.route("/ibkr/connect", methods=["POST", "OPTIONS"])
@token_required
def ibkr_connect():
    """Connect to IBKR TWS."""
    if request.method == "OPTIONS":
        return "", 204
    from app.ibkr import get_ibkr
    data = request.get_json() or {}
    host = data.get("host", "127.0.0.1")
    port = int(data.get("port", 7497))
    ibkr = get_ibkr(auto_connect=False, host=host, port=port)
    ok = ibkr.connect()
    return jsonify({"connected": ok, "host": host, "port": port})


@api.route("/ibkr/portfolio")
@token_required
def ibkr_portfolio():
    """Fetch live portfolio positions from TWS."""
    from app.ibkr import get_ibkr
    ibkr = get_ibkr(auto_connect=True)
    holdings = ibkr.fetch_portfolio()
    if not holdings:
        return jsonify({"error": "No positions found or TWS not connected", "connected": ibkr.is_connected()}), 404
    return jsonify({
        "holdings": holdings,
        "count": len(holdings),
        "total_value": round(sum(h["market_value"] for h in holdings), 2),
    })


@api.route("/ibkr/import-portfolio", methods=["POST", "OPTIONS"])
@token_required
def ibkr_import():
    """Import IBKR portfolio as a new portfolio in the system."""
    if request.method == "OPTIONS":
        return "", 204
    from app.ibkr import get_ibkr
    ibkr = get_ibkr(auto_connect=True)
    holdings = ibkr.fetch_portfolio()
    if not holdings:
        return jsonify({"error": "No positions from TWS"}), 404

    name = request.get_json().get("name", "IBKR Portfolio") if request.is_json else "IBKR Portfolio"
    pid = create_portfolio(request.user["user_id"], name, "Imported from IBKR TWS", "ibkr", "")
    save_holdings(pid, holdings)
    return jsonify({
        "portfolio_id": pid,
        "name": name,
        "holdings_count": len(holdings),
        "total_value": round(sum(h["market_value"] for h in holdings), 2),
    })


@api.route("/ibkr/historical/<key>")
@token_required
def ibkr_historical(key):
    """Fetch historical data for a specific instrument from TWS."""
    from app.ibkr import get_ibkr
    ibkr = get_ibkr(auto_connect=True)
    s = ibkr.fetch_historical(key)
    if s is None:
        return jsonify({"error": f"Could not fetch {key}"}), 404
    return jsonify({
        "key": key,
        "count": len(s),
        "start": str(s.index[0]),
        "end": str(s.index[-1]),
        "last": round(float(s.iloc[-1]), 4),
    })


# ═══ UPLOAD FORMAT EXAMPLE ═══

@api.route("/upload/example-format")
def upload_example():
    """Return example CSV formats for portfolio upload."""
    return jsonify({
        "portfolio_csv": {
            "description": "Portfolio positions — your holdings",
            "columns": ["ticker", "shares", "avg_cost", "currency", "exchange"],
            "example_rows": [
                ["NPN", 100, 3200, "ZAR", "JSE"],
                ["SBK", 500, 180, "ZAR", "JSE"],
                ["AGL", 200, 850, "ZAR", "JSE"],
                ["BHP", 50, 4500, "ZAR", "JSE"],
                ["USDZAR", 10000, 18.5, "USD", "FX"],
            ],
            "notes": "Columns: ticker/symbol, shares/quantity, avg_cost/price, currency (ZAR/USD), exchange (JSE/NGX/EGX/MASI/FX)",
        },
        "price_csv": {
            "description": "Historical price data — OHLCV or close only",
            "columns": ["Date", "NPN", "SBK", "AGL"],
            "example_rows": [
                ["2024-01-02", 3150, 175, 820],
                ["2024-01-03", 3180, 177, 835],
            ],
            "notes": "Date in YYYY-MM-DD, one column per ticker (adjusted close prices)",
        },
    })


# ═══ DATA HEALTH ═══
@api.route("/data/health")
@token_required
def data_health():
    p = generate_prices(); m = generate_macro()
    return jsonify({"status": "healthy", "prices": {"rows": len(p), "markets": list(p.columns)},
                    "macro": {"rows": len(m), "columns": list(m.columns)}})


# ═══════════════════════════════════════════════════════════════════
# PORTFOLIO ANALYTICS (Portfolio Visualizer–style)
# ═══════════════════════════════════════════════════════════════════

@api.route("/portfolios/<pid>/analytics")
@token_required
def portfolio_analytics(pid):
    """
    Portfolio Visualizer–style analytics: monthly returns, annual returns,
    portfolio growth, correlation matrix, rolling returns, contribution analysis.
    Uses individual stock price data from the ingestion pipeline.
    """
    holdings = get_holdings(pid)
    if not holdings:
        return jsonify({"error": "Portfolio not found or empty"}), 404

    # Use the same _build_portfolio_prices as compute_all for consistency
    pf_prices, holdings, _ = _build_portfolio_prices(holdings)
    if pf_prices is None or len(pf_prices) < 5:
        return jsonify({"error": "No stock price data available — ensure portfolio has valid tickers"}), 400

    price_df = pf_prices

    # Build matched_holdings map for weight computation
    matched_holdings = {}
    for h in holdings:
        ticker = h.get("asset_id", "").split(".")[0].upper()
        if ticker in price_df.columns:
            matched_holdings[ticker] = h

    # Compute weights from holdings market values
    total_val = sum(abs(matched_holdings.get(s, {}).get("market_value", 0)) for s in price_df.columns)
    weights = {}
    for s in price_df.columns:
        if total_val > 0:
            weights[s] = abs(matched_holdings.get(s, {}).get("market_value", 0)) / total_val
        else:
            weights[s] = 1.0 / len(price_df.columns)
    w_arr = np.array([weights.get(s, 0) for s in price_df.columns])
    if w_arr.sum() == 0:
        w_arr = np.ones(len(price_df.columns)) / len(price_df.columns)
    else:
        w_arr = w_arr / w_arr.sum()

    # Daily returns
    returns_df = price_df.pct_change().dropna()
    if len(returns_df) < 10:
        return jsonify({"error": "Insufficient return data"}), 400

    # Portfolio daily returns (weighted)
    port_returns = (returns_df * w_arr).sum(axis=1)
    port_returns.name = "Portfolio"

    # ── Monthly Returns ──────────────────────────────────────
    monthly = {}
    for sym in returns_df.columns:
        mr = (1 + returns_df[sym]).resample("ME").prod() - 1
        monthly[sym] = {d.strftime("%Y-%m"): round(float(v) * 100, 2) for d, v in mr.items()}
    port_mr = (1 + port_returns).resample("ME").prod() - 1
    monthly["Portfolio"] = {d.strftime("%Y-%m"): round(float(v) * 100, 2) for d, v in port_mr.items()}

    # Monthly returns table: rows = months, columns = stocks + portfolio
    all_months = sorted(set().union(*[set(v.keys()) for v in monthly.values()]))
    # Limit to last 36 months for display
    recent_months = all_months[-36:] if len(all_months) > 36 else all_months
    monthly_table = []
    for m in recent_months:
        row = {"month": m}
        for sym in list(returns_df.columns) + ["Portfolio"]:
            row[sym] = monthly.get(sym, {}).get(m)
        monthly_table.append(row)

    # ── Annual Returns ───────────────────────────────────────
    annual = {}
    for sym in returns_df.columns:
        ar = (1 + returns_df[sym]).resample("YE").prod() - 1
        annual[sym] = {str(d.year): round(float(v) * 100, 2) for d, v in ar.items()}
    port_ar = (1 + port_returns).resample("YE").prod() - 1
    annual["Portfolio"] = {str(d.year): round(float(v) * 100, 2) for d, v in port_ar.items()}

    all_years = sorted(set().union(*[set(v.keys()) for v in annual.values()]))
    annual_table = []
    for y in all_years:
        row = {"year": y}
        for sym in list(returns_df.columns) + ["Portfolio"]:
            row[sym] = annual.get(sym, {}).get(y)
        annual_table.append(row)

    # ── Portfolio Growth (cumulative) ────────────────────────
    cum_port = (1 + port_returns).cumprod()
    cum_stocks = {}
    for sym in returns_df.columns:
        cum_s = (1 + returns_df[sym]).cumprod()
        cum_stocks[sym] = cum_s

    growth_dates = [d.strftime("%Y-%m-%d") for d in cum_port.index]
    # Downsample if too many points (weekly)
    step = max(1, len(growth_dates) // 500)
    growth = {
        "dates": growth_dates[::step],
        "portfolio": [round(float(v), 4) for v in cum_port.values[::step]],
    }
    for sym in returns_df.columns:
        growth[sym] = [round(float(v), 4) for v in cum_stocks[sym].values[::step]]

    # ── Correlation Matrix ───────────────────────────────────
    corr = returns_df.corr()
    corr_matrix = []
    for r in corr.index:
        for c in corr.columns:
            corr_matrix.append({"x": r, "y": c, "value": round(float(corr.loc[r, c]), 3)})

    # ── Rolling Returns (21d, 63d, 252d) ─────────────────────
    rolling = {}
    for window, label in [(21, "1M"), (63, "3M"), (252, "1Y")]:
        if len(port_returns) >= window:
            roll = port_returns.rolling(window).apply(lambda x: (1 + x).prod() - 1, raw=False)
            roll = roll.dropna()
            step_r = max(1, len(roll) // 300)
            rolling[label] = {
                "dates": [d.strftime("%Y-%m-%d") for d in roll.index[::step_r]],
                "values": [round(float(v) * 100, 2) for v in roll.values[::step_r]],
            }

    # ── Return Contribution (per stock) ──────────────────────
    # Total return contribution = weight * stock_return over full period
    total_port_ret = float(cum_port.iloc[-1] - 1)
    contribution = {}
    for i, sym in enumerate(returns_df.columns):
        stock_total = float(cum_stocks[sym].iloc[-1] - 1)
        contrib = w_arr[i] * stock_total
        contribution[sym] = {
            "weight": round(float(w_arr[i]) * 100, 2),
            "stock_return": round(stock_total * 100, 2),
            "contribution": round(contrib * 100, 2),
        }
    contribution["_total"] = round(total_port_ret * 100, 2)

    # ── Asset Allocation ─────────────────────────────────────
    allocation = []
    for i, sym in enumerate(price_df.columns):
        h_info = matched_holdings.get(sym, {})
        allocation.append({
            "symbol": sym,
            "weight": round(float(w_arr[i]) * 100, 2),
            "market_value": round(float(h_info.get("market_value", 0)), 0),
            "sector": h_info.get("sector", ""),
        })

    # ── Summary Stats ────────────────────────────────────────
    ann_ret = float(port_returns.mean()) * 252
    ann_vol = float(port_returns.std()) * np.sqrt(252)
    sharpe = ann_ret / ann_vol if ann_vol > 0 else 0
    cum_max = cum_port.cummax()
    dd = (cum_port - cum_max) / cum_max
    max_dd = float(dd.min())

    # Use risk_core worst_dd if available for consistency with Overview page
    rc_dd = None
    try:
        cached = computed_cache.get(pid, {})
        rc_dd = cached.get("risk_core", {}).get("portfolio", {}).get("worst_dd")
    except Exception:
        pass

    summary = {
        "total_return": round(total_port_ret * 100, 2),
        "ann_return": round(ann_ret * 100, 2),
        "ann_volatility": round(ann_vol * 100, 2),
        "sharpe": round(sharpe, 3),
        "max_drawdown": round(rc_dd, 2) if rc_dd is not None else round(max_dd * 100, 2),
        "n_stocks": len(price_df.columns),
        "n_days": len(returns_df),
        "start_date": str(returns_df.index[0].date()),
        "end_date": str(returns_df.index[-1].date()),
    }

    return jsonify(_sanitize({
        "portfolio_id": pid,
        "symbols": list(price_df.columns),
        "summary": summary,
        "monthly_table": monthly_table,
        "annual_table": annual_table,
        "growth": growth,
        "correlation": corr_matrix,
        "rolling_returns": rolling,
        "contribution": contribution,
        "allocation": allocation,
    }))


# ═══════════════════════════════════════════════════════════════════
# INTERACTIVE STRESS SIMULATOR
# ═══════════════════════════════════════════════════════════════════

# In-memory scenario storage per portfolio
_sim_scenarios = {}  # {pid: {scenario_name: result}}


@api.route("/simulator/config")
@token_required
def simulator_config():
    """Return available simulation options for the UI."""
    return jsonify({
        "regimes": ["Low", "Medium", "High"],
        "stress_levels": list(STRESS_MULTIPLIERS.keys()),
        "crisis_events": [
            {"name": k, "period": f"{v[0]} / {v[1]}"}
            for k, v in CRISIS_PERIODS.items()
        ],
        "factors": ALL_FACTORS,
        "factor_groups": {k: v for k, v in FACTOR_MAP.items()},
        "weight_methods": ["eq", "opt", "custom"],
    })


@api.route("/simulator/<pid>/baseline", methods=["POST", "OPTIONS"])
@token_required
def simulator_baseline(pid):
    """Compute baseline metrics for portfolio holdings."""
    if request.method == "OPTIONS":
        return "", 204

    data = request.get_json() or {}
    weight_method = data.get("weight_method", "eq")
    custom_weights = data.get("custom_weights")

    holdings = get_holdings(pid)
    if not holdings:
        return jsonify({"error": "Portfolio not found or empty"}), 404

    # Build stock-level prices (NPN, SBK, AGL, etc.) — same as compute_all
    pf_prices, holdings, _ = _build_portfolio_prices(holdings)
    if pf_prices is not None and len(pf_prices.columns) >= 1:
        prices_df = pf_prices
    else:
        prices_df = generate_prices()

    markets = list(prices_df.columns)

    # Compute weights
    weights = compute_weights(prices_df, method=weight_method,
                              custom_weights=custom_weights)

    baseline = compute_baseline_metrics(prices_df, weights)

    # Store for later use
    _sim_scenarios.setdefault(pid, {})
    _sim_scenarios[pid]["_baseline"] = baseline
    _sim_scenarios[pid]["_weights"] = weights
    _sim_scenarios[pid]["_markets"] = markets
    _sim_scenarios[pid]["_prices_df"] = prices_df  # Cache for MC/HR endpoints

    portfolio_value = sum(abs(h.get("market_value", 0)) for h in holdings) or 100000

    return jsonify(_sanitize({
        "portfolio_id": pid,
        "markets": markets,
        "weights": weights,
        "portfolio_value": portfolio_value,
        "baseline": baseline,
    }))


@api.route("/simulator/<pid>/monte-carlo", methods=["POST", "OPTIONS"])
@token_required
def simulator_monte_carlo(pid):
    """Run Monte Carlo simulation with HMM regime modeling."""
    if request.method == "OPTIONS":
        return "", 204

    data = request.get_json() or {}
    scenario_name = data.get("scenario_name", "Scenario 1")
    n_sims = min(int(data.get("n_sims", 500)), 2000)
    n_days = min(int(data.get("n_days", 250)), 1000)
    regime = data.get("regime", "Medium")
    stress_level = data.get("stress_level", "1.0x")

    stored = _sim_scenarios.get(pid, {})
    weights = stored.get("_weights")

    # Use cached stock-level prices from baseline, or build fresh
    prices_df = stored.get("_prices_df")
    if prices_df is None:
        holdings = get_holdings(pid)
        if holdings:
            pf_prices, holdings, _ = _build_portfolio_prices(holdings)
            prices_df = pf_prices if pf_prices is not None and len(pf_prices.columns) >= 1 else generate_prices()
        else:
            prices_df = generate_prices()

    markets = list(prices_df.columns)

    if weights is None:
        weights = compute_weights(prices_df)
        _sim_scenarios.setdefault(pid, {})
        _sim_scenarios[pid]["_weights"] = weights
        _sim_scenarios[pid]["_markets"] = markets

    # Factor stress (optional)
    factor_means = None
    factor_info = None
    if data.get("factors") and data.get("shocks"):
        fc = classify_and_stress(prices_df, data["factors"], data["shocks"])
        if "stressed_means" in fc:
            factor_means = list(fc["stressed_means"].values())
            factor_info = fc
        elif "error" in fc:
            factor_info = fc

    result = run_monte_carlo(
        prices_df, weights, n_sims=n_sims, n_days=n_days,
        regime=regime, stress_level=stress_level,
        factor_stress_means=factor_means,
    )

    result["scenario_name"] = scenario_name
    if factor_info:
        result["factor_analysis"] = factor_info

    # Add baseline comparison
    baseline = stored.get("_baseline")
    if baseline:
        rep = result["paths"]["representative"]["metrics"]
        result["baseline_delta"] = {
            k: round(rep.get(k, 0) - baseline.get(k, 0), 4)
            for k in rep if k in baseline
        }
        result["baseline_metrics"] = {
            k: v for k, v in baseline.items()
            if k not in ("pcr", "cumulative_returns")
        }
        result["baseline_pcr"] = baseline.get("pcr", {})

    portfolio_value = sum(abs(h.get("market_value", 0))
                          for h in get_holdings(pid)) or 100000
    result["portfolio_value"] = portfolio_value

    # Store scenario
    _sim_scenarios.setdefault(pid, {})
    _sim_scenarios[pid][scenario_name] = result

    return jsonify(_sanitize(result))


@api.route("/simulator/<pid>/historical", methods=["POST", "OPTIONS"])
@token_required
def simulator_historical(pid):
    """Run historical crisis replay."""
    if request.method == "OPTIONS":
        return "", 204

    data = request.get_json() or {}
    scenario_name = data.get("scenario_name", "Historical 1")
    crisis = data.get("crisis", "COVID-19 Crash")

    stored = _sim_scenarios.get(pid, {})
    weights = stored.get("_weights")

    # Use cached stock-level prices from baseline, or build fresh
    prices_df = stored.get("_prices_df")
    if prices_df is None:
        holdings = get_holdings(pid)
        if holdings:
            pf_prices, holdings, _ = _build_portfolio_prices(holdings)
            prices_df = pf_prices if pf_prices is not None and len(pf_prices.columns) >= 1 else generate_prices()
        else:
            prices_df = generate_prices()

    markets = list(prices_df.columns)

    if weights is None:
        weights = compute_weights(prices_df)
        _sim_scenarios.setdefault(pid, {})
        _sim_scenarios[pid]["_weights"] = weights
        _sim_scenarios[pid]["_markets"] = markets

    result = run_historical_replay(prices_df, weights, crisis)
    result["scenario_name"] = scenario_name

    # Add baseline comparison
    baseline = stored.get("_baseline")
    if baseline and "metrics" in result:
        result["baseline_delta"] = {
            k: round(result["metrics"].get(k, 0) - baseline.get(k, 0), 4)
            for k in result["metrics"] if k in baseline
        }
        result["baseline_metrics"] = {
            k: v for k, v in baseline.items()
            if k not in ("pcr", "cumulative_returns")
        }
        result["baseline_pcr"] = baseline.get("pcr", {})

    portfolio_value = sum(abs(h.get("market_value", 0))
                          for h in get_holdings(pid)) or 100000
    result["portfolio_value"] = portfolio_value

    _sim_scenarios.setdefault(pid, {})
    _sim_scenarios[pid][scenario_name] = result

    return jsonify(_sanitize(result))


@api.route("/simulator/<pid>/classify", methods=["POST", "OPTIONS"])
@token_required
def simulator_classify(pid):
    """Classify portfolio assets via Fama-French factor regression."""
    if request.method == "OPTIONS":
        return "", 204

    data = request.get_json() or {}
    factors = data.get("factors", ["FF5", "Mom"])
    shocks = data.get("shocks")

    stored = _sim_scenarios.get(pid, {})

    # Use cached stock-level prices from baseline, or build fresh
    prices_df = stored.get("_prices_df")
    if prices_df is None:
        holdings = get_holdings(pid)
        if holdings:
            pf_prices, holdings, _ = _build_portfolio_prices(holdings)
            prices_df = pf_prices if pf_prices is not None and len(pf_prices.columns) >= 1 else generate_prices()
        else:
            prices_df = generate_prices()

    result = classify_and_stress(prices_df, factors, shocks)
    return jsonify(_sanitize(result))


@api.route("/simulator/<pid>/scenarios")
@token_required
def simulator_scenarios(pid):
    """List all saved scenarios for a portfolio."""
    stored = _sim_scenarios.get(pid, {})
    scenarios = []
    for name, data in stored.items():
        if name.startswith("_"):
            continue
        scenarios.append({
            "name": name,
            "type": data.get("type", "Unknown"),
            "regime": data.get("regime"),
            "stress_level": data.get("stress_level"),
            "crisis": data.get("crisis"),
        })
    return jsonify({"portfolio_id": pid, "scenarios": scenarios})


@api.route("/simulator/<pid>/scenario/<name>")
@token_required
def simulator_get_scenario(pid, name):
    """Get a specific scenario result."""
    stored = _sim_scenarios.get(pid, {})
    if name not in stored or name.startswith("_"):
        return jsonify({"error": f"Scenario '{name}' not found"}), 404
    return jsonify(_sanitize(stored[name]))


@api.route("/simulator/<pid>/kde", methods=["POST", "OPTIONS"])
@token_required
def simulator_kde(pid):
    """Compute KDE distribution curves for a given metric across all scenarios."""
    if request.method == "OPTIONS":
        return "", 204

    data = request.get_json() or {}
    metric = data.get("metric", "annual_volatility")

    stored = _sim_scenarios.get(pid, {})
    scenarios_data = {}

    for name, sc in stored.items():
        if name.startswith("_"):
            continue
        if sc.get("type") == "Monte Carlo":
            # Collect metric across all 3 paths
            vals = []
            for path_key in ["representative", "best", "worst"]:
                path = sc.get("paths", {}).get(path_key, {})
                m = path.get("metrics", {})
                if metric in m:
                    vals.append(m[metric])
            if vals:
                scenarios_data[name] = vals
        else:
            # Historical replay
            m = sc.get("metrics", {})
            if metric in m:
                scenarios_data[name] = m[metric]

    if not scenarios_data:
        return jsonify({"error": "No scenario data available for KDE"}), 404

    result = compute_kde_data(scenarios_data, metric)
    return jsonify(_sanitize(result))
