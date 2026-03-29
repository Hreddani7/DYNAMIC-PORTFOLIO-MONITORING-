"""
Data Ingestion — AfriSK InteliRisk v4 (JSE-only)
Priority: Bloomberg BLPAPI → Real CSV → Synthetic GBM

Loads JSE index + individual stocks, comprehensive SA macro data,
global factors (VIX, DXY, MOVE, commodities), and sovereign yields.
"""
import logging
import os

import numpy as np
import pandas as pd
from datetime import datetime

from app.config import MARKETS, DATA_DIR, N_ANN

log = logging.getLogger(__name__)
_cache = {}


def _load_bloomberg_data():
    """Attempt to load price and macro data from Bloomberg BLPAPI."""
    try:
        from app.bloomberg import get_bloomberg
        blp = get_bloomberg(auto_connect=True)
        if not blp.is_connected():
            return None, None

        price_df = blp.fetch_market_prices(start="20150101")
        macro_df = blp.fetch_macro_data(start="20150101")

        if price_df is not None and len(price_df) >= 100:
            log.info(f"[INGEST] Bloomberg prices loaded: {price_df.shape}")
        else:
            price_df = None

        if macro_df is not None and len(macro_df) >= 100:
            log.info(f"[INGEST] Bloomberg macro loaded:  {macro_df.shape}")
        else:
            macro_df = None

        return price_df, macro_df

    except Exception as exc:
        log.warning(f"[INGEST] Bloomberg load failed: {exc}")
        return None, None


def _try_load_csv(path):
    """Load an Investing.com-style CSV and return a clean price Series."""
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", thousands=",", na_values=["", "-", " "])
        df.columns = [c.strip().strip('"') for c in df.columns]
        df["Date"] = pd.to_datetime(df["Date"], format="mixed", dayfirst=False)
        df = df.set_index("Date").sort_index()
        df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
        return df["Price"].dropna()
    except Exception:
        return None


def _try_load_sarb_csv(path):
    """Load SARB-style CSV (header rows + Date,Value columns)."""
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", skiprows=2, na_values=["", "-", " "])
        df.columns = [c.strip() for c in df.columns]
        df["Date"] = pd.to_datetime(df["Date"].str.strip(), format="mixed", dayfirst=False)
        df = df.set_index("Date").sort_index()
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
        return df["Value"].dropna()
    except Exception:
        return None


def _try_load_fred_csv(path, value_col=None):
    """Load FRED-style CSV (observation_date, value_column)."""
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", na_values=["", "-", " ", "."])
        df.columns = [c.strip() for c in df.columns]
        date_col = [c for c in df.columns if "date" in c.lower()][0]
        val_col = value_col if value_col and value_col in df.columns else df.columns[1]
        df[date_col] = pd.to_datetime(df[date_col], format="mixed", dayfirst=False)
        df = df.set_index(date_col).sort_index()
        df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
        return df[val_col].dropna()
    except Exception:
        return None


def _load_real_data():
    """
    Load real market data from CSV files.
    Returns (price_df, macro_df, stocks_dict) where:
      price_df   — 4-column DataFrame of market index prices
      macro_df   — macro variables (yields, FX, commodities, global)
      stocks_dict — {market_key: {symbol: price_Series}} for CSAD + sector factors
    """
    if not os.path.isdir(DATA_DIR):
        return None, None, {}

    prices = {}
    macro = {}
    stocks = {}

    # ── MARKET INDEX PRICES (JSE only) ───────────────────────

    # JSE_SA: Use the longest available index series
    candidates = [
        ("JSE BANKS INDEX.csv", "SA DOMESTIC FACTORS"),
        ("JSE 40 INDEX.csv", "SA DOMESTIC FACTORS"),
        ("JSE MINING INDEX.csv", "SA DOMESTIC FACTORS"),
    ]
    best_s, best_len = None, 0
    for fn, subdir in candidates:
        s = _try_load_csv(os.path.join(DATA_DIR, subdir, fn))
        if s is not None and len(s) > best_len:
            best_s, best_len = s, len(s)
    if best_s is not None and best_len >= 10:
        prices["JSE_SA"] = best_s

    if len(prices) < 1:
        return None, None, {}

    price_df = pd.DataFrame(prices).sort_index().dropna(how="all")
    if len(price_df) > 0:
        price_df = price_df.ffill().bfill()
        # Remove days with extreme jumps caused by data gaps (> 15% daily move)
        for col in price_df.columns:
            ret = price_df[col].pct_change()
            bad_mask = ret.abs() > 0.15
            if bad_mask.any():
                price_df.loc[bad_mask, col] = np.nan
                price_df[col] = price_df[col].interpolate().ffill().bfill()

    # ── PER-MARKET INDIVIDUAL STOCKS ────────────────────────

    jse_dir = os.path.join(DATA_DIR, "SA DOMESTIC FACTORS")
    jse_stock_map = {
        "AngloAmerican":  "Anglo American Stock Price History.csv",
        "FirstRand":      "FirstRand Ltd Stock Price History.csv",
        "ImpalaPlatinum": "Impala Platinum Holdings Stock Price History.csv",
        "MTN_SA":         "MTN Group Stock Price History.csv",
        "Naspers":        "Naspers Stock Price History.csv",
        "Sasol":          "Sasol Ltd Stock Price History.csv",
        "StandardBank":   "Standard Bank Stock Price History.csv",
        "JSEBanks":       "JSE BANKS INDEX.csv",
        "JSEMining":      "JSE MINING INDEX.csv",
    }
    jse_sdict = {}
    for sym, fn in jse_stock_map.items():
        sv = _try_load_csv(os.path.join(jse_dir, fn))
        if sv is not None and len(sv) >= 5:
            jse_sdict[sym] = sv
    if jse_sdict:
        stocks["JSE_SA"] = jse_sdict

    # ── MACRO DATA ──────────────────────────────────────────

    # Global benchmarks
    s = _try_load_csv(os.path.join(DATA_DIR, "GLOBAL FACTORS", "CBOE Volatility Index Historical Data.csv"))
    if s is not None:
        macro["vix"] = s
    s = _try_load_csv(os.path.join(DATA_DIR, "GLOBAL FACTORS", "MSCI Emerging Markets Historical Data.csv"))
    if s is not None:
        macro["msci_em"] = s

    # USD liquidity
    s = _try_load_csv(os.path.join(DATA_DIR, "USD LIQUIDITY FACTOR", "US Dollar Index Historical Data.csv"))
    if s is not None:
        macro["dxy"] = s
    s = _try_load_csv(os.path.join(DATA_DIR, "USD LIQUIDITY FACTOR", "United States 10-Year Bond Yield Historical Data.csv"))
    if s is not None:
        macro["yield_US"] = s

    # Commodities (all 4 African-relevant)
    s = _try_load_csv(os.path.join(DATA_DIR, "COMMODITY FACTORS", "BRENT OIL DATA.csv"))
    if s is not None:
        macro["oil"] = s
    s = _try_load_csv(os.path.join(DATA_DIR, "COMMODITY FACTORS", "GOLD DATA.csv"))
    if s is not None:
        macro["gold"] = s
    s = _try_load_csv(os.path.join(DATA_DIR, "COMMODITY FACTORS", "XCUUSD DATA.csv"))
    if s is not None:
        macro["copper"] = s
    s = _try_load_csv(os.path.join(DATA_DIR, "COMMODITY FACTORS", "PLATINUM DATA.csv"))
    if s is not None:
        macro["platinum"] = s

    # Sovereign yields — all 4 African countries + US (for spread)
    s = _try_load_csv(os.path.join(DATA_DIR, "SA SOVEREIGN FACTORS", "SA 10Y YIELD.csv"))
    if s is not None:
        macro["yield_SA"] = s
    # FX
    s = _try_load_csv(os.path.join(DATA_DIR, "SA DOMESTIC FACTORS", "USDZAR CURRENCY.csv"))
    if s is not None:
        macro["USDZAR"] = s

    # ── ADDITIONAL SA SOVEREIGN / MACRO ────────────────────────
    # SA 2Y yield
    s = _try_load_csv(os.path.join(DATA_DIR, "SA SOVEREIGN FACTORS", "South Africa 2-Year Bond Yield Historical Data.csv"))
    if s is not None:
        macro["yield_SA_2Y"] = s
    # SA 20Y yield
    s = _try_load_csv(os.path.join(DATA_DIR, "SA SOVEREIGN FACTORS", "South Africa 20-Year Bond Yield Historical Data (1).csv"))
    if s is not None:
        macro["yield_SA_20Y"] = s
    # SA CDS 5Y
    s = _try_load_csv(os.path.join(DATA_DIR, "SA SOVEREIGN FACTORS", "SA CDS 5Y.csv"))
    if s is not None:
        macro["cds_SA"] = s
    # SA Yield Curve Spread (10Y-2Y) — FRED-style CSV
    s = _try_load_fred_csv(os.path.join(DATA_DIR, "SA SOVEREIGN FACTORS", "SA YIELD CURVE (SPREAD).csv"))
    if s is not None:
        macro["yield_spread_SA"] = s

    # ── SA DOMESTIC MACRO ──────────────────────────────────────
    # SARB Repo Rate
    s = _try_load_sarb_csv(os.path.join(DATA_DIR, "SA DOMESTIC FACTORS", "REPO RATE.csv"))
    if s is not None:
        macro["repo_rate"] = s
    # SA JIBAR (Interbank Rate)
    s = _try_load_sarb_csv(os.path.join(DATA_DIR, "SA DOMESTIC FACTORS", "INTERBANK RATE.csv"))
    if s is not None:
        macro["jibar"] = s
    # M2 Money Supply
    s = _try_load_fred_csv(os.path.join(DATA_DIR, "SA DOMESTIC FACTORS", "M2 money supply.csv"), "MYAGM2ZAM189N")
    if s is not None:
        macro["m2_supply"] = s

    # CPI Inflation (World Bank — yearly, interpolated to daily)
    try:
        cpi_path = os.path.join(DATA_DIR, "SA DOMESTIC FACTORS", "API_FP.CPI.TOTL.ZG_DS2_en_csv_v2_84.csv")
        cpi_df = pd.read_csv(cpi_path, skiprows=4)
        sa_row = cpi_df[cpi_df["Country Name"] == "South Africa"]
        if len(sa_row) > 0:
            years = [c for c in cpi_df.columns if c.isdigit()]
            cpi_vals = sa_row[years].values.flatten()
            cpi_dates = pd.to_datetime([f"{y}-06-30" for y in years])
            cpi_s = pd.Series(cpi_vals, index=cpi_dates, dtype=float).dropna()
            if len(cpi_s) > 5:
                # Resample yearly to daily via interpolation
                cpi_daily = cpi_s.resample("D").interpolate(method="linear")
                macro["cpi_sa"] = cpi_daily
    except Exception:
        pass

    # M3 Money Supply YoY (SARB — tab-separated monthly)
    try:
        m3_path = os.path.join(DATA_DIR, "SA DOMESTIC FACTORS", "south-africa.sarb-m3-money-supply-yy.csv")
        m3_df = pd.read_csv(m3_path, sep="\t", encoding="utf-8-sig", na_values=["", " "])
        if len(m3_df.columns) == 1 and "\t" in m3_df.columns[0]:
            # Re-parse — columns merged
            m3_df = pd.read_csv(m3_path, sep="\t", encoding="utf-8-sig")
        if "Date" in m3_df.columns and "ActualValue" in m3_df.columns:
            m3_df["Date"] = pd.to_datetime(m3_df["Date"].str.strip(), format="mixed")
            m3_df["ActualValue"] = pd.to_numeric(m3_df["ActualValue"], errors="coerce")
            m3_s = m3_df.set_index("Date")["ActualValue"].dropna().sort_index()
            if len(m3_s) > 5:
                m3_daily = m3_s.resample("D").interpolate(method="linear")
                macro["m3_supply_yoy"] = m3_daily
    except Exception:
        pass

    # ── GLOBAL EXTRAS ──────────────────────────────────────────
    # Euro Stoxx 50
    s = _try_load_csv(os.path.join(DATA_DIR, "GLOBAL FACTORS", "Euro Stoxx 50 Historical Data.csv"))
    if s is not None:
        macro["eurostoxx"] = s
    # MOVE Index (bond vol)
    s = _try_load_csv(os.path.join(DATA_DIR, "USD LIQUIDITY FACTOR", "ICE BofAML MOVE Historical Data.csv"))
    if s is not None:
        macro["move"] = s
    # Fed Funds Rate
    s = _try_load_fred_csv(os.path.join(DATA_DIR, "USD LIQUIDITY FACTOR", "FED FUNDS RATE DATA.csv"), "FEDFUNDS")
    if s is not None:
        macro["fed_funds"] = s

    macro_df = pd.DataFrame(macro).sort_index().ffill().bfill() if macro else None

    return price_df, macro_df, stocks


def _load_ibkr_data():
    """Attempt to load price, macro, and stock data from TWS.
    Returns (price_df, macro_df, stocks_dict) or (None, None, {}).
    Uses a thread with timeout to avoid blocking if TWS hangs.
    """
    try:
        import socket
        # Quick-check if TWS port is open (1s timeout)
        try:
            _sock = socket.create_connection(("127.0.0.1", 7497), timeout=1)
            _sock.close()
        except (ConnectionRefusedError, OSError, socket.timeout):
            return None, None, {}

        from app.ibkr import get_ibkr
        from app.ibkr.tws import IBKR_STOCK_MAP

        # Run connection + fetch in a thread with 15s timeout
        import threading
        _result = [None, None, {}]
        _error = [None]

        def _tws_worker():
            try:
                import sys, io, logging as _log
                # Suppress noisy ib_insync connection errors
                _log.getLogger("ib_insync").setLevel(_log.CRITICAL)
                _old_stderr = sys.stderr
                sys.stderr = io.StringIO()
                try:
                    ibkr = get_ibkr(auto_connect=True)
                    connected = ibkr.is_connected()
                finally:
                    sys.stderr = _old_stderr
                    _log.getLogger("ib_insync").setLevel(_log.WARNING)
                if not connected:
                    return
                prices_d, macro_d, stocks_raw = ibkr.fetch_all_historical()
                _result[0] = prices_d
                _result[1] = macro_d
                _result[2] = stocks_raw or {}
            except Exception as e:
                _error[0] = e

        t = threading.Thread(target=_tws_worker, daemon=True)
        t.start()
        t.join(timeout=15)
        if t.is_alive():
            log.warning("[INGEST] TWS connection timed out (15s)")
            return None, None, {}
        if _error[0]:
            raise _error[0]

        prices_d, macro_d, stocks_raw = _result
        if not prices_d and not macro_d:
            return None, None, {}

        prices_d, macro_d, stocks_raw = ibkr.fetch_all_historical()
        if not prices_d and not macro_d:
            return None, None, {}

        price_df = pd.DataFrame(prices_d).sort_index().ffill().bfill() if prices_d else None
        macro_df = pd.DataFrame(macro_d).sort_index().ffill().bfill() if macro_d else None

        # Map raw TWS stock keys to ingestion names
        stocks = {}
        if stocks_raw:
            jse_sdict = {}
            for key, series in stocks_raw.items():
                mapped = IBKR_STOCK_MAP.get(key, key)
                jse_sdict[mapped] = series
            if jse_sdict:
                stocks["JSE_SA"] = jse_sdict
            log.info(f"[INGEST] TWS stocks loaded: {list(jse_sdict.keys())}")

        if price_df is not None:
            log.info(f"[INGEST] TWS prices loaded: {price_df.shape}")
        if macro_df is not None:
            log.info(f"[INGEST] TWS macro loaded: {macro_df.shape}")

        return price_df, macro_df, stocks
    except Exception as exc:
        log.warning(f"[INGEST] IBKR TWS load failed: {exc}")
        return None, None, {}


def generate_prices(n_days=500):
    """Load market index prices: Bloomberg → TWS → CSV → Synthetic.

    Priority order:
    1. Bloomberg BLPAPI (supplemented by CSV)
    2. IBKR TWS historical data (supplemented by CSV)
    3. Real CSV files only
    4. Synthetic GBM fallback
    """
    if "prices" in _cache:
        return _cache["prices"]

    # Always load CSV as base data
    real_p, real_m, real_stocks = _load_real_data()
    if real_stocks:
        _cache["stocks"] = real_stocks

    # Priority 1: Bloomberg BLPAPI
    blp_p, blp_m = _load_bloomberg_data()
    if blp_p is not None and len(blp_p) >= 100:
        price_df = blp_p.copy()
        merged_m = blp_m.copy() if blp_m is not None else pd.DataFrame()
        if real_m is not None:
            for col in real_m.columns:
                if col not in merged_m.columns:
                    merged_m = merged_m.join(
                        real_m[[col]].reindex(merged_m.index).ffill(),
                        how="left",
                    )
        if len(merged_m) > 0:
            _cache["macro"] = merged_m
        _cache["prices"] = price_df
        _cache["data_source"] = "bloomberg"
        print(f"[DATA] Loaded BLOOMBERG+CSV data: {price_df.shape} | markets: {list(price_df.columns)}")
        return price_df

    # Priority 2: IBKR TWS historical data (supplement with CSV)
    tws_p, tws_m, tws_stocks = _load_ibkr_data()
    # Trigger TWS path if we got prices OR stocks OR macro from TWS
    tws_has_data = (
        (tws_p is not None and len(tws_p) >= 50) or
        (tws_stocks and any(len(sd) >= 5 for sd in tws_stocks.values())) or
        (tws_m is not None and len(tws_m) >= 50)
    )
    if tws_has_data:
        # Use TWS prices if available, otherwise fall back to CSV prices
        if tws_p is not None and len(tws_p) >= 50:
            price_df = tws_p.copy()
        elif real_p is not None:
            price_df = real_p.copy()
        else:
            price_df = pd.DataFrame()

        # Supplement with CSV for any missing price columns
        if real_p is not None:
            for col in real_p.columns:
                if col not in price_df.columns:
                    price_df[col] = real_p[col].reindex(price_df.index).ffill()

        # Merge macro: TWS base + CSV supplements
        merged_m = tws_m.copy() if tws_m is not None else pd.DataFrame()
        if real_m is not None:
            for col in real_m.columns:
                if col not in merged_m.columns:
                    if len(merged_m) > 0:
                        merged_m = merged_m.join(
                            real_m[[col]].reindex(merged_m.index).ffill(),
                            how="left",
                        )
                    else:
                        merged_m[col] = real_m[col]
        if len(merged_m) > 0:
            _cache["macro"] = merged_m

        # Merge stocks: TWS stocks override CSV
        if tws_stocks:
            merged_stocks = _cache.get("stocks", {})
            for mkt, sdict in tws_stocks.items():
                if mkt not in merged_stocks:
                    merged_stocks[mkt] = {}
                merged_stocks[mkt].update(sdict)
            _cache["stocks"] = merged_stocks

        if len(price_df) > 0:
            _cache["prices"] = price_df
            _cache["data_source"] = "tws"
            tws_stock_count = sum(len(sd) for sd in tws_stocks.values()) if tws_stocks else 0
            print(f"[DATA] Loaded TWS+CSV data: prices={price_df.shape} | stocks={tws_stock_count} from TWS | markets: {list(price_df.columns)}")
            return price_df

    # Priority 3: Real CSV files only
    if real_p is not None and len(real_p) >= 100:
        _cache["prices"] = real_p
        if real_m is not None:
            _cache["macro"] = real_m
        _cache["data_source"] = "csv"
        print(f"[DATA] Loaded REAL CSV data: {real_p.shape} | markets: {list(real_p.columns)}")
        return real_p

    # Fallback: Regime-switching synthetic GBM (JSE only)
    np.random.seed(42)
    dates = pd.bdate_range(end=datetime.now(), periods=n_days)

    regime_probs = np.random.dirichlet([5, 1, 1, 1, 1], size=n_days)
    vols = np.array([0.012, 0.025, 0.028, 0.035, 0.055])
    drifts = np.array([0.0003, -0.0008, -0.0015, -0.002, -0.005])

    returns = np.zeros(n_days)
    for t in range(n_days):
        vol = np.dot(regime_probs[t], vols)
        drift = np.dot(regime_probs[t], drifts)
        returns[t] = drift + vol * np.random.randn()

    df = pd.DataFrame({"JSE_SA": 15000 * np.exp(np.cumsum(returns))}, index=dates)
    _cache["prices"] = df
    _cache["data_source"] = "synthetic"
    print(f"[DATA] Generated SYNTHETIC data: {df.shape}")
    return df


def generate_macro(n_days=500):
    """Load macro data: Bloomberg → CSV → Synthetic."""
    if "macro" in _cache:
        return _cache["macro"]

    # generate_prices() populates macro cache from Bloomberg or CSV
    generate_prices(n_days)
    if "macro" in _cache:
        return _cache["macro"]

    # Synthetic fallback — all 4 African countries + global
    np.random.seed(123)
    dates = pd.bdate_range(end=datetime.now(), periods=n_days)
    data = {}

    for name, (mean, std) in {
        "vix": (18, 8), "dxy": (110, 5),
        "oil": (75, 15), "gold": (1900, 200),
        "copper": (8500, 1000), "platinum": (950, 100),
        "msci_em": (1050, 150),
    }.items():
        data[name] = np.maximum(mean + std * np.cumsum(np.random.randn(n_days) * 0.02), mean * 0.3)

    for country, base in [
        ("yield_US", 4.5), ("yield_SA", 9.0),
    ]:
        data[country] = base + 2.0 * np.cumsum(np.random.randn(n_days) * 0.01)

    data["USDZAR"] = 18.5 * np.exp(np.cumsum(np.random.randn(n_days) * 0.003))

    df = pd.DataFrame(data, index=dates)
    _cache["macro"] = df
    return df


def generate_stocks_per_market():
    """
    Return per-market individual stock price levels.
    {market_key: {symbol: pd.Series(price_levels)}}
    Used by Layer 3 for CSAD herding and sector factor computation.
    """
    if "stocks" not in _cache:
        generate_prices()  # populates cache including stocks
    return _cache.get("stocks", {})


def get_data_source():
    """Return current data source: bloomberg, tws, csv, or synthetic."""
    return _cache.get("data_source", "unknown")


def clear_cache():
    _cache.clear()
