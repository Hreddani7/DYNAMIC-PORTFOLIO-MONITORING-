"""
IBKR TWS / IB Gateway integration using ib_insync.

Capabilities:
- Fetch historical bars for any instrument (stocks, FX, commodities, indices)
- Pull live portfolio positions from a connected TWS account
- Export portfolio as CSV for upload

Connection: TWS on 127.0.0.1:7497 (paper) or 7496 (live)
            IB Gateway on 127.0.0.1:4001 (paper) or 4002 (live)
"""
import logging
import os
from datetime import datetime

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

try:
    from ib_insync import IB, Stock, Forex, Index, Future, ContFuture, util
    _HAS_IBINSYNC = True
except ImportError:
    _HAS_IBINSYNC = False

_instance = None

# ── Instrument definitions for African risk factors ───────────
# These are what we fetch from IBKR TWS for the risk engine

HIST_INSTRUMENTS = {
    # ── FX ────────────────────────────────────────────────────
    "USDZAR":    {"type": "Forex",  "symbol": "USD",     "exchange": "IDEALPRO","currency": "ZAR"},

    # ── Commodities (use ContFuture for continuous front-month) ──
    "BRENT":     {"type": "ContFuture", "symbol": "BZ",  "exchange": "NYMEX",  "currency": "USD"},
    "GOLD":      {"type": "ContFuture", "symbol": "GC",  "exchange": "COMEX",  "currency": "USD"},
    "COPPER":    {"type": "ContFuture", "symbol": "HG",  "exchange": "COMEX",  "currency": "USD"},
    "PLATINUM":  {"type": "ContFuture", "symbol": "PL",  "exchange": "NYMEX",  "currency": "USD"},

    # ── Global ────────────────────────────────────────────────
    "VIX":       {"type": "Index",  "symbol": "VIX",     "exchange": "CBOE",   "currency": "USD"},

    # ── US Yields (continuous future) ─────────────────────────
    "US10Y":     {"type": "ContFuture", "symbol": "ZN",  "exchange": "CBOT",   "currency": "USD"},

    # ── JSE TOP 40 STOCKS (full J200 constituents) ────────────
    # Mega-cap
    "NPN":  {"type": "Stock", "symbol": "NPN",  "exchange": "JSE", "currency": "ZAR"},
    "PRX":  {"type": "Stock", "symbol": "PRX",  "exchange": "JSE", "currency": "ZAR"},
    "CFR":  {"type": "Stock", "symbol": "CFR",  "exchange": "JSE", "currency": "ZAR"},
    "BTI":  {"type": "Stock", "symbol": "BTI",  "exchange": "JSE", "currency": "ZAR"},
    "AGL":  {"type": "Stock", "symbol": "AGL",  "exchange": "JSE", "currency": "ZAR"},
    "BHP":  {"type": "Stock", "symbol": "BHG",  "exchange": "JSE", "currency": "ZAR"},
    "GLN":  {"type": "Stock", "symbol": "GLN",  "exchange": "JSE", "currency": "ZAR"},
    # Banks
    "SBK":  {"type": "Stock", "symbol": "SBK",  "exchange": "JSE", "currency": "ZAR"},
    "FSR":  {"type": "Stock", "symbol": "FSR",  "exchange": "JSE", "currency": "ZAR"},
    "ABG":  {"type": "Stock", "symbol": "ABG",  "exchange": "JSE", "currency": "ZAR"},
    "NED":  {"type": "Stock", "symbol": "NED",  "exchange": "JSE", "currency": "ZAR"},
    "CPI":  {"type": "Stock", "symbol": "CPI",  "exchange": "JSE", "currency": "ZAR"},
    "INP":  {"type": "Stock", "symbol": "INP",  "exchange": "JSE", "currency": "ZAR"},
    # Insurance / Financial
    "SLM":  {"type": "Stock", "symbol": "SLM",  "exchange": "JSE", "currency": "ZAR"},
    "DSY":  {"type": "Stock", "symbol": "DSY",  "exchange": "JSE", "currency": "ZAR"},
    "OMU":  {"type": "Stock", "symbol": "OMU",  "exchange": "JSE", "currency": "ZAR"},
    "REM":  {"type": "Stock", "symbol": "REM",  "exchange": "JSE", "currency": "ZAR"},
    "RNI":  {"type": "Stock", "symbol": "RNI",  "exchange": "JSE", "currency": "ZAR"},
    # Mining
    # AMS (Anglo American Platinum) — delisted/restructured, not on IBKR
    "IMP":  {"type": "Stock", "symbol": "IMP",  "exchange": "JSE", "currency": "ZAR"},
    "GFI":  {"type": "Stock", "symbol": "GFI",  "exchange": "JSE", "currency": "ZAR"},
    "SSW":  {"type": "Stock", "symbol": "SSW",  "exchange": "JSE", "currency": "ZAR"},
    "ANG":  {"type": "Stock", "symbol": "ANG",  "exchange": "JSE", "currency": "ZAR"},
    "KIO":  {"type": "Stock", "symbol": "KIO",  "exchange": "JSE", "currency": "ZAR"},
    "EXX":  {"type": "Stock", "symbol": "EXX",  "exchange": "JSE", "currency": "ZAR"},
    "S32":  {"type": "Stock", "symbol": "S32",  "exchange": "JSE", "currency": "ZAR"},
    "HAR":  {"type": "Stock", "symbol": "HAR",  "exchange": "JSE", "currency": "ZAR"},
    # Telecom / Tech
    "MTN":  {"type": "Stock", "symbol": "MTN",  "exchange": "JSE", "currency": "ZAR"},
    "VOD":  {"type": "Stock", "symbol": "VOD",  "exchange": "JSE", "currency": "ZAR"},
    # Energy / Chemicals
    "SOL":  {"type": "Stock", "symbol": "SOL",  "exchange": "JSE", "currency": "ZAR"},
    # Retail / Consumer
    "SHP":  {"type": "Stock", "symbol": "SHP",  "exchange": "JSE", "currency": "ZAR"},
    "MRP":  {"type": "Stock", "symbol": "MRP",  "exchange": "JSE", "currency": "ZAR"},
    "WHL":  {"type": "Stock", "symbol": "WHL",  "exchange": "JSE", "currency": "ZAR"},
    "TBS":  {"type": "Stock", "symbol": "TBS",  "exchange": "JSE", "currency": "ZAR"},
    "PIK":  {"type": "Stock", "symbol": "PIK",  "exchange": "JSE", "currency": "ZAR"},
    # Healthcare
    "APN":  {"type": "Stock", "symbol": "APN",  "exchange": "JSE", "currency": "ZAR"},
    # Industrials / Paper
    "MNP":  {"type": "Stock", "symbol": "MNP",  "exchange": "JSE", "currency": "ZAR"},
    "BID":  {"type": "Stock", "symbol": "BID",  "exchange": "JSE", "currency": "ZAR"},
    # REITs / Property
    "GRT":  {"type": "Stock", "symbol": "GRT",  "exchange": "JSE", "currency": "ZAR"},
    # Other
    "INL":  {"type": "Stock", "symbol": "INL",  "exchange": "JSE", "currency": "ZAR"},
    # MCG (MultiChoice) — delisted, not on IBKR
}

# Map IBKR keys to ingestion macro keys
IBKR_TO_MACRO = {
    "VIX": "vix",
    "BRENT": "oil", "GOLD": "gold", "COPPER": "copper", "PLATINUM": "platinum",
    "USDZAR": "USDZAR", "US10Y": "yield_US",
}

IBKR_TO_PRICE = {
    # JSE indices not available on IBKR — constructed from stocks or CSV
}

# Map TWS stock keys → ingestion stock names (for Layer 3 CSAD)
IBKR_STOCK_MAP = {
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
    "TBS": "TigerBrands", "PIK": "PicknPay",
    # Healthcare
    "APN": "Aspen",
    # Industrials
    "MNP": "Mondi", "BID": "BidCorp",
    # REITs
    "GRT": "Growthpoint",
    # Other
    "INL": "InvestecLtd", "MCG": "MultiChoice",
    # Indices
    "JSE_BANKS": "JSEBanks", "JSE_MINING": "JSEMining",
}


class IBKRProvider:
    """Thread-safe IBKR TWS connection provider."""

    def __init__(self, host="127.0.0.1", port=7497, client_id=10):
        self.host = host
        self.port = port
        self.client_id = client_id
        self._ib = None
        self._connected = False

    def connect(self):
        if not _HAS_IBINSYNC:
            log.warning("[IBKR] ib_insync not installed")
            return False
        # Already connected — skip
        if self._ib and self._connected:
            try:
                if self._ib.isConnected():
                    return True
            except Exception:
                pass
            self._connected = False
        try:
            import asyncio
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            self._ib = IB()
            # Try multiple clientIds to avoid "already in use" errors
            for cid in [self.client_id, self.client_id + 1, self.client_id + 2,
                         self.client_id + 10, self.client_id + 20]:
                try:
                    self._ib.connect(self.host, self.port, clientId=cid, timeout=4)
                    self._connected = self._ib.isConnected()
                    if self._connected:
                        self.client_id = cid
                        log.info(f"[IBKR] Connected to TWS at {self.host}:{self.port} (clientId={cid})")
                        return True
                except Exception:
                    try:
                        self._ib.disconnect()
                    except Exception:
                        pass
                    self._ib = IB()
                    continue
            log.warning(f"[IBKR] All clientId attempts failed")
            self._connected = False
            return False
        except Exception as e:
            log.warning(f"[IBKR] Connection failed: {e}")
            self._connected = False
            return False

    def disconnect(self):
        if self._ib and self._connected:
            self._ib.disconnect()
            self._connected = False

    def is_connected(self):
        if self._ib is None:
            return False
        try:
            return self._ib.isConnected()
        except Exception:
            return False

    def status(self):
        connected = self.is_connected()
        return {
            "connected": connected,
            "ib_insync_available": _HAS_IBINSYNC,
            "host": self.host,
            "port": self.port,
            "client_id": self.client_id,
        }

    def _make_contract(self, spec):
        """Create an ib_insync contract from instrument spec."""
        if not _HAS_IBINSYNC:
            return None
        t = spec["type"]
        try:
            if t == "Stock":
                c = Stock(spec["symbol"], spec["exchange"], spec["currency"])
            elif t == "Forex":
                c = Forex(spec["symbol"] + spec["currency"])
            elif t == "Index":
                c = Index(spec["symbol"], spec["exchange"], spec["currency"])
            elif t == "Future":
                c = Future(spec["symbol"], exchange=spec["exchange"],
                           currency=spec["currency"])
            elif t == "ContFuture":
                c = ContFuture(spec["symbol"], exchange=spec["exchange"],
                               currency=spec["currency"])
            else:
                return None
            # Qualify ALL contract types to resolve ambiguity
            try:
                if self._ib and self._connected:
                    qualified = self._ib.qualifyContracts(c)
                    if qualified:
                        log.info(f"[IBKR] Qualified contract: {spec['symbol']} → {qualified[0]}")
                        return qualified[0]
                    else:
                        log.warning(f"[IBKR] Could not qualify {spec['symbol']} ({t})")
                        return c  # Return unqualified as fallback
            except Exception as qe:
                log.warning(f"[IBKR] Qualification failed for {spec['symbol']}: {qe}")
            return c
        except Exception as e:
            log.warning(f"[IBKR] Contract creation failed for {spec}: {e}")
            return None

    def fetch_historical(self, key, duration="10 Y", bar_size="1 day"):
        """Fetch historical bars for a single instrument."""
        if not self.is_connected():
            return None
        spec = HIST_INSTRUMENTS.get(key)
        if not spec:
            return None
        contract = self._make_contract(spec)
        if contract is None:
            return None
        try:
            bars = self._ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow="MIDPOINT" if spec["type"] == "Forex" else "TRADES",
                useRTH=True,
                formatDate=1,
            )
            if not bars:
                return None
            df = util.df(bars)
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date").sort_index()
            return df["close"].rename(key)
        except Exception as e:
            log.warning(f"[IBKR] Historical fetch failed for {key}: {e}")
            return None

    def fetch_all_historical(self):
        """Fetch historical data for all configured instruments.
        Returns (prices, macro, stocks) where stocks = {symbol: Series}.
        """
        if not self.is_connected():
            return {}, {}, {}

        prices = {}
        macro = {}
        stocks = {}

        for key, spec in HIST_INSTRUMENTS.items():
            series = self.fetch_historical(key)
            if series is None or len(series) < 10:
                log.warning(f"[IBKR] Skipped {key}: no data or < 10 bars")
                continue

            if key in IBKR_TO_PRICE:
                prices[IBKR_TO_PRICE[key]] = series
            elif key in IBKR_TO_MACRO:
                macro[IBKR_TO_MACRO[key]] = series
            else:
                # Individual stocks → store for Layer 3 CSAD + sector factors
                stocks[key] = series

            log.info(f"[IBKR] Fetched {key}: {len(series)} bars")

        return prices, macro, stocks

    def fetch_portfolio(self):
        """Pull live portfolio positions from TWS."""
        if not self.is_connected():
            return []
        try:
            positions = self._ib.positions()
            holdings = []
            for pos in positions:
                c = pos.contract
                holdings.append({
                    "asset_id": c.symbol,
                    "market": f"{c.exchange or c.primaryExchange}",
                    "sector": c.secType,
                    "quantity": float(pos.position),
                    "price": float(pos.avgCost),
                    "market_value": float(pos.position * pos.avgCost),
                    "currency": c.currency,
                    "weight": 0,
                })
            # Compute weights
            total = sum(abs(h["market_value"]) for h in holdings)
            if total > 0:
                for h in holdings:
                    h["weight"] = round(abs(h["market_value"]) / total, 4)
            return holdings
        except Exception as e:
            log.warning(f"[IBKR] Portfolio fetch failed: {e}")
            return []

    def export_portfolio_csv(self, path=None):
        """Export portfolio positions to CSV."""
        holdings = self.fetch_portfolio()
        if not holdings:
            return None
        df = pd.DataFrame(holdings)
        if path is None:
            path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.abspath(__file__)))), "uploads", "ibkr_portfolio.csv")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False)
        return path


def get_ibkr(auto_connect=False, host="127.0.0.1", port=7497, client_id=None):
    """Get or create the singleton IBKR provider."""
    global _instance
    if client_id is None:
        import random
        client_id = random.randint(50, 999)
    if _instance is None:
        _instance = IBKRProvider(host=host, port=port, client_id=client_id)
    if auto_connect and not _instance.is_connected():
        _instance.connect()
    return _instance
