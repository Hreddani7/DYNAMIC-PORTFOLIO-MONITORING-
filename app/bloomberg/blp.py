"""
Bloomberg BLPAPI Data Provider — AfriSK InteliRisk v4
Connects to a locally-running Bloomberg Terminal session (localhost:8194).

Data hierarchy:
  1. Bloomberg live subscription  (real-time quotes)
  2. Bloomberg historical BDH     (replaces CSV files for the 8-layer pipeline)
  3. CSV files                    (existing data/intellirisk/ folder)
  4. Synthetic GBM fallback       (always works for demos)

Tickers used:
  JSE_SA  → JALSH Index  (JSE All-Share Index)
  MASI_MA → MASI Index   (Moroccan All-Shares Index)
  EGX_EG  → EGX30 Index  (Egyptian Exchange EGX-30)
  NGX_NG  → NGXINDX Index (NGX All-Share Index)
"""
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# ── Ticker maps ─────────────────────────────────────────────────────────────

MARKET_TICKERS: Dict[str, str] = {
    "JSE_SA":  "JALSH Index",      # FTSE/JSE All-Share Index
    # "MASI_MA": "MASI Index",     # NOTE: requires Casablanca exchange subscription
    "EGX_EG":  "EGX30 Index",      # EGX 30 Index
    "NGX_NG":  "NGXINDX Index",    # NGX All-Share Index
}

MACRO_TICKERS: Dict[str, str] = {
    "vix":      "VIX Index",
    "dxy":      "DXY Curncy",
    "oil":      "CO1 Comdty",
    "gold":     "GC1 Comdty",
    "yield_SA": "GSAB10YR Index",
    "yield_MA": "GSMC10YR Index",
    "yield_NG": "GNIB10YR Index",
    "USDZAR":   "USDZAR BGN Curncy",
    "USDMAD":   "USDMAD BGN Curncy",
    "USDNGN":   "USDNGN BGN Curncy",
}

# Reverse maps: Bloomberg ticker → internal label
_MKT_REV  = {v: k for k, v in MARKET_TICKERS.items()}
_MCR_REV  = {v: k for k, v in MACRO_TICKERS.items()}
_ALL_REV  = {**_MKT_REV, **_MCR_REV}

# Live subscription fields
_LIVE_FIELDS = ["LAST_PRICE", "CHG_PCT_1D", "BID", "ASK", "VOLUME"]
# Reference (snapshot) fields
_REF_FIELDS  = ["PX_LAST", "CHG_PCT_1D", "VOLUME", "NAME"]


# ── Helper ───────────────────────────────────────────────────────────────────

def _blpdate_to_str(d) -> str:
    """Convert blpapi.Datetime / datetime.date / str to 'YYYY-MM-DD'."""
    try:
        return f"{d.year:04d}-{d.month:02d}-{d.day:02d}"
    except AttributeError:
        return str(d)[:10]


# ── Provider ─────────────────────────────────────────────────────────────────

class BloombergProvider:
    """
    Thread-safe Bloomberg data provider.

    Usage:
        blp = BloombergProvider()
        if blp.connect():
            prices = blp.fetch_market_prices()   # pd.DataFrame for pipeline
            macro  = blp.fetch_macro_data()
            quotes = blp.get_live_quotes()        # dict with real-time prices
            blp.start_subscription()             # optional: live push
    """

    def __init__(self, host: str = "localhost", port: int = 8194):
        self.host = host
        self.port = port

        self._blpapi = None          # blpapi module (None if not installed)
        self._session = None         # historical/reference session
        self._sub_session = None     # subscription session
        self._connected = False

        self._live_quotes: Dict[str, dict] = {}
        self._lock = threading.Lock()
        self._sub_thread: Optional[threading.Thread] = None
        self._stop_sub = threading.Event()

        self._try_import()

    # ── Import ----------------------------------------------------------------

    def _try_import(self):
        try:
            import blpapi
            self._blpapi = blpapi
            log.info("[BLP] blpapi module loaded successfully")
        except ImportError:
            log.warning("[BLP] blpapi not installed — Bloomberg disabled")

    # ── Connection -----------------------------------------------------------

    def connect(self) -> bool:
        """
        Open a synchronous Bloomberg session for historical/reference data.
        Returns True if connected, False otherwise.
        """
        if self._blpapi is None:
            return False
        if self._connected:
            return True
        blp = self._blpapi
        try:
            opts = blp.SessionOptions()
            opts.setServerHost(self.host)
            opts.setServerPort(self.port)
            self._session = blp.Session(opts)

            if not self._session.start():
                log.error("[BLP] Session.start() failed — is Bloomberg Terminal running?")
                self._session = None
                return False

            if not self._session.openService("//blp/refdata"):
                log.error("[BLP] openService('//blp/refdata') failed")
                self._session.stop()
                self._session = None
                return False

            self._connected = True
            log.info(f"[BLP] Connected to Bloomberg Terminal at {self.host}:{self.port}")
            return True

        except Exception as exc:
            log.error(f"[BLP] Connection error: {exc}")
            self._session = None
            return False

    def is_connected(self) -> bool:
        return self._connected and self._session is not None

    def disconnect(self):
        self._stop_sub.set()
        if self._sub_session:
            try:
                self._sub_session.stop()
            except Exception:
                pass
            self._sub_session = None
        if self._session:
            try:
                self._session.stop()
            except Exception:
                pass
            self._session = None
        self._connected = False
        log.info("[BLP] Disconnected")

    # ── Historical data (BDH) ------------------------------------------------

    def _bdh(self, tickers: List[str], field: str,
             start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        Bloomberg Historical Data Request for a single field.
        Returns DataFrame: index=date, columns=tickers (internal labels).

        Handles large requests: Bloomberg may send multiple PARTIAL_RESPONSE
        events before the final RESPONSE.  We wait up to 120 s total.
        """
        if not self.is_connected():
            return None
        blp = self._blpapi
        try:
            svc = self._session.getService("//blp/refdata")
            req = svc.createRequest("HistoricalDataRequest")

            for t in tickers:
                req.getElement("securities").appendValue(t)
            req.getElement("fields").appendValue(field)

            req.set("periodicitySelection", "DAILY")
            req.set("startDate", start_date)
            req.set("endDate", end_date)
            req.set("nonTradingDayFillOption", "ACTIVE_DAYS_ONLY")
            req.set("adjustmentFollowDPDF", True)

            self._session.sendRequest(req)

            results: Dict[str, Dict[str, float]] = {}   # label → {date_str: price}
            deadline = time.time() + 120                # 2-minute overall timeout

            while time.time() < deadline:
                event = self._session.nextEvent(15_000)  # 15-second per-event timeout

                for msg in event:
                    if msg.messageType() == blp.Name("HistoricalDataResponse"):
                        sec_data = msg.getElement("securityData")
                        ticker   = sec_data.getElementAsString("security")
                        fd_array = sec_data.getElement("fieldData")

                        rows: Dict[str, float] = {}
                        for i in range(fd_array.numValues()):
                            item = fd_array.getValue(i)
                            try:
                                d_raw = item.getElementAsDatetime("date")
                                d_str = _blpdate_to_str(d_raw)
                                price = item.getElementAsFloat(field)
                                rows[d_str] = price
                            except Exception as e:
                                log.debug(f"[BLP] Row parse error: {e}")
                                continue

                        if rows:
                            label = _ALL_REV.get(ticker, ticker)
                            results[label] = rows

                # Final RESPONSE → we have all data
                if event.eventType() == blp.Event.RESPONSE:
                    break
                # TIMEOUT with partial data → keep waiting; TIMEOUT with no data → stop
                if event.eventType() == blp.Event.TIMEOUT and not results:
                    log.warning("[BLP] BDH timeout with no data received")
                    break
                # PARTIAL_RESPONSE → continue collecting

            if not results:
                return None

            df = pd.DataFrame(results)
            df.index = pd.to_datetime(df.index)
            return df.sort_index()

        except Exception as exc:
            log.error(f"[BLP] BDH error: {exc}")
            return None

    def fetch_market_prices(self,
                            start: str = "20200101",
                            end:   str = None) -> Optional[pd.DataFrame]:
        """
        Fetch daily closing prices for the 4 African market indices.
        Returns DataFrame with columns = ['JSE_SA', 'MASI_MA', 'EGX_EG', 'NGX_NG'].
        """
        if not self.is_connected():
            return None
        if end is None:
            end = datetime.now().strftime("%Y%m%d")

        log.info(f"[BLP] Fetching market prices {start}→{end}")
        df = self._bdh(list(MARKET_TICKERS.values()), "PX_LAST", start, end)

        if df is None or df.empty:
            log.warning("[BLP] No market price data returned")
            return None

        # Keep only known market columns; fill minor gaps
        known = [c for c in df.columns if c in MARKET_TICKERS]
        if not known:
            return None

        df = df[known].sort_index()
        df = df.ffill(limit=5)             # forward-fill up to 5 missing days
        df = df.dropna(how="all")
        log.info(f"[BLP] Market prices: {df.shape} rows×cols")
        return df

    def fetch_macro_data(self,
                         start: str = "20200101",
                         end:   str = None) -> Optional[pd.DataFrame]:
        """
        Fetch daily macro series (VIX, DXY, Oil, Gold, yields, FX).
        Returns DataFrame with internal label columns.
        """
        if not self.is_connected():
            return None
        if end is None:
            end = datetime.now().strftime("%Y%m%d")

        log.info(f"[BLP] Fetching macro data {start}→{end}")
        df = self._bdh(list(MACRO_TICKERS.values()), "PX_LAST", start, end)

        if df is None or df.empty:
            log.warning("[BLP] No macro data returned")
            return None

        df = df.sort_index().ffill(limit=5).dropna(how="all")
        log.info(f"[BLP] Macro data: {df.shape}")
        return df

    # ── Reference data (BDP) — snapshot quotes ───────────────────────────────

    def _bdp(self, tickers: List[str],
             fields: List[str]) -> Dict[str, Dict[str, object]]:
        """Bloomberg Reference Data Request (current values)."""
        if not self.is_connected():
            return {}
        blp = self._blpapi
        try:
            svc = self._session.getService("//blp/refdata")
            req = svc.createRequest("ReferenceDataRequest")
            for t in tickers:
                req.getElement("securities").appendValue(t)
            for f in fields:
                req.getElement("fields").appendValue(f)

            self._session.sendRequest(req)
            results: Dict[str, Dict[str, object]] = {}

            while True:
                event = self._session.nextEvent(5_000)
                for msg in event:
                    if msg.messageType() == blp.Name("ReferenceDataResponse"):
                        sec_arr = msg.getElement("securityData")
                        for i in range(sec_arr.numValues()):
                            sec    = sec_arr.getValue(i)
                            ticker = sec.getElementAsString("security")
                            fd     = sec.getElement("fieldData")
                            data: Dict[str, object] = {}
                            for f in fields:
                                try:
                                    data[f] = fd.getElementAsFloat(f)
                                except Exception:
                                    try:
                                        data[f] = fd.getElementAsString(f)
                                    except Exception:
                                        data[f] = None
                            label = _ALL_REV.get(ticker, ticker)
                            results[label] = data
                if event.eventType() in (blp.Event.RESPONSE, blp.Event.TIMEOUT):
                    break

            return results
        except Exception as exc:
            log.error(f"[BLP] BDP error: {exc}")
            return {}

    def get_live_quotes(self) -> Dict[str, dict]:
        """
        Return the latest prices for all market indices + key macro instruments.

        Priority:
          1. Cached real-time subscription data
          2. BDP snapshot request fallback
        """
        # Return subscription cache if available and fresh (< 60 s old)
        with self._lock:
            if self._live_quotes:
                oldest = min(
                    (datetime.fromisoformat(v["timestamp"])
                     for v in self._live_quotes.values()
                     if v.get("timestamp")),
                    default=datetime.min,
                )
                if (datetime.now() - oldest).total_seconds() < 60:
                    return dict(self._live_quotes)

        # Fall back to a BDP snapshot
        if not self.is_connected():
            return {}

        tickers = list(MARKET_TICKERS.values()) + list(MACRO_TICKERS.values())[:4]
        raw = self._bdp(tickers, ["PX_LAST", "CHG_PCT_1D", "VOLUME", "NAME"])

        quotes: Dict[str, dict] = {}
        for label, data in raw.items():
            quotes[label] = {
                "ticker":     _ALL_REV.get(label, label),
                "name":       data.get("NAME", label),
                "price":      data.get("PX_LAST"),
                "change_pct": data.get("CHG_PCT_1D"),
                "volume":     data.get("VOLUME"),
                "timestamp":  datetime.now().isoformat(),
                "source":     "bloomberg_bdp",
            }
        return quotes

    # ── Real-time subscription ------------------------------------------------

    def start_subscription(self):
        """
        Start a background thread that subscribes to real-time market data.
        Updates _live_quotes whenever Bloomberg pushes a tick.
        Safe to call multiple times (no-op if already running).
        """
        if self._blpapi is None:
            return
        if self._sub_thread and self._sub_thread.is_alive():
            return
        if not self._connected:
            log.warning("[BLP] Cannot start subscription — not connected")
            return

        self._stop_sub.clear()
        self._sub_thread = threading.Thread(
            target=self._subscription_loop, daemon=True, name="blp-sub"
        )
        self._sub_thread.start()
        log.info("[BLP] Real-time subscription thread started")

    def _subscription_loop(self):
        """Background thread: open a second session for live market data."""
        blp = self._blpapi
        sub_session = None
        try:
            opts = blp.SessionOptions()
            opts.setServerHost(self.host)
            opts.setServerPort(self.port)

            sub_session = blp.Session(opts)
            if not sub_session.start():
                log.error("[BLP] Subscription session failed to start")
                return
            if not sub_session.openService("//blp/mktdata"):
                log.error("[BLP] Failed to open mktdata service")
                sub_session.stop()
                return

            self._sub_session = sub_session

            subs = blp.SubscriptionList()
            subscribe_tickers = (
                list(MARKET_TICKERS.values()) +
                list(MACRO_TICKERS.values())[:4]   # VIX, DXY, Oil, Gold
            )
            for ticker in subscribe_tickers:
                corr_id = blp.CorrelationId(ticker)
                subs.add(ticker, _LIVE_FIELDS, [], corr_id)

            sub_session.subscribe(subs)
            log.info(f"[BLP] Subscribed to {len(subscribe_tickers)} tickers")

            while not self._stop_sub.is_set():
                event = sub_session.nextEvent(1_000)  # 1-second poll

                if event.eventType() == blp.Event.SUBSCRIPTION_DATA:
                    for msg in event:
                        self._process_tick(msg)

                elif event.eventType() == blp.Event.SUBSCRIPTION_STATUS:
                    for msg in event:
                        log.debug(f"[BLP] Sub status: {msg.messageType()}")

        except Exception as exc:
            log.error(f"[BLP] Subscription loop error: {exc}")
        finally:
            if sub_session:
                try:
                    sub_session.stop()
                except Exception:
                    pass
            self._sub_session = None
            log.info("[BLP] Subscription loop ended")

    def _process_tick(self, msg):
        """Parse one subscription tick and update _live_quotes."""
        blp = self._blpapi
        try:
            ticker = str(msg.correlationId().value())
            label  = _ALL_REV.get(ticker, ticker)

            data: Dict[str, object] = {}
            for field in _LIVE_FIELDS:
                if msg.hasElement(field):
                    try:
                        data[field] = msg.getElement(field).getValueAsFloat()
                    except Exception:
                        pass

            if not data:
                return

            with self._lock:
                existing = self._live_quotes.get(label, {})
                self._live_quotes[label] = {
                    "ticker":     ticker,
                    "name":       existing.get("name", label),
                    "price":      data.get("LAST_PRICE", existing.get("price")),
                    "change_pct": data.get("CHG_PCT_1D",  existing.get("change_pct")),
                    "bid":        data.get("BID",         existing.get("bid")),
                    "ask":        data.get("ASK",         existing.get("ask")),
                    "volume":     data.get("VOLUME",      existing.get("volume")),
                    "timestamp":  datetime.now().isoformat(),
                    "source":     "bloomberg_live",
                }
        except Exception as exc:
            log.debug(f"[BLP] Tick parse error: {exc}")

    # ── Status ----------------------------------------------------------------

    def status(self) -> dict:
        """Return a JSON-serialisable status dict for the API."""
        with self._lock:
            quote_count = len(self._live_quotes)
            last_tick   = None
            if self._live_quotes:
                last_tick = max(
                    (v.get("timestamp", "") for v in self._live_quotes.values()),
                    default=None,
                )

        return {
            "blpapi_available":   self._blpapi is not None,
            "connected":          self._connected,
            "host":               self.host,
            "port":               self.port,
            "subscription_alive": bool(self._sub_thread and self._sub_thread.is_alive()),
            "live_tickers":       quote_count,
            "last_tick":          last_tick,
            "market_tickers":     MARKET_TICKERS,
            "macro_tickers":      {k: v for k, v in list(MACRO_TICKERS.items())[:6]},
        }


# ── Singleton ─────────────────────────────────────────────────────────────────

_provider: Optional[BloombergProvider] = None
_init_lock = threading.Lock()


def get_bloomberg(auto_connect: bool = True) -> BloombergProvider:
    """
    Return the global BloombergProvider instance (created once on first call).
    Automatically attempts to connect if auto_connect=True.
    """
    global _provider
    if _provider is None:
        with _init_lock:
            if _provider is None:
                _provider = BloombergProvider()
                if auto_connect:
                    connected = _provider.connect()
                    if connected:
                        _provider.start_subscription()
    return _provider
