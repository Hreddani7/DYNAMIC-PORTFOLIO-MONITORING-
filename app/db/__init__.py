"""Database — SQLite with portfolio upload support."""
import os, sqlite3, hashlib, uuid, json
from datetime import datetime
from app.config import UPLOAD_DIR

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "afrisk.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY, username TEXT UNIQUE, email TEXT,
            password_hash TEXT, role TEXT DEFAULT 'retail_user', created_at TEXT);
        CREATE TABLE IF NOT EXISTS portfolios (
            portfolio_id TEXT PRIMARY KEY, user_id TEXT, name TEXT, description TEXT,
            source TEXT DEFAULT 'manual', file_name TEXT, created_at TEXT, updated_at TEXT);
        CREATE TABLE IF NOT EXISTS holdings (
            portfolio_id TEXT, asset_id TEXT, market TEXT, sector TEXT,
            weight REAL, quantity REAL, price REAL, market_value REAL,
            currency TEXT, as_of_date TEXT, PRIMARY KEY (portfolio_id, asset_id));
    """)
    for uid, un, pw, role in [
        ("admin-001", "admin", "admin123", "admin"),
        ("inst-001", "institution", "inst123", "institutional_user"),
        ("ret-001", "retail", "retail123", "retail_user"),
    ]:
        try:
            conn.execute("INSERT INTO users VALUES (?,?,?,?,?,?)",
                         (uid, un, f"{un}@afrisk.io", hashlib.sha256(pw.encode()).hexdigest(), role, datetime.now().isoformat()))
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()


def create_portfolio(user_id, name, description="", source="manual", file_name=""):
    conn = get_db()
    pid = f"PF-{uuid.uuid4().hex[:8]}"
    now = datetime.now().isoformat()
    conn.execute("INSERT INTO portfolios VALUES (?,?,?,?,?,?,?,?)",
                 (pid, user_id, name, description, source, file_name, now, now))
    conn.commit()
    conn.close()
    return pid


def save_holdings(portfolio_id, holdings_list):
    """Save normalized holdings to DB. holdings_list = list of dicts."""
    conn = get_db()
    conn.execute("DELETE FROM holdings WHERE portfolio_id=?", (portfolio_id,))
    for h in holdings_list:
        conn.execute("INSERT OR REPLACE INTO holdings VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (portfolio_id, h.get("asset_id", ""), h.get("market", ""), h.get("sector", ""),
                      h.get("weight", 0), h.get("quantity", 0), h.get("price", 0), h.get("market_value", 0),
                      h.get("currency", ""), datetime.now().date().isoformat()))
    conn.commit()
    conn.close()


def get_holdings(portfolio_id):
    conn = get_db()
    rows = conn.execute("SELECT * FROM holdings WHERE portfolio_id=? ORDER BY weight DESC", (portfolio_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_portfolios(user_id, role="retail_user"):
    conn = get_db()
    if role == "admin":
        rows = conn.execute("SELECT * FROM portfolios ORDER BY created_at DESC").fetchall()
    else:
        rows = conn.execute("SELECT * FROM portfolios WHERE user_id=? ORDER BY created_at DESC", (user_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def clear_all_portfolios():
    """Clear all portfolios and holdings — users start fresh each session."""
    conn = get_db()
    conn.execute("DELETE FROM holdings")
    conn.execute("DELETE FROM portfolios")
    conn.commit()
    conn.close()


def create_sample_portfolio(user_id):
    """Create default JSE portfolio."""
    pid = create_portfolio(user_id, "JSE Sample Portfolio", "Default JSE Top 40 sample", "sample")
    holdings = [
        {"asset_id": "NPN.JSE",  "market": "JSE_SA", "sector": "Technology", "weight": 0.15, "quantity": 500,  "price": 3200, "market_value": 1600000, "currency": "ZAR"},
        {"asset_id": "SBK.JSE",  "market": "JSE_SA", "sector": "Banking",    "weight": 0.12, "quantity": 2000, "price": 180,  "market_value": 360000,  "currency": "ZAR"},
        {"asset_id": "AGL.JSE",  "market": "JSE_SA", "sector": "Mining",     "weight": 0.12, "quantity": 500,  "price": 850,  "market_value": 425000,  "currency": "ZAR"},
        {"asset_id": "BHP.JSE",  "market": "JSE_SA", "sector": "Mining",     "weight": 0.10, "quantity": 100,  "price": 4500, "market_value": 450000,  "currency": "ZAR"},
        {"asset_id": "FSR.JSE",  "market": "JSE_SA", "sector": "Banking",    "weight": 0.09, "quantity": 2000, "price": 150,  "market_value": 300000,  "currency": "ZAR"},
        {"asset_id": "MTN.JSE",  "market": "JSE_SA", "sector": "Telecom",    "weight": 0.09, "quantity": 250,  "price": 1200, "market_value": 300000,  "currency": "ZAR"},
        {"asset_id": "SOL.JSE",  "market": "JSE_SA", "sector": "Energy",     "weight": 0.08, "quantity": 400,  "price": 750,  "market_value": 300000,  "currency": "ZAR"},
        {"asset_id": "AMS.JSE",  "market": "JSE_SA", "sector": "Mining",     "weight": 0.08, "quantity": 120,  "price": 2100, "market_value": 252000,  "currency": "ZAR"},
        {"asset_id": "IMP.JSE",  "market": "JSE_SA", "sector": "Mining",     "weight": 0.07, "quantity": 600,  "price": 400,  "market_value": 240000,  "currency": "ZAR"},
        {"asset_id": "GFI.JSE",  "market": "JSE_SA", "sector": "Mining",     "weight": 0.05, "quantity": 500,  "price": 320,  "market_value": 160000,  "currency": "ZAR"},
        {"asset_id": "SHP.JSE",  "market": "JSE_SA", "sector": "Retail",     "weight": 0.05, "quantity": 1000, "price": 180,  "market_value": 180000,  "currency": "ZAR"},
    ]
    save_holdings(pid, holdings)
    return pid
