#!/usr/bin/env python3
"""AfriSK — African Structural Risk Intelligence Platform"""
import os

# Load .env file for secrets (HF_TOKEN, etc.)
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

from flask import Flask, send_from_directory
from app.db import init_db, clear_all_portfolios
from app.ingestion import generate_prices, generate_macro
from app.api import api as api_blueprint

static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
app = Flask(__name__, static_folder=static_dir)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB upload limit

@app.after_request
def cors(r):
    r.headers["Access-Control-Allow-Origin"] = "*"
    r.headers["Access-Control-Allow-Headers"] = "*"
    r.headers["Access-Control-Allow-Methods"] = "*"
    return r

app.register_blueprint(api_blueprint)

@app.route("/")
def index(): return send_from_directory(static_dir, "index.html")

@app.route("/<path:path>")
def static_files(path): return send_from_directory(static_dir, path)

if __name__ == "__main__":
    print("=" * 55)
    print("  AfriSK — Risk Intelligence Engine")
    print("=" * 55)
    init_db()
    clear_all_portfolios()  # Start fresh — no pre-existing portfolios
    p = generate_prices(); m = generate_macro()
    print(f"[DATA] {len(p)} days × {len(p.columns)} markets + {len(m.columns)} macro")
    print(f"[API]  /api/v1 ready (upload, compute, chat)")
    print("-" * 55)
    print("  http://localhost:5000")
    print("  admin/admin123 (or create account)")
    print("=" * 55)
    app.run(host="0.0.0.0", port=5000, debug=False)
