#!/usr/bin/env python3
"""AfriSK — African Structural Risk Intelligence Platform"""
import os
from flask import Flask, send_from_directory
from app.db import init_db
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
    print("  AfriSK — InteliRisk v4 Engine")
    print("=" * 55)
    init_db()
    p = generate_prices(); m = generate_macro()
    print(f"[DATA] {len(p)} days × {len(p.columns)} markets + {len(m.columns)} macro")
    print(f"[API]  /api/v1 ready (upload, compute, chat)")
    print("-" * 55)
    print("  http://localhost:5000")
    print("  admin/admin123 | institution/inst123 | retail/retail123")
    print("=" * 55)
    app.run(host="0.0.0.0", port=5000, debug=False)
