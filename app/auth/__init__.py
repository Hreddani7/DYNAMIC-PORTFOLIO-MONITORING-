"""
Authentication & RBAC — JWT tokens, password hashing, Flask middleware.
"""
import hashlib
from datetime import datetime, timedelta
from functools import wraps

import jwt as pyjwt
from flask import request, jsonify

from app.config import SECRET_KEY, JWT_ALGORITHM, JWT_EXPIRY_HOURS
from app.db import get_db


def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


def authenticate_user(username, password):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    conn.close()
    if not user:
        return None
    if user["password_hash"] != hash_password(password):
        return None
    return dict(user)


def create_token(user):
    return pyjwt.encode(
        {
            "sub": user["username"],
            "role": user["role"],
            "user_id": user["user_id"],
            "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRY_HOURS),
        },
        SECRET_KEY,
        algorithm=JWT_ALGORITHM,
    )


def token_required(f):
    """Flask decorator — requires valid JWT in Authorization header."""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not token:
            return jsonify({"error": "Authentication required"}), 401
        try:
            request.user = pyjwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
        except pyjwt.ExpiredSignatureError:
            return jsonify({"error": "Token expired"}), 401
        except Exception:
            return jsonify({"error": "Invalid token"}), 401
        return f(*args, **kwargs)
    return decorated


def require_role(*roles):
    """Flask decorator — requires user to have one of the specified roles."""
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            token = request.headers.get("Authorization", "").replace("Bearer ", "")
            if not token:
                return jsonify({"error": "Authentication required"}), 401
            try:
                user = pyjwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
                request.user = user
            except Exception:
                return jsonify({"error": "Invalid token"}), 401
            if user.get("role") not in roles and "admin" not in roles:
                if user.get("role") != "admin":
                    return jsonify({"error": f"Requires role: {', '.join(roles)}"}), 403
            return f(*args, **kwargs)
        return decorated
    return decorator
