"""
SAHAR Conseil — Module de sécurité partagé
Fonctions de sécurité réutilisables pour toutes les apps Streamlit.
"""

import hashlib
import hmac
import time
import re
import logging
from functools import wraps

import streamlit as st

logger = logging.getLogger("sahar.security")


# ─── RATE LIMITING ───────────────────────────────────────────────────────────

def init_rate_limiter():
    """Initialize rate limiter in session state."""
    if "_rate_limiter" not in st.session_state:
        st.session_state["_rate_limiter"] = {
            "requests": [],
            "blocked_until": 0,
            "failed_auth": 0,
        }


def check_rate_limit(
    max_requests: int = 30,
    window_seconds: int = 60,
    block_duration: int = 120,
) -> bool:
    """
    Returns True if rate limit exceeded.
    Blocks session for `block_duration` seconds after exceeding limit.
    """
    init_rate_limiter()
    now = time.time()
    rl = st.session_state["_rate_limiter"]

    if now < rl["blocked_until"]:
        remaining = int(rl["blocked_until"] - now)
        st.error(f"⏳ Trop de requêtes. Réessayez dans {remaining}s.")
        return True

    rl["requests"] = [t for t in rl["requests"] if now - t < window_seconds]

    if len(rl["requests"]) >= max_requests:
        rl["blocked_until"] = now + block_duration
        logger.warning("Rate limit exceeded for session")
        st.error("⏳ Trop de requêtes. Veuillez patienter 2 minutes.")
        return True

    rl["requests"].append(now)
    return False


# ─── AUTH BRUTE-FORCE PROTECTION ─────────────────────────────────────────────

def check_auth_rate_limit(max_attempts: int = 5, lockout_seconds: int = 300) -> bool:
    """
    Returns True if too many failed auth attempts.
    Locks out for `lockout_seconds` after `max_attempts` failures.
    """
    init_rate_limiter()
    rl = st.session_state["_rate_limiter"]

    if rl.get("auth_locked_until", 0) > time.time():
        remaining = int(rl["auth_locked_until"] - time.time())
        st.error(f"🔒 Compte temporairement verrouillé. Réessayez dans {remaining}s.")
        return True

    if rl.get("failed_auth", 0) >= max_attempts:
        rl["auth_locked_until"] = time.time() + lockout_seconds
        rl["failed_auth"] = 0
        logger.warning("Auth lockout triggered")
        st.error("🔒 Trop de tentatives. Compte verrouillé 5 minutes.")
        return True

    return False


def record_failed_auth():
    """Record a failed authentication attempt."""
    init_rate_limiter()
    st.session_state["_rate_limiter"]["failed_auth"] = (
        st.session_state["_rate_limiter"].get("failed_auth", 0) + 1
    )


def reset_auth_failures():
    """Reset failed auth counter on success."""
    init_rate_limiter()
    st.session_state["_rate_limiter"]["failed_auth"] = 0


# ─── INPUT SANITIZATION ─────────────────────────────────────────────────────

def sanitize(text: str, max_length: int = 200) -> str:
    """
    Sanitize user text input.
    Strips XSS vectors, SQL injection patterns, and limits length.
    """
    if not isinstance(text, str):
        return ""
    text = text.strip()[:max_length]
    # Remove HTML/script tags
    text = re.sub(r'<[^>]*>', '', text)
    # Remove dangerous characters
    text = re.sub(r'["\';{}]', '', text)
    # Remove javascript: and event handlers
    text = re.sub(r'(javascript|on\w+)\s*[:=]', '', text, flags=re.IGNORECASE)
    # Remove SQL injection patterns
    text = re.sub(
        r'\b(UNION|SELECT|INSERT|UPDATE|DELETE|DROP|ALTER|EXEC|EXECUTE)\b',
        '',
        text,
        flags=re.IGNORECASE,
    )
    return text.strip()


def sanitize_numeric(value, min_val=None, max_val=None, default=0):
    """Sanitize numeric input with bounds checking."""
    try:
        val = float(value)
        if min_val is not None:
            val = max(val, min_val)
        if max_val is not None:
            val = min(val, max_val)
        return val
    except (ValueError, TypeError):
        return default


# ─── PASSWORD UTILS ──────────────────────────────────────────────────────────

def secure_compare(a: str, b: str) -> bool:
    """Timing-safe string comparison to prevent timing attacks."""
    return hmac.compare_digest(
        hashlib.sha256(a.encode("utf-8")).digest(),
        hashlib.sha256(b.encode("utf-8")).digest(),
    )


# ─── SESSION SECURITY ───────────────────────────────────────────────────────

def init_session_security():
    """Initialize security-related session state."""
    if "_session_id" not in st.session_state:
        st.session_state["_session_id"] = hashlib.sha256(
            f"{time.time()}-{id(st)}".encode()
        ).hexdigest()[:16]
    if "_session_created" not in st.session_state:
        st.session_state["_session_created"] = time.time()


def check_session_timeout(max_idle_seconds: int = 3600) -> bool:
    """Returns True if session has timed out (1h default)."""
    init_session_security()
    last_activity = st.session_state.get("_last_activity", st.session_state["_session_created"])
    if time.time() - last_activity > max_idle_seconds:
        for key in list(st.session_state.keys()):
            if key.startswith("auth"):
                del st.session_state[key]
        st.warning("⏰ Session expirée. Veuillez vous reconnecter.")
        return True
    st.session_state["_last_activity"] = time.time()
    return False


# ─── EXPORT SECURITY ────────────────────────────────────────────────────────

def safe_filename(name: str) -> str:
    """Generate a safe filename from user input."""
    name = re.sub(r'[^\w\s\-.]', '', name)
    name = re.sub(r'\s+', '_', name)
    return name[:100]
