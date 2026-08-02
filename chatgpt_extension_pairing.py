"""Small, dependency-free signed tickets for the Chrome extension handoff."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import secrets
import time
from typing import Any, Dict


class InvalidPairingTicket(ValueError):
    pass


class ExpiredPairingTicket(InvalidPairingTicket):
    pass


def normalize_email(value: str) -> str:
    email = (value or "").strip().lower()
    if len(email) > 254 or not re.fullmatch(
        r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", email
    ):
        raise ValueError("Enter a valid ChatGPT account email")
    return email


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def make_ticket(secret: str, user_id: str, email: str, api_url: str) -> str:
    payload = {
        "user_id": str(user_id),
        "email": normalize_email(email),
        "api_url": api_url.rstrip("/"),
        "nonce": secrets.token_urlsafe(18),
        "iat": int(time.time()),
    }
    encoded = _b64encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signature = hmac.new(secret.encode("utf-8"), encoded.encode("ascii"), hashlib.sha256).digest()
    return f"{encoded}.{_b64encode(signature)}"


def load_ticket(secret: str, ticket: str, max_age_s: int) -> Dict[str, Any]:
    try:
        encoded, supplied_signature = ticket.split(".", 1)
        expected_signature = hmac.new(
            secret.encode("utf-8"), encoded.encode("ascii"), hashlib.sha256
        ).digest()
        if not hmac.compare_digest(_b64decode(supplied_signature), expected_signature):
            raise InvalidPairingTicket("Invalid ChatGPT extension setup ticket")
        payload = json.loads(_b64decode(encoded).decode("utf-8"))
    except InvalidPairingTicket:
        raise
    except Exception as exc:
        raise InvalidPairingTicket("Invalid ChatGPT extension setup ticket") from exc

    if not isinstance(payload, dict) or not payload.get("user_id") or not payload.get("api_url"):
        raise InvalidPairingTicket("Incomplete ChatGPT extension setup ticket")
    try:
        issued_at = int(payload["iat"])
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidPairingTicket("Incomplete ChatGPT extension setup ticket") from exc
    age = int(time.time()) - issued_at
    if age > max_age_s:
        raise ExpiredPairingTicket("ChatGPT extension setup expired")
    if age < -60:
        raise InvalidPairingTicket("Invalid ChatGPT extension setup ticket")
    try:
        payload["email"] = normalize_email(payload.get("email", ""))
    except ValueError as exc:
        raise InvalidPairingTicket(str(exc)) from exc
    return payload
