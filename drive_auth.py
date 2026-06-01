# -*- coding: utf-8 -*-
"""Google Drive OAuth (separate consent from session login).

Session-login OAuth (auth.py) uses the minimal `openid email profile` scope.
For Drive folder watching we need a second consent screen that grants
`drive.readonly` AND issues a refresh_token (so we can poll the folder long
after the user closed the tab).

To force a refresh_token from Google we must pass `access_type=offline` AND
`prompt=consent`. Without `prompt=consent`, Google omits the refresh_token
on re-consent flows for users who previously authorized the app.

Refresh tokens are stored encrypted (Fernet, reusing encryption.py) on the
DriveAccount row. Polling code (drive_client.py) decrypts + exchanges for a
fresh access_token per request.
"""
import os
from datetime import datetime
from typing import Optional

from fastapi import HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from authlib.integrations.starlette_client import OAuth

from models import User, DriveAccount

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
APP_URL = os.environ.get("APP_URL", "http://localhost:8000")

DRIVE_SCOPE = "openid email https://www.googleapis.com/auth/drive.readonly"

GOOGLE_DRIVE_AUTH_ENABLED = bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET)

_drive_oauth = OAuth()
if GOOGLE_DRIVE_AUTH_ENABLED:
    _drive_oauth.register(
        name="google_drive",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={
            "scope": DRIVE_SCOPE,
            # Refresh-token must-haves.
            "access_type": "offline",
            "prompt": "consent",
        },
    )


async def handle_drive_connect(request: Request) -> RedirectResponse:
    """Start the Drive OAuth flow. User must already have a session."""
    if not GOOGLE_DRIVE_AUTH_ENABLED:
        raise HTTPException(500, detail="Google OAuth not configured")
    redirect_uri = f"{APP_URL}/auth/google/drive/callback"
    return await _drive_oauth.google_drive.authorize_redirect(request, redirect_uri)


async def handle_drive_callback(request: Request, current_user: User, db: Session) -> DriveAccount:
    """Exchange the OAuth code, capture refresh_token, persist on DriveAccount."""
    if not GOOGLE_DRIVE_AUTH_ENABLED:
        raise HTTPException(500, detail="Google OAuth not configured")
    try:
        token = await _drive_oauth.google_drive.authorize_access_token(request)
    except Exception as exc:
        print(f"[Drive Auth] OAuth error: {exc}", flush=True)
        raise HTTPException(400, detail=f"OAuth error: {str(exc)}")

    refresh_token = token.get("refresh_token")
    if not refresh_token:
        # Google only returns refresh_token on FIRST consent OR when
        # prompt=consent is passed. Our client_kwargs include prompt=consent
        # so this should always be set; if not, surface clearly.
        raise HTTPException(
            400,
            detail=(
                "Google did not return a refresh_token. Revoke this app's access "
                "at https://myaccount.google.com/permissions and reconnect."
            ),
        )

    user_info = token.get("userinfo") or {}
    google_email = user_info.get("email") or current_user.email

    from encryption import encrypt as _encrypt
    encrypted = _encrypt(refresh_token)

    # One DriveAccount per (user, google_email). Re-connecting overwrites
    # the refresh token (e.g. operator picked a different Google account or
    # revoked + re-authorized).
    acc = (
        db.query(DriveAccount)
        .filter_by(user_id=current_user.id, google_email=google_email)
        .first()
    )
    if acc:
        acc.refresh_token_encrypted = encrypted
    else:
        acc = DriveAccount(
            user_id=current_user.id,
            google_email=google_email,
            refresh_token_encrypted=encrypted,
        )
        db.add(acc)
    db.commit()
    db.refresh(acc)
    print(f"[Drive Auth] Connected drive for user={current_user.id} email={google_email}", flush=True)
    return acc
