# -*- coding: utf-8 -*-
"""
Authentication module for Veo Studio
Handles Google OAuth 2.0 authentication and session management
"""

import os
import uuid
import secrets
from datetime import datetime, timedelta
from typing import Optional
from functools import wraps

from fastapi import HTTPException, Request, Depends
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from authlib.integrations.starlette_client import OAuth

from models import User, UserSession, get_db_session

# =============================================================================
# Configuration
# =============================================================================

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
SESSION_SECRET = os.environ.get("SESSION_SECRET", "")
if not SESSION_SECRET:
    # CRITICAL: Without a fixed SESSION_SECRET, every server restart generates a new
    # secret which invalidates all OAuth state cookies → CSRF mismatch error on login.
    # Set SESSION_SECRET as a permanent environment variable in your deployment.
    SESSION_SECRET = secrets.token_hex(32)
    print("[Auth] WARNING: SESSION_SECRET not set — using random key. OAuth will break on server restart. Set SESSION_SECRET env var in Render.", flush=True)
APP_URL = os.environ.get("APP_URL", "http://localhost:8000")
SESSION_DURATION_DAYS = int(os.environ.get("SESSION_DURATION_DAYS", "7"))

# Check if Google OAuth is configured
GOOGLE_AUTH_ENABLED = bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET)

if GOOGLE_AUTH_ENABLED:
    print(f"[Auth] Google OAuth ENABLED")
    print(f"[Auth] Redirect URI: {APP_URL}/auth/google/callback")
else:
    print(f"[Auth] Google OAuth DISABLED (set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET)")

# =============================================================================
# OAuth Setup
# =============================================================================

oauth = OAuth()

if GOOGLE_AUTH_ENABLED:
    oauth.register(
        name='google',
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={
            'scope': 'openid email profile'
        }
    )

# =============================================================================
# Session Management
# =============================================================================

def create_session(db: Session, user: User) -> str:
    """Create a new session for a user."""
    # Clean up old sessions for this user
    db.query(UserSession).filter(
        UserSession.user_id == user.id,
        UserSession.expires_at < datetime.utcnow()
    ).delete()
    
    # Create new session
    session_token = secrets.token_urlsafe(48)
    session = UserSession(
        id=session_token,
        user_id=user.id,
        expires_at=datetime.utcnow() + timedelta(days=SESSION_DURATION_DAYS)
    )
    db.add(session)
    db.commit()
    
    return session_token


def validate_session(db: Session, session_token: str) -> Optional[User]:
    """Validate a session token and return the user if valid."""
    if not session_token:
        return None
    
    session = db.query(UserSession).filter(
        UserSession.id == session_token,
        UserSession.expires_at > datetime.utcnow()
    ).first()
    
    if not session:
        return None
    
    return session.user


def delete_session(db: Session, session_token: str):
    """Delete a session."""
    db.query(UserSession).filter(UserSession.id == session_token).delete()
    db.commit()


def cleanup_expired_sessions(db: Session):
    """Remove all expired sessions."""
    db.query(UserSession).filter(
        UserSession.expires_at < datetime.utcnow()
    ).delete()
    db.commit()


def validate_bearer_token(request: Request, db: Session):
    """v886: personal API token path — accepts an active UserWorkerToken as
    'Authorization: Bearer <token>' on ANY session-protected endpoint, so CLI
    clients (send_to_platform.py) can call import/promote/jobs without a
    browser cookie. Returns the token's User, None when no bearer header is
    present, and raises 401/403 when a bearer header is present but invalid
    (explicit failure beats silently falling back to the cookie path).
    """
    header = request.headers.get("authorization", "")
    if not header.startswith("Bearer "):
        return None
    token_value = header[7:].strip()
    from models import UserWorkerToken
    token = db.query(UserWorkerToken).filter(
        UserWorkerToken.id == token_value,
        UserWorkerToken.is_active == True
    ).first()
    if not token:
        raise HTTPException(status_code=401, detail="Invalid or revoked API token")
    user = token.user
    if not user or not user.is_active:
        raise HTTPException(status_code=403, detail="Account disabled")
    return user


# =============================================================================
# User Management
# =============================================================================

def get_or_create_user(db: Session, google_user: dict) -> User:
    """Get or create a user from Google OAuth data."""
    google_id = google_user.get('sub')
    email = google_user.get('email')
    
    # Try to find by Google ID first
    user = db.query(User).filter(User.google_id == google_id).first()
    
    # If not found, try by email
    if not user:
        user = db.query(User).filter(User.email == email).first()
        if user:
            # Link Google ID to existing email account
            user.google_id = google_id
    
    # If still not found, create new user
    if not user:
        user = User(
            id=str(uuid.uuid4()),
            email=email,
            name=google_user.get('name'),
            picture=google_user.get('picture'),
            google_id=google_id,
        )
        db.add(user)
    else:
        # Update user info
        user.name = google_user.get('name', user.name)
        user.picture = google_user.get('picture', user.picture)
    
    user.last_login = datetime.utcnow()
    db.commit()
    db.refresh(user)
    
    return user


# =============================================================================
# FastAPI Dependencies
# =============================================================================

async def get_current_user(
    request: Request,
    db: Session = Depends(get_db_session)
) -> User:
    """
    FastAPI dependency to get the current authenticated user.
    Raises 401 if not authenticated.
    """
    if not GOOGLE_AUTH_ENABLED:
        # If auth is disabled, return a default user
        return _get_or_create_default_user(db)

    # v886: CLI bearer-token path (send_to_platform.py) — checked before the
    # cookie so API clients never need a browser session.
    token_user = validate_bearer_token(request, db)
    if token_user:
        return token_user

    session_token = request.cookies.get("session")
    if not session_token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    user = validate_session(db, session_token)
    if not user:
        raise HTTPException(status_code=401, detail="Session expired")
    
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account disabled")
    
    return user


async def get_optional_user(
    request: Request,
    db: Session = Depends(get_db_session)
) -> Optional[User]:
    """
    FastAPI dependency to get the current user if authenticated.
    Returns None if not authenticated (doesn't raise error).
    """
    if not GOOGLE_AUTH_ENABLED:
        return _get_or_create_default_user(db)
    
    session_token = request.cookies.get("session")
    if not session_token:
        return None
    
    return validate_session(db, session_token)


def _get_or_create_default_user(db: Session) -> User:
    """Get or create a default user when auth is disabled."""
    default_email = "default@local"
    user = db.query(User).filter(User.email == default_email).first()
    
    if not user:
        user = User(
            id=str(uuid.uuid4()),
            email=default_email,
            name="Default User",
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    
    return user


# =============================================================================
# OAuth Handlers (to be called from main.py routes)
# =============================================================================

async def handle_google_login(request: Request) -> RedirectResponse:
    """Initiate Google OAuth flow."""
    if not GOOGLE_AUTH_ENABLED:
        raise HTTPException(status_code=500, detail="Google OAuth not configured")
    
    redirect_uri = f"{APP_URL}/auth/google/callback"
    return await oauth.google.authorize_redirect(request, redirect_uri)


async def handle_google_callback(request: Request, db: Session) -> tuple[User, str]:
    """
    Handle Google OAuth callback.
    Returns (user, session_token) tuple.
    """
    if not GOOGLE_AUTH_ENABLED:
        raise HTTPException(status_code=500, detail="Google OAuth not configured")
    
    try:
        token = await oauth.google.authorize_access_token(request)
    except Exception as e:
        print(f"[Auth] OAuth error: {e}", flush=True)
        raise HTTPException(status_code=400, detail=f"OAuth error: {str(e)}")
    
    # Get user info from token
    user_info = token.get('userinfo')
    if not user_info:
        raise HTTPException(status_code=400, detail="Failed to get user info from Google")
    
    # Get or create user
    user = get_or_create_user(db, user_info)
    
    # Create session
    session_token = create_session(db, user)
    
    print(f"[Auth] User logged in: {user.email}", flush=True)
    
    return user, session_token
