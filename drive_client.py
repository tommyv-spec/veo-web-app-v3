# -*- coding: utf-8 -*-
"""Google Drive REST v3 client — raw HTTP (no google-api-python-client).

We only need three operations:
1. Refresh OAuth access_token from a stored refresh_token.
2. List video files in a folder modified since a cutoff.
3. Download a file's bytes by file_id.

Using `requests` + Drive REST v3 directly keeps deps lean (already have
`requests` for HikerAPI) and avoids the ~30MB google-api-python-client.
"""
import os
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import requests

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")

# Drive v3 endpoints.
_TOKEN_URL = "https://oauth2.googleapis.com/token"
_DRIVE_FILES = "https://www.googleapis.com/drive/v3/files"

# In-process access-token cache. Tokens expire in 3600s; we refresh ~5min early.
# Keyed by refresh_token (since each user has a distinct refresh_token).
_ACCESS_CACHE: Dict[str, Tuple[str, float]] = {}


class DriveError(Exception):
    pass


def _get_access_token(refresh_token: str) -> str:
    """Exchange refresh_token for a fresh access_token, with in-process cache."""
    now = time.time()
    cached = _ACCESS_CACHE.get(refresh_token)
    if cached and cached[1] - now > 300:  # >5 min remaining
        return cached[0]

    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        raise DriveError("GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET not set")

    resp = requests.post(
        _TOKEN_URL,
        data={
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=15,
    )
    if resp.status_code != 200:
        raise DriveError(f"refresh_token exchange HTTP {resp.status_code}: {resp.text[:200]}")
    data = resp.json()
    access_token = data.get("access_token")
    expires_in = int(data.get("expires_in", 3600))
    if not access_token:
        raise DriveError(f"no access_token in response: {data}")
    _ACCESS_CACHE[refresh_token] = (access_token, now + expires_in)
    return access_token


def list_folder_videos(
    refresh_token: str,
    folder_id: str,
    modified_after: Optional[datetime] = None,
    page_size: int = 100,
    max_pages: int = 10,
) -> List[Dict]:
    """List video files in a Drive folder, optionally filtered by modifiedTime.

    Returns dicts with id, name, mimeType, size, modifiedTime, createdTime.
    Skips trashed files. Only includes mimeType matching the v3 query
    `mimeType contains 'video/'` (covers mp4/quicktime/webm/etc.).
    """
    access_token = _get_access_token(refresh_token)
    headers = {"Authorization": f"Bearer {access_token}"}

    q_parts = [
        f"'{folder_id}' in parents",
        "trashed = false",
        "mimeType contains 'video/'",
    ]
    if modified_after is not None:
        # Drive expects RFC 3339 UTC timestamps.
        iso = modified_after.strftime("%Y-%m-%dT%H:%M:%SZ")
        q_parts.append(f"modifiedTime > '{iso}'")
    q = " and ".join(q_parts)

    out: List[Dict] = []
    page_token: Optional[str] = None
    pages = 0
    while pages < max_pages:
        params = {
            "q": q,
            "fields": "nextPageToken, files(id, name, mimeType, size, modifiedTime, createdTime)",
            "pageSize": page_size,
            "orderBy": "modifiedTime desc",
            "spaces": "drive",
        }
        if page_token:
            params["pageToken"] = page_token
        resp = requests.get(_DRIVE_FILES, headers=headers, params=params, timeout=20)
        if resp.status_code != 200:
            raise DriveError(f"list HTTP {resp.status_code}: {resp.text[:200]}")
        data = resp.json()
        out.extend(data.get("files", []))
        page_token = data.get("nextPageToken")
        pages += 1
        if not page_token:
            break
    return out


def download_file(refresh_token: str, file_id: str, dest_path: str, max_bytes: int = 500 * 1024 * 1024) -> int:
    """Stream-download a Drive file to dest_path. Returns bytes written.

    max_bytes caps the download (default 500MB) — refuses files larger than
    that without writing to disk first. Drive folders may contain non-final
    drops; sanity-cap protects against accidental huge uploads.
    """
    access_token = _get_access_token(refresh_token)
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{_DRIVE_FILES}/{file_id}"

    with requests.get(url, headers=headers, params={"alt": "media"}, stream=True, timeout=120) as r:
        if r.status_code != 200:
            raise DriveError(f"download HTTP {r.status_code}: {r.text[:200] if hasattr(r, 'text') else ''}")
        written = 0
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 64):
                if not chunk:
                    continue
                written += len(chunk)
                if written > max_bytes:
                    raise DriveError(f"file exceeds max_bytes ({max_bytes})")
                f.write(chunk)
        return written


def list_top_level_folders(refresh_token: str, page_size: int = 200) -> List[Dict]:
    """List folders the user has access to (for picker UI).

    Returns top-level + shared folders the user owns or has access to.
    Each dict has id, name, parents (list, may be empty for root-shared).
    """
    access_token = _get_access_token(refresh_token)
    headers = {"Authorization": f"Bearer {access_token}"}
    q = "mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    params = {
        "q": q,
        "fields": "files(id, name, parents)",
        "pageSize": page_size,
        "orderBy": "modifiedTime desc",
        "spaces": "drive",
    }
    resp = requests.get(_DRIVE_FILES, headers=headers, params=params, timeout=20)
    if resp.status_code != 200:
        raise DriveError(f"folders list HTTP {resp.status_code}: {resp.text[:200]}")
    return resp.json().get("files", [])
