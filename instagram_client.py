"""Thin wrapper around HikerAPI for resolving handles + listing recent clips.

Distilled from the reference script
`C:\\Users\\tomma\\Downloads\\instagram_top_videos_v6_0.py` — kept to the
two endpoints we actually use. Single retry on 429 with 5s backoff.
"""
import time
from datetime import datetime, timezone
from typing import List, Dict, Optional
import requests

HIKER_BASE = "https://api.hikerapi.com"


class HikerAPIError(Exception):
    pass


def _get(path: str, params: dict, api_key: str, _retry: int = 1) -> dict:
    headers = {"x-access-key": api_key, "accept": "application/json"}
    resp = requests.get(f"{HIKER_BASE}{path}", headers=headers, params=params, timeout=20)
    if resp.status_code == 429 and _retry > 0:
        time.sleep(5)
        return _get(path, params, api_key, _retry=_retry - 1)
    if resp.status_code == 401:
        raise HikerAPIError("HikerAPI 401 — invalid key")
    if resp.status_code >= 400:
        raise HikerAPIError(f"HikerAPI {resp.status_code}: {resp.text[:200]}")
    return resp.json()


def resolve_user_id(handle: str, api_key: str) -> str:
    """Returns the numeric IG user_id for a given handle. 1 API call."""
    data = _get("/v1/user/by/username", {"username": handle}, api_key)
    pk = data.get("pk") or data.get("id") or data.get("user", {}).get("pk")
    if not pk:
        raise HikerAPIError(f"HikerAPI response missing pk: {data}")
    return str(pk)


def fetch_recent_clips(ig_user_id: str, api_key: str, limit: int = 0, max_pages: int = 50) -> List[Dict]:
    """Returns recent reels for the given user, paginating chunks until
    cursor exhausts OR limit reached OR max_pages hit (safety stop).

    limit=0 (default) means "all reels, no cap". max_pages caps the safety
    bound at ~50 pages (1000+ reels), preventing runaway loops on bugged
    pagination.

    Each dict: shortcode, url, thumb_url, video_url, caption, views, likes, comments, posted_at.
    """
    out: List[Dict] = []
    cursor = None
    pages = 0
    while pages < max_pages:
        params = {"user_id": ig_user_id, "max_amount": 50}
        if cursor:
            params["max_id"] = cursor
        data = _get("/v1/user/clips/chunk", params, api_key)
        items, next_cursor = _extract_items_and_cursor(data)
        for it in items:
            if _looks_like_video(it):
                out.append(_clip_to_dict(it))
                if limit and len(out) >= limit:
                    return out[:limit]
        pages += 1
        if not next_cursor or not items:
            break
        cursor = next_cursor
    return out


def _extract_items_and_cursor(data):
    """HikerAPI chunk responses come in a few shapes. Returns (items, next_cursor)."""
    items: list = []
    cursor = None
    if isinstance(data, list):
        # [items, cursor] form
        if len(data) > 0 and isinstance(data[0], list):
            items = data[0]
            if len(data) > 1 and isinstance(data[1], (str, int)):
                cursor = str(data[1]) or None
        else:
            items = data
    elif isinstance(data, dict):
        if isinstance(data.get("items"), list):
            items = data["items"]
        elif isinstance(data.get("data"), list):
            items = data["data"]
        # Cursor candidates across HikerAPI variants.
        for k in ("next_max_id", "next_cursor", "max_id", "next_min_id", "end_cursor"):
            v = data.get(k)
            if v:
                cursor = str(v)
                break
    return items, cursor


def _extract_items(data) -> list:
    """Back-compat wrapper for code paths that only need items."""
    items, _ = _extract_items_and_cursor(data)
    return items


def _looks_like_video(media: dict) -> bool:
    return media.get("media_type") == 2 or "video_versions" in media or media.get("video_url")


def _clip_to_dict(m: dict) -> dict:
    shortcode = m.get("code") or m.get("shortcode") or ""
    caption_obj = m.get("caption") or m.get("caption_text") or {}
    if isinstance(caption_obj, dict):
        caption = caption_obj.get("text") or ""
    else:
        caption = caption_obj or ""
    thumb_url = None
    iv = m.get("image_versions2") or {}
    if isinstance(iv, dict):
        candidates = iv.get("candidates") or []
        if candidates:
            thumb_url = candidates[0].get("url")
    if not thumb_url:
        thumb_url = m.get("thumbnail_url")
    # Direct video URL — HikerAPI returns this so we don't need yt-dlp.
    video_url = m.get("video_url")
    if not video_url:
        vv = m.get("video_versions") or []
        if isinstance(vv, list) and vv:
            video_url = vv[0].get("url") if isinstance(vv[0], dict) else None
    ts = m.get("taken_at") or m.get("posted_at")
    posted_at = None
    if isinstance(ts, (int, float)):
        posted_at = datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
    return {
        "shortcode": shortcode,
        "url": f"https://www.instagram.com/reel/{shortcode}/" if shortcode else None,
        "thumb_url": thumb_url,
        "video_url": video_url,
        "caption": caption[:1000],
        "views": int(m.get("play_count") or m.get("video_view_count") or 0),
        "likes": int(m.get("like_count") or 0),
        "comments": int(m.get("comment_count") or 0),
        "posted_at": posted_at,
    }
