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


def fetch_recent_clips(ig_user_id: str, api_key: str, limit: int = 20) -> List[Dict]:
    """Returns up to `limit` recent reels for the given user. 1 API call.

    Each dict has: shortcode, url, thumb_url, caption, views, likes, comments, posted_at (datetime).
    """
    data = _get("/v1/user/clips/chunk", {"user_id": ig_user_id, "max_amount": limit}, api_key)
    items = _extract_items(data)
    return [_clip_to_dict(it) for it in items[:limit] if _looks_like_video(it)]


def _extract_items(data) -> list:
    """HikerAPI returns either [items, cursor] or {items, next_*}."""
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list):
        return data[0]
    if isinstance(data, dict):
        if isinstance(data.get("items"), list):
            return data["items"]
        if isinstance(data.get("data"), list):
            return data["data"]
    if isinstance(data, list):
        return data
    return []


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
    ts = m.get("taken_at") or m.get("posted_at")
    posted_at = None
    if isinstance(ts, (int, float)):
        posted_at = datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
    return {
        "shortcode": shortcode,
        "url": f"https://www.instagram.com/reel/{shortcode}/" if shortcode else None,
        "thumb_url": thumb_url,
        "caption": caption[:1000],
        "views": int(m.get("play_count") or m.get("video_view_count") or 0),
        "likes": int(m.get("like_count") or 0),
        "comments": int(m.get("comment_count") or 0),
        "posted_at": posted_at,
    }
