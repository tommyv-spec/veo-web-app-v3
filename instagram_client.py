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
    """Returns recent videos for the given user across BOTH the reels chunk
    AND the general media chunk (deduped by shortcode), paginating cursors
    until exhausted OR limit/max_pages reached.

    limit=0 (default) means "all videos, no cap".

    Each dict: shortcode, url, thumb_url, video_url, caption, views, likes, comments, posted_at.
    """
    seen = set()
    out: List[Dict] = []

    def _ingest(items):
        for it in items:
            if not _looks_like_video(it):
                continue
            sc = it.get("code") or it.get("shortcode")
            if not sc or sc in seen:
                continue
            seen.add(sc)
            out.append(_clip_to_dict(it))

    # Pass A — v1 reels chunk (cheap, returns ~12 most recent).
    # NOTE: HikerAPI v1 chunk endpoints do NOT actually paginate — each call
    # returns the same first page regardless of cursor. We hit it ONCE for
    # the recent items, then fall through to v2 (which supports proper
    # cursor pagination) for historical reach.
    try:
        data = _get("/v1/user/clips/chunk", {"user_id": ig_user_id}, api_key)
        items, _cur = _extract_items_and_cursor(data)
        before = len(out)
        _ingest(items)
        added = len(out) - before
        print(f"[ig-client] v1 clips/chunk items={len(items)} new={added}", flush=True)
    except HikerAPIError as e:
        print(f"[ig-client] v1 clips/chunk failed: {e}", flush=True)
    if limit and len(out) >= limit:
        return out[:limit]

    # Pass A-2 — v2 clips with real pagination via page_id.
    pages = 0
    cursor = None
    prev_cursor = None
    while pages < max_pages:
        params = {"user_id": ig_user_id}
        if cursor:
            params["page_id"] = cursor
        try:
            data = _get("/v2/user/clips", params, api_key)
        except HikerAPIError as e:
            print(f"[ig-client] v2 clips page {pages} failed: {e}", flush=True)
            break
        if pages == 0:
            try:
                if isinstance(data, dict):
                    shape = sorted(list(data.keys()))[:20]
                elif isinstance(data, list):
                    shape = ["LIST", f"len={len(data)}", f"[0]={'list' if data and isinstance(data[0], list) else type(data[0]).__name__ if data else 'empty'}", f"[1]={type(data[1]).__name__ if len(data) > 1 else 'absent'}"]
                else:
                    shape = [type(data).__name__]
                print(f"[ig-client] FIRST RESPONSE SHAPE v2 clips: {shape}", flush=True)
            except Exception:
                pass
        items, next_cursor = _extract_items_and_cursor(data)
        before = len(out)
        _ingest(items)
        added = len(out) - before
        print(f"[ig-client] v2 clips page={pages} items={len(items)} new={added} cursor={'yes' if next_cursor else 'no'}", flush=True)
        pages += 1
        if limit and len(out) >= limit:
            return out[:limit]
        if not next_cursor or not items:
            break
        # Stall only on cursor-repeat (true loop). new=0 can be valid: v1
        # already grabbed the recent batch so v2 page 0 dupes — but the
        # cursor still advances to the next chunk of older items.
        if next_cursor == cursor:
            print(f"[ig-client] v2 clips pagination stalled (cursor unchanged) — stopping", flush=True)
            break
        prev_cursor = cursor
        cursor = next_cursor

    # Pass B-1 — v1 medias chunk (recent feed, also non-reel video posts).
    try:
        data = _get("/v1/user/medias/chunk", {"user_id": ig_user_id}, api_key)
        items, _cur = _extract_items_and_cursor(data)
        before = len(out)
        _ingest(items)
        added = len(out) - before
        print(f"[ig-client] v1 medias/chunk items={len(items)} new={added}", flush=True)
    except HikerAPIError as e:
        print(f"[ig-client] v1 medias/chunk failed: {e}", flush=True)
    if limit and len(out) >= limit:
        return out[:limit]

    # Pass B-2 — v2 medias with page_id pagination for historical reach.
    pages = 0
    cursor = None
    prev_cursor = None
    while pages < max_pages:
        params = {"user_id": ig_user_id}
        if cursor:
            params["page_id"] = cursor
        try:
            data = _get("/v2/user/medias", params, api_key)
        except HikerAPIError as e:
            print(f"[ig-client] v2 medias page {pages} failed: {e}", flush=True)
            break
        if pages == 0:
            try:
                if isinstance(data, dict):
                    shape = sorted(list(data.keys()))[:20]
                elif isinstance(data, list):
                    shape = ["LIST", f"len={len(data)}", f"[0]={'list' if data and isinstance(data[0], list) else type(data[0]).__name__ if data else 'empty'}", f"[1]={type(data[1]).__name__ if len(data) > 1 else 'absent'}"]
                else:
                    shape = [type(data).__name__]
                print(f"[ig-client] FIRST RESPONSE SHAPE v2 medias: {shape}", flush=True)
            except Exception:
                pass
        items, next_cursor = _extract_items_and_cursor(data)
        before = len(out)
        _ingest(items)
        added = len(out) - before
        print(f"[ig-client] v2 medias page={pages} items={len(items)} new={added} cursor={'yes' if next_cursor else 'no'}", flush=True)
        pages += 1
        if limit and len(out) >= limit:
            return out[:limit]
        if not next_cursor or not items:
            break
        if next_cursor == cursor:
            print(f"[ig-client] v2 medias pagination stalled (cursor unchanged) — stopping", flush=True)
            break
        prev_cursor = cursor
        cursor = next_cursor

    return out


def _extract_items_and_cursor(data):
    """HikerAPI responses come in several shapes. Returns (items, next_cursor).

    Known shapes:
    - v1 chunk: [[items], cursor] OR {items: [...], ...}
    - v2 paginated: {next_page_id: "...", response: {items: [...]}} —
      Instagram-private-API style with items nested under `response`.
    """
    items: list = []
    cursor = None
    if isinstance(data, list):
        if len(data) > 0 and isinstance(data[0], list):
            items = data[0]
            if len(data) > 1 and data[1]:
                c = data[1]
                cursor = str(c) if isinstance(c, (str, int)) else None
        else:
            items = data
    elif isinstance(data, dict):
        # v2 endpoints wrap data inside `response`. Recurse into it for items.
        if isinstance(data.get("response"), (dict, list)):
            inner_items, _inner_cursor = _extract_items_and_cursor(data["response"])
            if inner_items:
                items = inner_items
        # Top-level item arrays under common keys.
        if not items:
            for ikey in ("items", "data", "medias", "videos", "clips"):
                v = data.get(ikey)
                if isinstance(v, list):
                    items = v
                    break
        # Cursor — try every plausible key.
        for k in ("next_page_id", "next_max_id", "next_cursor", "max_id", "end_cursor", "next_min_id", "next_page", "page_token", "next_page_token"):
            v = data.get(k)
            if v:
                cursor = str(v)
                break
        if not cursor:
            for k in data.keys():
                lk = k.lower()
                if ("cursor" in lk or "max_id" in lk or "next" in lk or "page_id" in lk) and data[k]:
                    v = data[k]
                    if isinstance(v, (str, int)):
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
