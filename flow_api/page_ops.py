"""In-page primitives: run captcha + fetch INSIDE the logged-in labs.google page via
Patchright page.evaluate, and sniff the auth bearer off the page's own traffic.

These are the only pieces that touch the live browser. Everything else is pure Python.
All async (matches the worker's async Patchright usage).
"""
import asyncio
import json
import logging
import time

from . import config

logger = logging.getLogger(__name__)


class TokenStore:
    """Holds the latest ya29 bearer sniffed off the page's requests."""

    def __init__(self):
        self.token = ""
        self.captured_at = 0.0

    def set(self, token: str):
        self.token = token
        self.captured_at = time.time()

    @property
    def age_s(self) -> float:
        return time.time() - self.captured_at if self.captured_at else 1e9


def install_token_capture(page) -> TokenStore:
    """Listen to the page's outgoing requests; keep the latest 'Bearer ya29.*' token.

    Mirrors FlowKit's webRequest sniff. The logged-in Flow page emits these on its own
    aisandbox/labs calls, so the token appears without us doing anything DOM-ish.
    """
    store = TokenStore()

    def _on_request(req):
        try:
            auth = (req.headers or {}).get("authorization", "")
            if auth.startswith("Bearer ya29."):
                tok = auth[len("Bearer "):].strip()
                if tok and tok != store.token:
                    store.set(tok)
                    logger.info("flow_api: captured fresh bearer (len=%d)", len(tok))
        except Exception:
            pass

    page.on("request", _on_request)
    return store


async def wait_for_token(store: TokenStore, page=None, timeout: float = 30.0) -> str:
    """Wait until a bearer has been sniffed. Optionally nudge the page to emit one
    by reloading project data is the caller's job; here we just poll the store."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if store.token and store.age_s < 3000:  # ~50 min freshness guard
            return store.token
        await asyncio.sleep(0.5)
    return store.token  # may be '' — caller treats empty as failure


_CAPTCHA_JS = """
async ([siteKey, action]) => {
  function waitG(t) {
    return new Promise((res, rej) => {
      const s = Date.now();
      const c = () => {
        if (window.grecaptcha && window.grecaptcha.enterprise && window.grecaptcha.enterprise.execute) return res();
        if (Date.now() - s > t) return rej(new Error('grecaptcha not available'));
        setTimeout(c, 200);
      };
      c();
    });
  }
  await waitG(10000);
  return await window.grecaptcha.enterprise.execute(siteKey, { action });
}
"""


async def mint_captcha(page, action: str) -> str:
    """Mint a reCAPTCHA Enterprise token in the page's MAIN world. Free."""
    return await page.evaluate(_CAPTCHA_JS, [config.RECAPTCHA_SITE_KEY, action])


_FETCH_JS = """
async ([url, method, headers, bodyStr]) => {
  const opts = { method, headers, credentials: 'include' };
  if (bodyStr !== null) opts.body = bodyStr;
  let status = 0, ok = false, text = '';
  try {
    const r = await fetch(url, opts);
    status = r.status; ok = r.ok;
    text = await r.text();
  } catch (e) {
    return { status: 0, ok: false, data: null, text: 'fetch failed: ' + (e && e.message) };
  }
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch (e) { data = null; }
  return { status, ok, data, text: data ? '' : (text || '').slice(0, 2000) };
}
"""


async def api_fetch(page, url: str, method: str, token: str,
                    body_obj=None, extra_headers: dict = None) -> dict:
    """Run a fetch to the Flow API from inside the page (correct origin/cookies).

    Returns {status, ok, data, text}. Never raises — network errors come back as
    status 0 with a text reason.
    """
    headers = {"authorization": f"Bearer {token}"}
    if body_obj is not None:
        headers["content-type"] = "application/json"
    if extra_headers:
        headers.update(extra_headers)
    body_str = json.dumps(body_obj) if body_obj is not None else None
    try:
        return await page.evaluate(_FETCH_JS, [url, method, headers, body_str])
    except Exception as e:
        return {"status": 0, "ok": False, "data": None, "text": f"evaluate failed: {e}"}
