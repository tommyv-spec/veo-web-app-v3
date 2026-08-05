"""In-page primitives (sync Patchright): run captcha + fetch INSIDE the logged-in
labs.google page, and sniff the auth bearer off the page's own traffic.

Sync flavor — both image_worker.py and flow_worker.py use sync_playwright. These are
the only pieces that touch the live browser; everything else is pure Python.
"""
import logging
import json
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


def wait_for_token(store: TokenStore, page=None, timeout: float = 30.0) -> str:
    """Block until a bearer has been sniffed."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if store.token and store.age_s < 3000:  # ~50 min freshness guard
            return store.token
        time.sleep(0.5)
    return store.token  # may be '' — caller treats empty as failure


# v896 — page.evaluate does NOT reach the main world under Patchright: it runs
# in an ISOLATED execution context (that is how automation stays hidden), and
# window.grecaptcha lives in the main world. Probed live 2026-08-05 on the
# operator's Flow session: page.evaluate saw `typeof grecaptcha = undefined`
# while a <script> tag saw `object`, with two recaptcha scripts already loaded
# by Flow. The old code here could therefore only ever fail — it just threw
# 'grecaptcha not available', which callers then misread as an account block.
#
# A <script> tag runs in the main world and the DOM is shared between worlds, so
# the mint runs inside an injected script and returns its token through a DOM
# attribute. Same fix that image_worker.py carries (verified there: 4/4 tokens
# in 0.9s, then a full node rendered and uploaded).
_CAPTCHA_JS = """
async ([siteKey, action]) => {
  const id = 'kv-mint-' + Math.random().toString(36).slice(2);
  const holder = document.createElement('div');
  holder.id = id;
  holder.style.display = 'none';
  (document.body || document.documentElement).appendChild(holder);

  const mainWorld = `
    (async () => {
      const el = document.getElementById(${JSON.stringify(id)});
      if (!el) return;
      const KEY = ${JSON.stringify(siteKey)};
      const ACTION = ${JSON.stringify(action)};
      const done = (attr, val) => { el.setAttribute(attr, val); el.setAttribute('data-done', '1'); };
      const ready = () => !!(window.grecaptcha && window.grecaptcha.enterprise
                             && window.grecaptcha.enterprise.execute);
      const waitFor = async (ms) => {
        const s = Date.now();
        while (Date.now() - s < ms) {
          if (ready()) return true;
          await new Promise(r => setTimeout(r, 200));
        }
        return ready();
      };
      try {
        if (!await waitFor(5000)) {
          try {
            await new Promise((res, rej) => {
              const s = document.createElement('script');
              s.src = 'https://www.google.com/recaptcha/enterprise.js?render=' + encodeURIComponent(KEY);
              s.async = true;
              s.onload = () => res();
              s.onerror = () => rej(new Error('enterprise.js failed to load (blocked?)'));
              (document.head || document.documentElement).appendChild(s);
            });
          } catch (e) {
            return done('data-err', 'grecaptcha not available in main world: '
                                    + ((e && e.message) || 'load failed'));
          }
          if (!await waitFor(12000)) {
            return done('data-err', 'grecaptcha not available in main world after loading enterprise.js');
          }
        }
        await new Promise(r => { try { window.grecaptcha.enterprise.ready(r); } catch (e) { r(); } });
        const tok = await window.grecaptcha.enterprise.execute(KEY, { action: ACTION });
        done('data-token', tok || '');
      } catch (e) {
        done('data-err', 'grecaptcha execute rejected: ' + ((e && e.message) || 'unknown'));
      }
    })();
  `;

  try {
    const s = document.createElement('script');
    s.textContent = mainWorld;
    (document.head || document.documentElement).appendChild(s);
    s.remove();
  } catch (e) {
    holder.remove();
    throw new Error('main-world script injection failed: ' + ((e && e.message) || 'unknown'));
  }

  const t0 = Date.now();
  while (Date.now() - t0 < 35000) {
    if (holder.getAttribute('data-done')) break;
    await new Promise(r => setTimeout(r, 150));
  }
  const token = holder.getAttribute('data-token') || '';
  const err = holder.getAttribute('data-err')
              || (holder.getAttribute('data-done') ? '' : 'main-world mint timed out (CSP?)');
  holder.remove();
  if (!token) throw new Error(err || 'captcha mint failed');
  return token;
}
"""


def mint_captcha(page, action: str) -> str:
    """Mint a reCAPTCHA Enterprise token in the page's MAIN world. Free."""
    return page.evaluate(_CAPTCHA_JS, [config.RECAPTCHA_SITE_KEY, action])


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


def api_fetch(page, url: str, method: str, token: str,
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
        return page.evaluate(_FETCH_JS, [url, method, headers, body_str])
    except Exception as e:
        return {"status": 0, "ok": False, "data": None, "text": f"evaluate failed: {e}"}
