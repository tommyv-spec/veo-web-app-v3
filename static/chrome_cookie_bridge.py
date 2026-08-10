#!/usr/bin/env python3
"""Read cookies out of a Chrome-format profile at the FILE level, as Playwright
cookie dicts, so a Chrome-only Google account can seed the Firefox worker.

WHY THIS IS THE FLOW-WORKER LOGIC, NOT A NEW INVENTION
------------------------------------------------------
`worker_profile_pull.build_lean_golden_from_profile` — the pull flow_worker uses
— already solves the two hard parts, and `chatgpt_session_pull` already calls it
for the ChatGPT account:

  * the account lives in a NON-STABLE channel (Chrome Beta), so the pull may
    close that channel to release the file lock without ever touching the
    operator's daily stable Chrome (`allow_channel_close=True`);
  * Beta has App-Bound Encryption OFF, so its cookies are scheme `v10` and
    decrypt on this machine from `Local State`'s DPAPI-protected key.

The output of that pull is a lean GOLDEN — a private copy of the profile that no
browser holds open. This module reads the cookies out of that golden. So the
account information is collected exactly the way flow_worker collects it; only
the destination differs (a Firefox context instead of a Chrome launch).

WHY NOT LAUNCH THE GOLDEN AND CALL ctx.cookies()
------------------------------------------------
Measured 2026-08-03: that dies with `- [pid=45472] <gracefully close end>`. The
golden is built from `chrome-beta` while the worker launches `channel=chrome`,
and a golden opened by the wrong channel exits during startup. Reading the files
has no channel, takes no profile lock, and finishes in about a second.

Windows-only by design (that is where the workers run); returns [] elsewhere.
Never raises — every failure degrades to "no cookies" and the caller falls
through to the next login path.
"""

import base64
import ctypes
import ctypes.wintypes
import json
import os
import shutil
import sqlite3
import tempfile

# Chrome stores expiry in microseconds since 1601-01-01; Playwright wants
# seconds since the Unix epoch.
_WINDOWS_EPOCH_DELTA_US = 11_644_473_600_000_000

_SAMESITE = {0: "None", 1: "Lax", 2: "Strict"}

# Where a cookie DB can live inside a profile directory, newest layout first.
_COOKIE_PATHS = (("Default", "Network", "Cookies"), ("Default", "Cookies"),
                 ("Network", "Cookies"), ("Cookies",))


class _DATA_BLOB(ctypes.Structure):
    _fields_ = [("cbData", ctypes.wintypes.DWORD),
                ("pbData", ctypes.POINTER(ctypes.c_char))]


def _dpapi_unprotect(blob):
    """CryptUnprotectData — decrypts what the current Windows user encrypted."""
    src = _DATA_BLOB(len(blob),
                     ctypes.cast(ctypes.create_string_buffer(blob, len(blob)),
                                 ctypes.POINTER(ctypes.c_char)))
    out = _DATA_BLOB()
    if not ctypes.windll.crypt32.CryptUnprotectData(
            ctypes.byref(src), None, None, None, None, 0, ctypes.byref(out)):
        raise OSError("CryptUnprotectData failed")
    try:
        return ctypes.string_at(out.pbData, out.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(out.pbData)


def master_key(profile_dir):
    """The profile's AES key, unwrapped from Local State via DPAPI."""
    with open(os.path.join(profile_dir, "Local State"), encoding="utf-8") as f:
        state = json.load(f)
    enc = base64.b64decode(state["os_crypt"]["encrypted_key"])
    if enc[:5] != b"DPAPI":
        raise ValueError("unexpected encrypted_key prefix")
    return _dpapi_unprotect(enc[5:])


def _copy_shared(src, dst):
    """Copy a file another process holds open.

    shutil.copy2 opens without FILE_SHARE_WRITE and raises WinError 32 against a
    live Chrome. CreateFileW with all three share flags reads it anyway. This is
    only a safety net — the golden is a private copy and normally unlocked."""
    GENERIC_READ, SHARE_ALL, OPEN_EXISTING = 0x80000000, 0x07, 3
    k32 = ctypes.windll.kernel32
    # Declare the signatures. ctypes defaults a return value to c_int, which
    # TRUNCATES a 64-bit HANDLE — the truncated value then reads as a failure.
    k32.CreateFileW.restype = ctypes.c_void_p
    k32.CreateFileW.argtypes = [
        ctypes.c_wchar_p, ctypes.wintypes.DWORD, ctypes.wintypes.DWORD,
        ctypes.c_void_p, ctypes.wintypes.DWORD, ctypes.wintypes.DWORD,
        ctypes.c_void_p]
    k32.ReadFile.argtypes = [
        ctypes.c_void_p, ctypes.c_void_p, ctypes.wintypes.DWORD,
        ctypes.POINTER(ctypes.wintypes.DWORD), ctypes.c_void_p]
    k32.CloseHandle.argtypes = [ctypes.c_void_p]

    handle = k32.CreateFileW(str(src), GENERIC_READ, SHARE_ALL, None,
                             OPEN_EXISTING, 0, None)
    if not handle or handle == ctypes.c_void_p(-1).value:
        raise OSError(f"CreateFileW failed on {src}")
    try:
        buf = ctypes.create_string_buffer(1 << 20)
        got = ctypes.wintypes.DWORD()
        with open(dst, "wb") as out:
            while True:
                if not k32.ReadFile(handle, buf, len(buf), ctypes.byref(got), None):
                    raise OSError("ReadFile failed")
                if not got.value:
                    return
                out.write(buf.raw[:got.value])
    finally:
        k32.CloseHandle(handle)


def _readable_copy(profile_dir):
    """A readable COPY of the profile's cookie DB, or None."""
    for parts in _COOKIE_PATHS:
        src = os.path.join(profile_dir, *parts)
        if not os.path.isfile(src):
            continue
        dst = os.path.join(tempfile.mkdtemp(prefix="cbridge_"), "Cookies")
        try:
            shutil.copy2(src, dst)          # normal case: the golden is a copy
            return dst
        except OSError:
            pass
        try:
            _copy_shared(src, dst)          # a browser holds it open
            return dst
        except OSError:
            continue
    return None


def read_cookies(profile_dir, domains=(), log=print):
    """Playwright cookie dicts from a Chrome profile, filtered to `domains`.

    `domains` matches as a substring of the cookie host, so "google.com" covers
    ".google.com" and "accounts.google.com" alike. Empty = everything.
    """
    if os.name != "nt":
        return []
    name = os.path.basename(profile_dir.rstrip("\\/"))
    try:
        key = master_key(profile_dir)
    except Exception as e:
        log(f"  cookie bridge: no master key in {name} ({str(e)[:70]})")
        return []
    db = _readable_copy(profile_dir)
    if not db:
        log(f"  cookie bridge: cookie DB in {name} is locked or missing")
        return []
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError:
        log("  cookie bridge: the 'cryptography' package is not installed")
        return []

    try:
        con = sqlite3.connect(db)
        rows = con.execute(
            "SELECT host_key, name, encrypted_value, path, expires_utc, "
            "is_secure, is_httponly, samesite FROM cookies").fetchall()
        con.close()
    except Exception as e:
        log(f"  cookie bridge: cannot read the cookie DB ({str(e)[:70]})")
        return []

    aes = AESGCM(key)
    out, failed = [], 0
    for host, cname, enc, path, expires, secure, httponly, samesite in rows:
        if domains and not any(d in (host or "") for d in domains):
            continue
        if not enc or enc[:3] not in (b"v10", b"v11"):
            failed += 1
            continue
        try:
            # v10/v11: 3-byte scheme prefix, 12-byte nonce, then AES-GCM
            # ciphertext+tag. Some builds prepend a 32-byte domain hash to the
            # plaintext; strip it when the value is not valid UTF-8 without it.
            plain = aes.decrypt(enc[3:15], enc[15:], None)
            try:
                value = plain.decode("utf-8")
            except UnicodeDecodeError:
                value = plain[32:].decode("utf-8", "replace")
        except Exception:
            failed += 1
            continue
        cookie = {
            "name": cname,
            "value": value,
            "domain": host,
            "path": path or "/",
            "secure": bool(secure),
            "httpOnly": bool(httponly),
            "sameSite": _SAMESITE.get(samesite, "Lax"),
        }
        if expires:
            unix = (expires - _WINDOWS_EPOCH_DELTA_US) / 1_000_000.0
            if unix > 0:                    # Playwright rejects a negative expiry
                cookie["expires"] = unix
        out.append(cookie)
    log(f"  cookie bridge: {len(out)} cookie(s) from {name}"
        + (f", {failed} undecryptable" if failed else ""))
    return out


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Dump cookie NAMES from a Chrome profile")
    ap.add_argument("profile_dir")
    ap.add_argument("--domains", default="chatgpt.com,openai.com,google.com")
    a = ap.parse_args()
    got = read_cookies(a.profile_dir, tuple(d for d in a.domains.split(",") if d))
    by_host = {}
    for c in got:
        by_host.setdefault(c["domain"], []).append(c["name"])
    for h in sorted(by_host):
        print(f"  {h}: {len(by_host[h])} -> {sorted(by_host[h])[:6]}")
