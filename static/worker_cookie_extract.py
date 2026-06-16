"""Extract a logged-in Google session from a real Chrome profile and hand it to
the worker's OWN fresh, automatable profile.

WHY this exists: Chrome 136+ blocks remote-debugging (pipe + port) on a real /
default profile dir, so the worker cannot drive the operator's real profile
directly. And app-bound encryption (Chrome 127+) means the cookies cannot just
be copied to another folder (they decrypt to nothing). The only path that yields
a logged-in profile the worker CAN control: decrypt the cookies in place via
Chrome's elevation service (IElevator COM), then inject them into the worker's
own fresh profile with Playwright add_cookies.

Windows-only. Requires pycryptodome (Crypto.Cipher.AES) for AES-256-GCM.
"""
import base64
import ctypes
import json
import os
import sqlite3
import tempfile
from ctypes import POINTER, byref, c_void_p, c_long, c_ulong, WINFUNCTYPE

# Per-channel elevation-service COM identifiers (CLSID + IElevator IID).
# These are version-stable per channel but Google can rotate them — if decrypt
# starts failing after a Chrome update, refresh these from the channel's
# elevation_service registration.
_ELEVATOR = {
    "chrome":        ("708860E0-F641-4611-8895-7D867DD3675B", "463ABECF-410D-407F-8AF5-0DF35A005CC8"),
    "chrome-beta":   ("DD2646BA-3707-4BF8-B9A7-038691A68FC2", "A2721D66-376E-4D2F-9F0F-9070E9A42B5F"),
    "chrome-dev":    ("DA7FDCA5-2CAA-4637-AA17-0740584DE7DA", "BB2AA26B-343A-4072-8B6F-80557B8CE571"),
    "chrome-canary": ("704C2872-2049-435E-A469-0A534313C42B", "4F7CE041-28E9-484F-9DD0-61A8CACEFEE4"),
}


def _guid(s):
    from comtypes import GUID
    return GUID("{" + s + "}")


def get_app_bound_key(user_data_dir, channel, log=print):
    """Decrypt Local State os_crypt.app_bound_encrypted_key via the channel's
    elevation service. Returns the 32-byte AES-256 cookie key, or None."""
    ids = _ELEVATOR.get(channel)
    if not ids:
        log(f"cookie-extract: no elevation IDs for channel {channel!r}")
        return None
    try:
        with open(os.path.join(user_data_dir, "Local State"), "r", encoding="utf-8") as f:
            blob_b64 = json.load(f)["os_crypt"]["app_bound_encrypted_key"]
    except (OSError, ValueError, KeyError) as e:
        log(f"cookie-extract: cannot read app_bound_encrypted_key: {e}")
        return None
    blob = base64.b64decode(blob_b64)
    if blob[:4] != b"APPB":
        log("cookie-extract: app_bound key missing APPB prefix")
        return None
    blob = blob[4:]

    ole32 = ctypes.windll.ole32
    oleaut = ctypes.windll.oleaut32
    oleaut.SysAllocStringByteLen.restype = c_void_p
    oleaut.SysAllocStringByteLen.argtypes = [ctypes.c_char_p, ctypes.c_uint]
    oleaut.SysStringByteLen.restype = c_ulong
    oleaut.SysStringByteLen.argtypes = [c_void_p]

    ole32.CoInitializeEx(None, 0)
    clsid = _guid(ids[0]); iid = _guid(ids[1])
    p = c_void_p()
    hr = ole32.CoCreateInstance(byref(clsid), None, 4, byref(iid), byref(p))  # CLSCTX_LOCAL_SERVER
    if hr != 0 or not p.value:
        log(f"cookie-extract: CoCreateInstance failed hr=0x{hr & 0xffffffff:08x}")
        return None
    # Allow the service to impersonate so it can unwrap the user-bound layer.
    ole32.CoSetProxyBlanket(p, 0xffffffff, 0xffffffff, None, 6, 3, None, 0x40)
    inb = oleaut.SysAllocStringByteLen(blob, len(blob))
    outb = c_void_p(); lasterr = c_ulong(0)
    vtbl = ctypes.cast(p, POINTER(c_void_p))[0]
    decrypt = ctypes.cast(
        ctypes.cast(vtbl, POINTER(c_void_p))[5],  # IElevator vtable slot 5 = DecryptData
        WINFUNCTYPE(c_long, c_void_p, c_void_p, POINTER(c_void_p), POINTER(c_ulong)))
    hr2 = decrypt(p, inb, byref(outb), byref(lasterr))
    if hr2 != 0 or not outb.value:
        log(f"cookie-extract: DecryptData failed hr=0x{hr2 & 0xffffffff:08x} last_error={lasterr.value}")
        return None
    n = oleaut.SysStringByteLen(outb.value)
    data = ctypes.string_at(outb.value, n)
    # The decrypted blob ends with the 32-byte AES key.
    if len(data) < 32:
        log(f"cookie-extract: decrypted key too short ({len(data)})")
        return None
    return data[-32:]


def _decrypt_v20(enc, key):
    """AES-256-GCM decrypt a Chrome v20 cookie value. Returns the plaintext
    cookie string, or None. v20 layout: 'v20' + nonce(12) + ct + tag(16);
    the decrypted plaintext has a 32-byte prefix to strip."""
    try:
        from Crypto.Cipher import AES
    except Exception:
        return None
    if enc[:3] != b"v20":
        return None
    nonce = enc[3:15]; ct = enc[15:-16]; tag = enc[-16:]
    try:
        pt = AES.new(key, AES.MODE_GCM, nonce=nonce).decrypt_and_verify(ct, tag)
    except Exception:
        return None
    return pt[32:].decode("utf-8", "ignore") if len(pt) > 32 else pt.decode("utf-8", "ignore")


def extract_cookies(user_data_dir, profile_dir, channel, log=print):
    """Decrypt the profile's cookies into Playwright add_cookies() dicts.
    Returns [] on failure. The profile's Chrome must be CLOSED (DB lock)."""
    key = get_app_bound_key(user_data_dir, channel, log=log)
    if not key:
        return []
    src = os.path.join(user_data_dir, profile_dir, "Network", "Cookies")
    if not os.path.isfile(src):
        log(f"cookie-extract: Cookies DB not found at {src}")
        return []
    # Copy the DB so we don't touch the live file.
    tmp = os.path.join(tempfile.gettempdir(), "wrk_cookies_copy.db")
    try:
        import shutil as _sh
        _sh.copy2(src, tmp)
    except Exception as e:
        log(f"cookie-extract: cannot copy Cookies DB: {e}")
        return []
    out = []
    try:
        con = sqlite3.connect(tmp)
        cur = con.execute(
            "SELECT host_key,name,encrypted_value,path,expires_utc,is_secure,is_httponly,samesite "
            "FROM cookies")
        for host, name, enc, path, expires, secure, httponly, samesite in cur.fetchall():
            if not enc:
                continue
            val = _decrypt_v20(enc, key)
            if val is None:
                continue
            ck = {"name": name, "value": val, "domain": host, "path": path or "/",
                  "httpOnly": bool(httponly), "secure": bool(secure)}
            # Chrome expires_utc = microseconds since 1601; Playwright wants unix secs.
            if expires and expires > 0:
                ck["expires"] = int(expires / 1_000_000 - 11644473600)
            ck["sameSite"] = {0: "None", 1: "Lax", 2: "Strict"}.get(samesite, "Lax")
            out.append(ck)
        con.close()
    except Exception as e:
        log(f"cookie-extract: sqlite read failed: {e}")
    finally:
        try:
            os.remove(tmp)
        except Exception:
            pass
    log(f"cookie-extract: decrypted {len(out)} cookies from {profile_dir}")
    return out
