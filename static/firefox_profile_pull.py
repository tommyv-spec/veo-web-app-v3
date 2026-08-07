#!/usr/bin/env python3
"""Seed a Firefox (Camoufox) worker golden from a real Firefox profile on this
machine, selected BY GOOGLE ACCOUNT EMAIL.

This is the Firefox counterpart of `worker_profile_pull.py`, which does the same
job for Chrome. Without it a Firefox worker demands a manual Google sign-in on
first run, and the worker only waits ~2 minutes before misreporting the cause
("Account is NOT ULTRA").

WHY IT CANNOT JUST COPY THE PROFILE
-----------------------------------
Two hard constraints, both learned the expensive way on 2026-08-07:

1. Firefox REFUSES a profile written by a newer Firefox. It exits 0 with no
   error and Playwright reports only "Failed to launch the browser process".
   The real Firefox here is 153; Camoufox ships 152. Copying the whole profile
   directory would therefore produce a browser that silently never starts.
   => Copy DATA files only. Never compatibility.ini / prefs.js / extensions.

2. A Chrome profile is unreadable by Firefox. flow_worker already refuses to
   seed a Firefox golden from the Chrome laptop pull, because doing so wrote
   Chrome files into firefox-golden and broke the login every launch.

HOW THE EMAIL LOOKUP WORKS
--------------------------
Chrome records the signed-in address in its `Preferences` JSON, so Chrome's
locate_profile() can match offline. **Firefox stores no Google account email
anywhere on disk** — a Google login is nothing but cookies. So the mapping is
resolved by ASKING GOOGLE: copy the profile's cookie store into a throwaway
Camoufox profile, load accounts.google.com/ListAccounts, and read back which
addresses that cookie jar is signed into. Results are cached so the probe runs
once per profile per session.

An explicit override always wins and skips the probe entirely:
    ACCOUNT1_FIREFOX_PROFILE=tocgh2wh.default-release

Note: unlike Chrome, Firefox cookie values are NOT OS-encrypted, so no
App-Bound-Encryption / DPAPI handling is needed. The real profile is only ever
READ — never launched, automated, or signed out.
"""

import configparser
import os
import shutil
import sqlite3
import tempfile

# Data files that carry the session and survive a version gap. Deliberately does
# NOT include compatibility.ini / prefs.js / extensions / storage — those are
# version- and path-sensitive and are what make a copied profile refuse to open.
_DURABLE_FILES = (
    "cookies.sqlite",
    "cookies.sqlite-wal",
    "cookies.sqlite-shm",
    "key4.db",
    "cert9.db",
    "logins.json",
    "permissions.sqlite",
)

_EMAIL_CACHE = {}


def firefox_profiles_root():
    """%APPDATA%\\Mozilla\\Firefox on Windows; the standard dirs elsewhere."""
    override = os.environ.get("FIREFOX_PROFILES_ROOT")
    if override:
        return override
    appdata = os.environ.get("APPDATA")
    if appdata:
        return os.path.join(appdata, "Mozilla", "Firefox")
    home = os.path.expanduser("~")
    mac = os.path.join(home, "Library", "Application Support", "Firefox")
    return mac if os.path.isdir(mac) else os.path.join(home, ".mozilla", "firefox")


def list_firefox_profiles():
    """Every profile directory, newest-used first where we can tell.

    Reads profiles.ini when present (authoritative), else scans Profiles/.
    """
    root = firefox_profiles_root()
    found = []

    ini = os.path.join(root, "profiles.ini")
    if os.path.isfile(ini):
        cp = configparser.ConfigParser()
        try:
            cp.read(ini, encoding="utf-8")
            for section in cp.sections():
                if not section.lower().startswith("profile"):
                    continue
                path = cp.get(section, "Path", fallback=None)
                if not path:
                    continue
                is_rel = cp.get(section, "IsRelative", fallback="1").strip() == "1"
                full = os.path.join(root, path.replace("/", os.sep)) if is_rel else path
                if os.path.isdir(full):
                    found.append(full)
        except Exception:
            pass

    if not found:
        prof_dir = os.path.join(root, "Profiles")
        if os.path.isdir(prof_dir):
            found = [os.path.join(prof_dir, d) for d in os.listdir(prof_dir)
                     if os.path.isdir(os.path.join(prof_dir, d))]

    # A profile with no cookie store cannot hold a Google session.
    found = [d for d in found if os.path.isfile(os.path.join(d, "cookies.sqlite"))]
    found.sort(key=lambda d: os.path.getmtime(os.path.join(d, "cookies.sqlite")),
               reverse=True)
    return found


def google_cookie_count(profile_dir):
    """How many google.com cookies this profile holds. Offline, read-only.

    Copies the DB first: a live Firefox holds a lock, and we must never write
    to the operator's real profile.
    """
    src = os.path.join(profile_dir, "cookies.sqlite")
    if not os.path.isfile(src):
        return 0
    tmp = os.path.join(tempfile.gettempdir(),
                       f"ffpull_probe_{abs(hash(profile_dir))}.sqlite")
    try:
        shutil.copy2(src, tmp)
        con = sqlite3.connect(tmp)
        try:
            n = con.execute(
                "SELECT COUNT(*) FROM moz_cookies WHERE host LIKE '%google.com'"
            ).fetchone()[0]
        finally:
            con.close()
        return int(n or 0)
    except Exception:
        return 0
    finally:
        try:
            os.remove(tmp)
        except Exception:
            pass


def profile_google_emails(profile_dir, log=print):
    """Which Google addresses this profile's cookie jar is signed into.

    Runs the browser probe in a SUBPROCESS. The worker calls this from inside a
    running asyncio loop, where Playwright's sync API refuses to start
    ("It looks like you are using Playwright Sync API inside the asyncio loop").
    A child process has its own loop and is immune. Falls back to in-process
    when the subprocess route is unavailable (e.g. running as a plain script).
    """
    if profile_dir in _EMAIL_CACHE:
        return _EMAIL_CACHE[profile_dir]

    import subprocess
    import sys as _sys
    try:
        proc = subprocess.run(
            [_sys.executable, os.path.abspath(__file__), "--emails-for", profile_dir],
            capture_output=True, text=True, timeout=180)
        found = []
        for line in (proc.stdout or "").splitlines():
            if line.startswith("EMAIL="):
                found.append(line.split("=", 1)[1].strip())
        if found:
            _EMAIL_CACHE[profile_dir] = found
            return found
        if proc.returncode != 0:
            log(f"[ff-pull] email probe exited {proc.returncode}: "
                f"{(proc.stderr or '')[-160:]}")
    except Exception as e:
        log(f"[ff-pull] email subprocess failed ({str(e)[:100]}) - trying in-process")
    else:
        _EMAIL_CACHE[profile_dir] = []
        return []

    return _probe_google_emails_inproc(profile_dir, log=log)


def _probe_google_emails_inproc(profile_dir, log=print):
    """The actual browser probe. Only safe outside an asyncio loop."""
    if profile_dir in _EMAIL_CACHE:
        return _EMAIL_CACHE[profile_dir]

    emails = []
    work = tempfile.mkdtemp(prefix="ffpull_email_")
    try:
        for name in ("cookies.sqlite", "cookies.sqlite-wal", "cookies.sqlite-shm"):
            src = os.path.join(profile_dir, name)
            if os.path.isfile(src):
                shutil.copy2(src, os.path.join(work, name))

        from playwright.sync_api import sync_playwright
        from camoufox.sync_api import NewBrowser
        kwargs = {"user_data_dir": work, "headless": True, "os": "windows"}
        try:
            from camoufox.addons import DefaultAddons
            kwargs["exclude_addons"] = [DefaultAddons.UBO]
        except ImportError:
            pass

        with sync_playwright() as p:
            ctx = NewBrowser(p, persistent_context=True, **kwargs)
            try:
                import re

                page = ctx.pages[0] if ctx.pages else ctx.new_page()
                # myaccount renders the signed-in address in the page. Measured
                # 2026-08-07: accounts.google.com/ListAccounts returned nothing
                # usable from a cookie-only jar, myaccount returned the address
                # on the first try. Read the rendered HTML, not inner_text —
                # the address lives in attributes as well as visible text.
                page.goto("https://myaccount.google.com/", timeout=45000)
                if "signin" in page.url or "ServiceLogin" in page.url:
                    log(f"[ff-pull] {os.path.basename(profile_dir)} is not signed in")
                else:
                    html = page.content()
                    hits = sorted(set(re.findall(
                        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", html or "")))
                    # Drop asset filenames that look superficially like addresses.
                    emails = [h for h in hits
                              if not h.lower().endswith((".png", ".js", ".css", ".svg", ".gif"))]
            finally:
                try:
                    ctx.close()
                except Exception:
                    pass
    except Exception as e:
        log(f"[ff-pull] could not resolve email for "
            f"{os.path.basename(profile_dir)}: {str(e)[:120]}")
    finally:
        shutil.rmtree(work, ignore_errors=True)

    _EMAIL_CACHE[profile_dir] = emails
    return emails


def locate_firefox_profile(email, account_num=None, log=print):
    """Directory of the Firefox profile signed into `email`, or None.

    Order: explicit env override > email match via Google > the only candidate.
    """
    if account_num:
        override = os.environ.get(f"ACCOUNT{account_num}_FIREFOX_PROFILE", "").strip()
        if override:
            cand = override if os.path.isdir(override) else os.path.join(
                firefox_profiles_root(), "Profiles", override)
            if os.path.isdir(cand):
                log(f"[ff-pull] using configured profile: {os.path.basename(cand)}")
                return cand
            log(f"[ff-pull] ACCOUNT{account_num}_FIREFOX_PROFILE={override!r} not found")

    candidates = [d for d in list_firefox_profiles() if google_cookie_count(d) > 0]
    if not candidates:
        log("[ff-pull] no Firefox profile on this machine has a Google session")
        return None

    if email:
        target = email.strip().lower()
        for d in candidates:
            emails = [e.lower() for e in profile_google_emails(d, log=log)]
            if target in emails:
                log(f"[ff-pull] matched {email} -> {os.path.basename(d)}")
                return d
        log(f"[ff-pull] no Firefox profile is signed into {email}")
        # Do NOT silently fall back to a different account — that would run the
        # worker as the wrong user.
        return None

    if len(candidates) == 1:
        log(f"[ff-pull] single candidate: {os.path.basename(candidates[0])}")
        return candidates[0]

    log(f"[ff-pull] {len(candidates)} profiles have Google sessions but no email "
        f"was given - set ACCOUNTn_FIREFOX_PROFILE to choose")
    return None


def build_firefox_golden_from_profile(email, golden_folder, label="",
                                      account_num=None, log=print):
    """Build `golden_folder` as a Firefox profile carrying `email`'s session.

    Copies the durable DATA files only (see _DURABLE_FILES). Returns True on
    success, False on any skip. Never raises — a failure must degrade to the
    manual sign-in, never crash the worker.
    """
    tag = f"[{label}] " if label else ""
    try:
        src = locate_firefox_profile(email, account_num=account_num, log=log)
        if not src:
            log(f"{tag}ff-pull: no source profile - worker will ask for a manual sign-in")
            return False

        staged = golden_folder + ".ffpull-tmp"
        shutil.rmtree(staged, ignore_errors=True)
        os.makedirs(staged, exist_ok=True)

        copied = []
        for name in _DURABLE_FILES:
            s = os.path.join(src, name)
            if os.path.isfile(s):
                try:
                    shutil.copy2(s, os.path.join(staged, name))
                    copied.append(name)
                except Exception as e:
                    log(f"{tag}ff-pull: could not copy {name}: {str(e)[:80]}")

        if "cookies.sqlite" not in copied:
            log(f"{tag}ff-pull: no cookie store copied - aborting, golden unchanged")
            shutil.rmtree(staged, ignore_errors=True)
            return False

        # Atomic-ish swap so a failure never leaves a half-built golden.
        backup = golden_folder + ".ffpull-old"
        shutil.rmtree(backup, ignore_errors=True)
        if os.path.isdir(golden_folder):
            os.rename(golden_folder, backup)
        os.rename(staged, golden_folder)
        shutil.rmtree(backup, ignore_errors=True)

        log(f"{tag}ff-pull: golden built from {os.path.basename(src)} "
            f"({len(copied)} files: {', '.join(copied)})")
        return True
    except Exception as e:
        log(f"{tag}ff-pull: failed ({str(e)[:150]}) - falling back to manual sign-in")
        return False


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--email", default="", help="Google address to look for")
    ap.add_argument("--list", action="store_true", help="List profiles and exit")
    ap.add_argument("--emails-for", default="", metavar="PROFILE_DIR",
                    help="Internal: probe one profile, print EMAIL=<addr> lines")
    a = ap.parse_args()

    if a.emails_for:
        # Subprocess entry point used by profile_google_emails(): the worker
        # runs inside an asyncio loop where the sync Playwright API refuses to
        # start, so the probe is exiled to a child process.
        for e in _probe_google_emails_inproc(a.emails_for, log=lambda m: None):
            print(f"EMAIL={e}")
        raise SystemExit(0)

    if a.list or not a.email:
        print(f"Firefox root: {firefox_profiles_root()}")
        profs = list_firefox_profiles()
        if not profs:
            print("  (no profiles with a cookie store)")
        for d in profs:
            n = google_cookie_count(d)
            print(f"  {os.path.basename(d):32s} google cookies: {n}")
            if n:
                em = profile_google_emails(d)
                print(f"      signed in as: {', '.join(em) if em else '(could not resolve)'}")
        raise SystemExit(0)

    found = locate_firefox_profile(a.email)
    print(f"profile for {a.email}: {found or 'NOT FOUND'}")
