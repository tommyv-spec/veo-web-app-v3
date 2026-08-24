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
import sys
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
        # Relay the probe's own diagnostics. The child sends them on stderr
        # (stdout is reserved for EMAIL= lines); without this relay they die in
        # the captured pipe and a probe that resolved nothing is
        # indistinguishable from a signed-out profile — which is exactly how
        # the accountchooser misclassification stayed invisible for a day.
        for line in (proc.stderr or "").splitlines():
            if line.startswith("[ff-pull]"):
                log(line)
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
                # PARSE THE PAGE FIRST, JUDGE THE URL SECOND (fixed 2026-08-23).
                #
                # This used to bail whenever the URL contained "signin", before
                # reading anything. Google has since stopped sending a fresh
                # browser with a copied cookie jar straight to myaccount — it
                # routes to the ACCOUNT CHOOSER instead:
                #
                #   https://accounts.google.com/v3/signin/accountchooser?authuser=0&...
                #
                # That URL contains "signin", so a perfectly live profile was
                # declared "not signed in" while the page it refused to read
                # listed the address in plain sight. The chooser appearing is
                # POSITIVE evidence: a dead session gets a password prompt, not a
                # list of accounts to pick from.
                #
                # So: always read the HTML and look for addresses. Only call it
                # signed out when the page yields none AND the URL is a real
                # sign-in wall. A password/challenge page has no address list,
                # so it still resolves to "not signed in" correctly.
                html = page.content()
                hits = sorted(set(re.findall(
                    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", html or "")))
                # Drop asset filenames that look superficially like addresses.
                emails = [h for h in hits
                          if not h.lower().endswith((".png", ".js", ".css", ".svg", ".gif"))]
                url = page.url or ""
                if emails:
                    where = ("account chooser" if "accountchooser" in url
                             else "myaccount")
                    # Say only what was proven. The chooser lists REMEMBERED
                    # accounts, signed-out ones included, so "signed in" was a
                    # claim this probe cannot make — and reading it as one is
                    # what sent four decodes at a logged-out Gemini on
                    # 2026-08-24. Liveness is session_is_live()'s job.
                    proven = ("carries an account for" if where == "account chooser"
                              else "signed in as")
                    log(f"[ff-pull] {os.path.basename(profile_dir)} {proven} "
                        f"(resolved via {where}): {', '.join(emails[:3])}")
                elif "signin" in url or "ServiceLogin" in url:
                    log(f"[ff-pull] {os.path.basename(profile_dir)} is not signed in "
                        f"(no address on {url[:80]})")
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


def _read_cookies_wal_applied(profile_dir):
    """Cookies from a profile whose Firefox is RUNNING.

    `read_cookies_for_playwright` plain-copies cookies.sqlite. When the operator's
    own Firefox is open — the normal case, it is the SSO source — the current
    session sits in a `-wal` that has not been checkpointed, so that copy returns
    rows that are minutes-to-hours stale. On 2026-08-24 the WAL was 557 KB and a
    WAL-blind read made a perfectly good, logged-in profile probe as SIGNED OUT.

    con.backup() reads THROUGH the WAL, the same call build_firefox_golden uses.
    """
    src = os.path.join(profile_dir, "cookies.sqlite")
    if not os.path.isfile(src):
        return []
    tmp = os.path.join(tempfile.gettempdir(),
                       f"ffpull_live_{abs(hash(profile_dir))}.sqlite")
    try:
        con = sqlite3.connect(f"file:{src}?mode=ro", uri=True, timeout=10)
        try:
            dst = sqlite3.connect(tmp)
            try:
                con.backup(dst)
            finally:
                dst.close()
        finally:
            con.close()
    except Exception:
        # a degraded read must never crash a caller; fall back to the plain copy
        return read_cookies_for_playwright(profile_dir, log=lambda m: None)
    out = []
    try:
        c = sqlite3.connect(tmp)
        try:
            for name, value, host in c.execute(
                    "SELECT name, value, host FROM moz_cookies"):
                if name and host is not None:
                    out.append({"name": name, "value": value or "", "domain": host})
        finally:
            c.close()
    except Exception:
        return []
    return out


# DO NOT ADD A COOKIE-REPLAY LIVENESS PROBE HERE.
#
# A `session_is_live()` lived here for a few hours on 2026-08-24. It took the
# profile's Google auth cookies and replayed them at myaccount.google.com from
# python-requests with a spoofed Firefox user-agent, to answer "is this jar
# still good". Two things went wrong and both are worth the paragraph:
#
# 1. IT WAS WRONG. It read cookies.sqlite without the -wal, so a profile that
#    was signed in the whole time probed as dead - twice - and that verdict was
#    briefly wired into this function as a hard refusal to build a golden.
# 2. IT GOT THE ACCOUNT SIGNED OUT. Same credentials + non-browser client +
#    the account-SECURITY surface, repeated across ~13 profiles in one sweep,
#    is the shape of a session-hijack attempt. Google invalidated the session
#    everywhere, including the operator's own desktop Firefox, and he had to
#    sign in again.
#
# The browser that launches a moment later answers the same question for free,
# accurately, and without touching myaccount. If a jar is bad, the SSO handshake
# says so; if Google truly wants credentials it lands on /signin/challenge/ and
# gemini_decode_worker reports that. Let the browser do it.


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

        # NOTE: deliberately NOT probing myaccount.google.com here. `session_is_live`
        # exists for diagnosis and stays callable by hand, but it is off the build
        # path on purpose: myaccount is an account-MANAGEMENT surface and touching
        # it with a freshly copied jar invites the very password challenge this
        # lane cannot answer. The browser's own SSO handshake is both the cheaper
        # and the more accurate test, and it runs a moment later anyway.

        staged = golden_folder + ".ffpull-tmp"
        shutil.rmtree(staged, ignore_errors=True)
        os.makedirs(staged, exist_ok=True)

        copied = []

        # cookies.sqlite is copied with SQLite's BACKUP API, not shutil (2026-08-23).
        #
        # Why: when Firefox is RUNNING — the normal case, since the operator's own
        # profile is the SSO source — the live session cookies sit in the -wal and
        # have not been checkpointed into the main file. A plain file copy of
        # (db, -wal, -shm) is wrong three ways: the three files are copied at three
        # different instants from a database being written, so the snapshot can be
        # torn; and -shm belongs to the WRITING PROCESS, so a copied one can make
        # SQLite misread or ignore the WAL outright. The result is a golden that
        # carries cookie ROWS but no live SESSION — which reads as "39 Google
        # cookies present" and still lands on the signed-out page.
        #
        # con.backup() reads through the WAL and writes ONE consistent file, so the
        # session survives. -wal/-shm are deliberately NOT copied; SQLite rebuilds
        # them. Falls back to the old copy if the backup path fails, because a
        # degraded pull must never crash the worker.
        src_db = os.path.join(src, "cookies.sqlite")
        if os.path.isfile(src_db):
            try:
                con = sqlite3.connect(f"file:{src_db}?mode=ro", uri=True, timeout=10)
                try:
                    dst = sqlite3.connect(os.path.join(staged, "cookies.sqlite"))
                    try:
                        con.backup(dst)
                    finally:
                        dst.close()
                finally:
                    con.close()
                copied.append("cookies.sqlite")
                log(f"{tag}ff-pull: cookies.sqlite snapshotted via SQLite backup "
                    f"(WAL applied; -wal/-shm intentionally not copied)")
            except Exception as e:
                log(f"{tag}ff-pull: SQLite backup failed ({str(e)[:80]}) - "
                    f"falling back to a raw copy, session may not survive")
                for name in ("cookies.sqlite", "cookies.sqlite-wal", "cookies.sqlite-shm"):
                    s = os.path.join(src, name)
                    if os.path.isfile(s):
                        try:
                            shutil.copy2(s, os.path.join(staged, name))
                            copied.append(name)
                        except Exception as e2:
                            log(f"{tag}ff-pull: could not copy {name}: {str(e2)[:80]}")

        for name in _DURABLE_FILES:
            if name.startswith("cookies.sqlite"):
                continue  # handled above
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
        # Send the probe's own log lines back to the PARENT on stderr (2026-08-23).
        #
        # They used to be discarded (`log=lambda m: None`). That is how a
        # misclassification stayed invisible for a day: the probe knew it had
        # landed on an account chooser, said so, and the message went nowhere —
        # the caller only ever saw the summary "no Firefox profile is signed
        # into <email>". stdout stays reserved for the EMAIL= lines the parent
        # parses, so diagnostics go to stderr.
        for e in _probe_google_emails_inproc(
                a.emails_for,
                log=lambda m: print(m, file=sys.stderr, flush=True)):
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


def read_cookies_for_playwright(profile_dir, log=print):
    """Firefox cookies.sqlite -> Playwright add_cookies() dicts.

    Needed because rebuilding the golden only rewrites FILES. When the session
    dies mid-run the browser already has the profile open, so overwriting
    cookies.sqlite underneath it changes nothing about the live session — the
    dead cookies stay in memory. Injecting them into the running context is what
    actually recovers without a browser restart.

    Reads a COPY: the source may be locked, and the operator's real profile must
    never be written to. Returns [] on any failure.
    """
    src = os.path.join(profile_dir, "cookies.sqlite")
    if not os.path.isfile(src):
        return []
    tmp = os.path.join(tempfile.gettempdir(),
                       f"ffpull_ck_{abs(hash(profile_dir))}.sqlite")
    out = []
    try:
        shutil.copy2(src, tmp)
        con = sqlite3.connect(tmp)
        try:
            rows = con.execute(
                "SELECT name, value, host, path, expiry, isSecure, isHttpOnly, sameSite "
                "FROM moz_cookies"
            ).fetchall()
        finally:
            con.close()
        # Firefox stores sameSite as 0/1/2; Playwright wants the words.
        same = {0: "None", 1: "Lax", 2: "Strict"}
        for name, value, host, path, expiry, secure, httponly, ss in rows:
            if not name or host is None:
                continue
            ck = {
                "name": name,
                "value": value or "",
                "domain": host,
                "path": path or "/",
                "httpOnly": bool(httponly),
                "secure": bool(secure),
                "sameSite": same.get(ss, "Lax"),
            }
            # expiry 0 == session cookie -> omit the key entirely. Firefox also
            # stores NEGATIVE expiries for some rows, and Playwright rejects the
            # whole add_cookies() batch on one bad value ("only -1 or a positive
            # number ... is allowed"), so guard for >0 rather than truthiness.
            # expiry 0 == session cookie -> omit the key entirely. Two traps:
            #  * Firefox stores NEGATIVE expiries on some rows.
            #  * This profile stores expiry in MILLISECONDS (measured: 1787448315241
            #    == 2026-08-21), while Playwright wants SECONDS. Feeding ms through
            #    puts the date in the year 58000 and Playwright rejects the WHOLE
            #    add_cookies() batch: "only -1 or a positive number ... is allowed".
            # One bad value loses every cookie, so normalise and guard here.
            try:
                exp = float(expiry or 0)
                if exp > 1e11:      # clearly milliseconds, not seconds
                    exp /= 1000.0
                if exp > 0:
                    ck["expires"] = exp
            except (TypeError, ValueError):
                pass
            out.append(ck)
    except Exception as e:
        log(f"[ff-pull] could not read cookies for injection: {str(e)[:120]}")
        return []
    finally:
        try:
            os.remove(tmp)
        except Exception:
            pass
    return out
