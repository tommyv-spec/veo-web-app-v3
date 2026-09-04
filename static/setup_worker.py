#!/usr/bin/env python3
"""
Veo Flow Worker - Setup Script
===============================
One-time setup to run your own Flow worker on your machine.

Usage:
    python setup_worker.py
    python setup_worker.py --token YOUR_TOKEN   (skip interactive prompt)
    python setup_worker.py --update             (update worker script only)
"""

import json
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================

WEB_APP_URL = "https://kavenobuilder.com"
WORKER_DOWNLOAD_URL = f"{WEB_APP_URL}/api/user-worker/download/flow_worker.py"
WORKER_DIR = Path.home() / "veo-worker"
MIN_PYTHON = (3, 9)




# ============================================================
# SETUP FLOW
# ============================================================

def check_python():
    if sys.version_info < MIN_PYTHON:
        print(f"Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required (you have {sys.version})")
        print("Download from: https://python.org/downloads")
        sys.exit(1)
    print(f"  Python {sys.version_info.major}.{sys.version_info.minor} OK")


def find_windows_python():
    """Find a proper Windows Python (not MSYS2/Cygwin) that has pip."""
    import shutil, platform
    if platform.system() != "Windows":
        return sys.executable

    # Bad paths to avoid — MSYS2, Cygwin, Git Bash all lack pip
    bad_prefixes = ("msys", "cygwin", "mingw", "ucrt64", "clang64")

    candidates = []

    # 1. py launcher (most reliable on Windows)
    py = shutil.which("py")
    if py:
        candidates.append(py)

    # 2. python / python3 from PATH, skipping bad prefixes
    for name in ("python", "python3"):
        found = shutil.which(name)
        if found:
            low = found.replace("\\", "/").lower()
            if not any(b in low for b in bad_prefixes):
                candidates.append(found)

    # 3. Common install locations
    import os
    for root in (os.environ.get("LOCALAPPDATA", ""), os.environ.get("ProgramFiles", ""), os.environ.get("ProgramFiles(x86)", "")):
        if not root:
            continue
        for sub in ("Python313", "Python312", "Python311", "Python310", "Python39"):
            p = os.path.join(root, "Programs", "Python", sub, "python.exe")
            if os.path.exists(p):
                candidates.append(p)
            p2 = os.path.join(root, sub, "python.exe")
            if os.path.exists(p2):
                candidates.append(p2)

    # Pick the first candidate that actually has pip
    for candidate in candidates:
        try:
            result = subprocess.run([candidate, "-m", "pip", "--version"], capture_output=True, timeout=10)
            if result.returncode == 0:
                return candidate
        except Exception:
            continue

    # Fallback to current executable
    return sys.executable


def install_dependencies():
    print("\n  Installing/updating Python packages...")

    # On Windows, make sure we use a Python that has pip (not MSYS2)
    import platform
    pip_python = find_windows_python() if platform.system() == "Windows" else sys.executable
    if pip_python != sys.executable:
        print(f"  Using Python: {pip_python}")

    # Check what's already installed.
    # camoufox is the Firefox (Camoufox) automation backend used by BROWSER_MODE=firefox,
    # the new default (see write_config) — must be present on a clean machine.
    packages = [
        ("playwright", "playwright"),
        ("requests", "requests"),
        ("camoufox", "camoufox>=0.5.4"),
    ]
    missing = []
    for import_name, pip_spec in packages:
        try:
            __import__(import_name)
        except ImportError:
            missing.append(pip_spec)

    if not missing:
        print("  ✓ Python packages already installed")
    else:
        print(f"  Installing: {', '.join(missing)}")
        pip_cmd = [pip_python, "-m", "pip", "install"] + missing + ["--quiet"]
        try:
            subprocess.check_call(pip_cmd)
        except subprocess.CalledProcessError:
            # Fallback with --user flag
            pip_cmd_fallback = [pip_python, "-m", "pip", "install"] + missing + ["--quiet", "--user"]
            subprocess.check_call(pip_cmd_fallback)
        print("  ✓ Python packages OK")

        # If we installed using a different Python than the one running this script,
        # re-launch using that Python so the installed packages are importable
        if pip_python != sys.executable and "--relaunched" not in sys.argv:
            print(f"  Re-launching with correct Python: {pip_python}")
            import subprocess as _sp
            result = _sp.run([pip_python] + sys.argv + ["--relaunched"])
            sys.exit(result.returncode)

    # Fetch the Camoufox (Firefox) browser binary. This downloads from GitHub, which
    # the operator's VPN can sometimes block -- a failed fetch must WARN, not abort
    # setup, since flow_worker._ensure_camoufox() retries the fetch at launch anyway.
    print("  Fetching Camoufox browser binary...")
    try:
        subprocess.check_call([pip_python, "-m", "camoufox", "fetch"])
        print("  OK Camoufox browser ready")
    except Exception as e:
        print(f"  WARNING: Camoufox browser fetch failed ({e})")
        print("  This can happen if GitHub is blocked (VPN/firewall).")
        print(f"  Fix it later by running: {pip_python} -m camoufox fetch")

    # Verify Chrome is available (we use system Chrome, not Playwright's Chromium)
    chrome_paths = [
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    ]
    chrome_found = any(os.path.exists(p) for p in chrome_paths)
    if chrome_found:
        print("  ✓ Google Chrome found")
    else:
        print("  ⚠ Google Chrome not detected — please install it from https://google.com/chrome")
        print("    The worker requires Google Chrome (not Chromium)")


def download_worker():
    import requests as req
    import hashlib
    worker_path = WORKER_DIR / "flow_worker.py"
    
    # Check if local file exists and compare hashes
    if worker_path.exists():
        local_hash = hashlib.md5(worker_path.read_bytes()).hexdigest()[:12]
        try:
            resp = req.get(f"{WEB_APP_URL}/api/user-worker/version", timeout=5)
            if resp.ok:
                remote_hash = resp.json().get("version", "")
                if remote_hash and local_hash != remote_hash:
                    # Remote is different — but local might be NEWER (deployed via scp)
                    # Check .deploy_version stamp
                    deploy_stamp = WORKER_DIR / ".deploy_version"
                    if deploy_stamp.exists():
                        print(f"\n  Local worker was deployed directly (hash: {local_hash})")
                        print(f"  Remote hash: {remote_hash}")
                        print(f"  Keeping local version (deployed via scp)")
                        return
                    # No stamp — download from Render
                    print(f"\n  Downloading worker script...")
                    dl = req.get(WORKER_DOWNLOAD_URL, timeout=30)
                    dl.raise_for_status()
                    worker_path.write_bytes(dl.content)
                    print(f"  Worker saved: {worker_path}")
                else:
                    print(f"\n  Worker already up to date ({local_hash})")
                return
        except Exception:
            pass
    
    # No local file or version check failed — download
    print(f"\n  Downloading worker script...")
    try:
        resp = req.get(WORKER_DOWNLOAD_URL, timeout=30)
        resp.raise_for_status()
        worker_path.write_bytes(resp.content)
        print(f"  Worker saved: {worker_path}")
    except Exception as e:
        if worker_path.exists():
            print(f"  Download failed but local worker exists — keeping it")
        else:
            print(f"  Could not download worker: {e}")
            print(f"  You can manually place flow_worker.py in {WORKER_DIR}")


def get_token(args_token=None, admin_mode=False):
    if admin_mode:
        return "local-worker-secret-key-12345"
    if args_token:
        return args_token
    print("\n  Get your worker token from the web app:")
    print(f"  {WEB_APP_URL} → Settings → My Worker → Generate Token")
    print(f"  Or run with --admin for admin mode")
    print()
    token = input("  Paste your worker token: ").strip()
    if not token:
        print("  Token required. Exiting.")
        sys.exit(1)
    return token


def setup_chrome(account_num=1):
    """Set up Chrome profile for a given account number."""
    if account_num == 1:
        session_dir = WORKER_DIR / "chrome-session"
        download_dir = WORKER_DIR / "chrome-download"
    else:
        session_dir = WORKER_DIR / f"chrome-session-{account_num}"
        download_dir = WORKER_DIR / f"chrome-download-{account_num}"

    # Determine golden dir path
    if account_num == 1:
        golden_dir = WORKER_DIR / "chrome-golden"
    else:
        golden_dir = WORKER_DIR / f"chrome-golden-{account_num}"

    # If session already exists, ask whether to reuse or start fresh
    if session_dir.exists() and (session_dir / "Default").exists():
        print(f"\n  Chrome session {account_num} already exists.")
        print(f"  Y = restore from golden (clean logged-in state)")
        print(f"  N = wipe everything and start fresh (login required)")
        reuse = input("  Reuse existing session? (Y/n): ").strip().lower()
        if reuse in ("", "y", "yes"):
            if golden_dir.exists():
                print(f"\n  Restoring session and download from golden profile...")
                # Wipe and re-copy both from golden
                shutil.rmtree(session_dir, ignore_errors=True)
                shutil.rmtree(download_dir, ignore_errors=True)
                try:
                    shutil.copytree(golden_dir, session_dir,
                        ignore=shutil.ignore_patterns('SingletonLock', 'SingletonSocket', 'SingletonCookie'),
                        ignore_dangling_symlinks=True)
                    print(f"  ✓ Session restored from golden: {session_dir}")
                except Exception as e:
                    print(f"  ⚠ Could not restore session: {e}")
                try:
                    shutil.copytree(golden_dir, download_dir,
                        ignore=shutil.ignore_patterns('SingletonLock', 'SingletonSocket', 'SingletonCookie'),
                        ignore_dangling_symlinks=True)
                    print(f"  ✓ Download restored from golden: {download_dir}")
                except Exception as e:
                    print(f"  ⚠ Could not restore download: {e}")
            else:
                print(f"  ⚠ No golden profile found — keeping existing session as-is")
                download_dir.mkdir(parents=True, exist_ok=True)
            return session_dir, download_dir
        # Fresh setup — wipe all 3 folders
        print("  Removing old session, download, and golden folders...")
        shutil.rmtree(session_dir, ignore_errors=True)
        shutil.rmtree(download_dir, ignore_errors=True)
        shutil.rmtree(golden_dir, ignore_errors=True)
    else:
        # No existing session — clean up whatever partial state exists
        shutil.rmtree(session_dir, ignore_errors=True)
        shutil.rmtree(download_dir, ignore_errors=True)
        shutil.rmtree(golden_dir, ignore_errors=True)

    # Create fresh empty session — user will login via the worker on first start
    print(f"\n  Creating fresh Chrome session for account {account_num}...")
    print(f"  You'll log into Google when the worker starts.")
    session_dir.mkdir(parents=True, exist_ok=True)
    download_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n  Account {account_num} ready (login required on first run)")
    return session_dir, download_dir


def setup_multiple_accounts():
    """Ask user how many accounts they want and set them all up."""
    print("\n  How many Google accounts do you want to use?")
    print("  More accounts = higher throughput (parallel processing)")
    try:
        count_str = input("  Number of accounts [1]: ").strip()
    except EOFError:
        count_str = "1"
    
    num_accounts = 1
    if count_str:
        try:
            num_accounts = max(1, min(4, int(count_str)))
        except ValueError:
            num_accounts = 1
    
    accounts = []
    for i in range(1, num_accounts + 1):
        if i > 1:
            print(f"\n{'─' * 40}")
        print(f"\n  Setting up Account {i} of {num_accounts}...")
        session_dir, download_dir = setup_chrome(account_num=i)
        accounts.append({
            'num': i,
            'session_dir': session_dir,
            'download_dir': download_dir,
        })
    
    return accounts


def write_config(token, accounts, admin_mode=False):
    """Write .env config supporting single or multi-account setup."""
    env_path = WORKER_DIR / ".env"
    
    mode = "admin" if admin_mode else "user"
    token_key = "LOCAL_WORKER_API_KEY" if admin_mode else "USER_WORKER_TOKEN"
    
    if len(accounts) == 1:
        # Single account — simple config
        acc = accounts[0]
        env_path.write_text(
            f"WORKER_MODE={mode}\n"
            f"{token_key}={token}\n"
            f"WEB_APP_URL={WEB_APP_URL}\n"
            f"SESSION_FOLDER={acc['session_dir']}\n"
            f"DOWNLOAD_SESSION_FOLDER={acc['download_dir']}\n"
            # Firefox (Camoufox) is the default: Chrome 151+ mints reCAPTCHA tokens Flow
            # refuses (~0% success as of 2026-08-07), while Firefox minted 10/10 accepted
            # tokens. Set BROWSER_MODE=stealth here to fall back to Chrome.
            f"BROWSER_MODE=firefox\n"
            f"MULTI_ACCOUNT=false\n"
            f"PROXY_TYPE=none\n"
        )
    else:
        # Multi-account config
        lines = [
            f"WORKER_MODE={mode}",
            f"{token_key}={token}",
            f"WEB_APP_URL={WEB_APP_URL}",
            # Firefox (Camoufox) is the default: Chrome 151+ mints reCAPTCHA tokens Flow
            # refuses (~0% success as of 2026-08-07), while Firefox minted 10/10 accepted
            # tokens. Set BROWSER_MODE=stealth here to fall back to Chrome.
            f"BROWSER_MODE=firefox",
            f"MULTI_ACCOUNT=true",
            f"MULTI_ACCOUNT_MODE=true",
            f"PROXY_TYPE=none",
            f"",
            f"# Account sessions",
        ]
        
        # First account also sets SESSION_FOLDER for single-account fallback
        lines.append(f"SESSION_FOLDER={accounts[0]['session_dir']}")
        lines.append(f"DOWNLOAD_SESSION_FOLDER={accounts[0]['download_dir']}")
        lines.append("")
        
        for acc in accounts:
            n = acc['num']
            lines.append(f"ACCOUNT{n}_SESSION={acc['session_dir']}")
            lines.append(f"ACCOUNT{n}_DOWNLOAD={acc['download_dir']}")
            lines.append(f"ACCOUNT{n}_ENABLED=true")
        
        # Disable remaining accounts
        for n in range(len(accounts) + 1, 5):
            lines.append(f"ACCOUNT{n}_ENABLED=false")
        
        env_path.write_text("\n".join(lines) + "\n")
    
    print(f"  Config saved: {env_path}")
    if len(accounts) > 1:
        print(f"  Multi-account mode: {len(accounts)} accounts enabled")


def create_launch_scripts():
    web_app_url = WEB_APP_URL.rstrip("/")
    update_url = f"{web_app_url}/api/user-worker/download/flow_worker.py"

    if platform.system() == "Windows":
        # .bat file
        bat = WORKER_DIR / "start_worker.bat"
        bat.write_text(
            f'@echo off\n'
            f'cd /d "{WORKER_DIR}"\n'
            f'echo Loading config...\n'
            f'for /f "usebackq tokens=1,* delims==" %%a in (".env") do set "%%a=%%b"\n'
            f'echo Checking for updates...\n'
            f'powershell -Command "try {{ Invoke-WebRequest -Uri \"{update_url}\" -OutFile flow_worker.py.tmp -UseBasicParsing -TimeoutSec 15; Move-Item -Force flow_worker.py.tmp flow_worker.py; Write-Host \"Updated worker\" }} catch {{ Write-Host \"Update skipped (offline?)\" }}"\n'
            # Unbuffered, or the poll line sits in a ~24 minute stdout buffer
            # the moment this window is redirected to a file, and a worker that
            # is running perfectly looks dead. flow_worker.py prints that line
            # in three places and only ONE flushes. Set AFTER the .env loop so
            # it cannot be overridden by a stale .env.
            f'set PYTHONUNBUFFERED=1\n'
            f'echo Starting KavenoBuilder Flow Worker...\n'
            f'"{sys.executable}" flow_worker.py --single\n'
            f'pause\n'
        )
        print(f"  Launch script: {bat}")

        # .ps1 file (more reliable)
        ps1 = WORKER_DIR / "start_worker.ps1"
        ps1.write_text(
            f'Set-Location "{WORKER_DIR}"\n'
            f'Get-Content .env | ForEach-Object {{\n'
            f'    if ($_ -match "^([^=]+)=(.*)$") {{\n'
            f'        [Environment]::SetEnvironmentVariable($matches[1], $matches[2], "Process")\n'
            f'    }}\n'
            f'}}\n'
            f'Write-Host "Checking for updates..."\n'
            f'try {{\n'
            f'    Invoke-WebRequest -Uri "{update_url}" -OutFile "flow_worker.py.tmp" -UseBasicParsing -TimeoutSec 15\n'
            f'    Move-Item -Force "flow_worker.py.tmp" "flow_worker.py"\n'
            f'    Write-Host "Worker updated." -ForegroundColor Green\n'
            f'}} catch {{\n'
            f'    Write-Host "Update skipped (offline?)." -ForegroundColor Yellow\n'
            f'}}\n'
            # Same reason as the .bat above, and set after the .env loop so a
            # stale .env cannot put the buffering back.
            f'$env:PYTHONUNBUFFERED = "1"\n'
            f'Write-Host "Starting KavenoBuilder Flow Worker..."\n'
            f'& "{sys.executable}" flow_worker.py --single\n'
        )
        print(f"  PowerShell script: {ps1}")
    else:
        sh = WORKER_DIR / "start_worker.sh"
        sh.write_text(
            f'#!/bin/bash\n'
            f'cd "{WORKER_DIR}"\n'
            f'set -a\n'
            f'source .env\n'
            f'set +a\n'
            f'echo "Checking for updates..."\n'
            f'curl -sf --max-time 15 "{update_url}" -o flow_worker.py.tmp && mv flow_worker.py.tmp flow_worker.py && echo "Worker updated." || echo "Update skipped."\n'
            # Same reason as the .bat above. `set -a` exported everything in
            # .env; this line runs after it, so it wins.
            f'export PYTHONUNBUFFERED=1\n'
            f'echo "Starting KavenoBuilder Flow Worker..."\n'
            f'python3 flow_worker.py --single\n'
        )
        sh.chmod(0o755)
        print(f"  Launch script: {sh}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Veo Flow Worker Setup")
    parser.add_argument("--token", type=str, help="Worker token (skip prompt)")
    parser.add_argument("--admin", action="store_true", help="Admin mode (use LOCAL_WORKER_API_KEY instead of user token)")
    parser.add_argument("--update", action="store_true", help="Update worker script only")
    parser.add_argument("--add-account", action="store_true", help="Add another Google account")
    parser.add_argument("--relaunched", action="store_true", help=argparse.SUPPRESS)  # internal flag
    args = parser.parse_args()

    print("=" * 55)
    print("  Veo Flow Worker Setup")
    print("=" * 55)

    if args.update:
        print("\nUpdating worker script...")
        WORKER_DIR.mkdir(exist_ok=True)
        download_worker()
        create_launch_scripts()
        print("\nDone! Restart your worker using start_worker.ps1 (Windows) or start_worker.sh (Mac/Linux)")
        return

    # Step 1: Python check
    print("\n[1/6] Checking Python...")
    check_python()

    # Step 2: Create directory
    print("\n[2/6] Creating worker directory...")
    WORKER_DIR.mkdir(exist_ok=True)
    print(f"  {WORKER_DIR}")

    # Step 3: Dependencies
    print("\n[3/6] Installing dependencies...")
    install_dependencies()

    # Step 4: Download worker
    print("\n[4/6] Downloading worker...")
    download_worker()

    # Step 5: Chrome profile(s)
    print("\n[5/6] Setting up Chrome...")
    if args.add_account:
        # Adding an account to existing setup - find next account number
        existing = 1
        while (WORKER_DIR / f"chrome-session-{existing + 1}").exists():
            existing += 1
        next_num = existing + 1
        print(f"  Adding Account {next_num}...")
        session_dir, download_dir = setup_chrome(account_num=next_num)
        # Read existing .env and add new account
        env_path = WORKER_DIR / ".env"
        env_text = env_path.read_text() if env_path.exists() else ""
        if "MULTI_ACCOUNT=false" in env_text:
            env_text = env_text.replace("MULTI_ACCOUNT=false", "MULTI_ACCOUNT=true")
        if "MULTI_ACCOUNT_MODE" not in env_text:
            env_text += "\nMULTI_ACCOUNT_MODE=true\n"
        env_text += f"\nACCOUNT{next_num}_SESSION={session_dir}\n"
        env_text += f"ACCOUNT{next_num}_DOWNLOAD={download_dir}\n"
        env_text += f"ACCOUNT{next_num}_ENABLED=true\n"
        env_path.write_text(env_text)
        print(f"\n  Account {next_num} added! Restart your worker to use it.")
        return
    else:
        accounts = setup_multiple_accounts()

    # Create golden profile for each account if not already present.
    # For fresh sessions (no Chrome profile copied), the golden will be
    # created automatically by the worker after the first successful login.
    print("\n  Checking golden profiles...")
    for acc in accounts:
        acc_num = acc['num']
        session_dir = acc['session_dir']
        golden_dir = WORKER_DIR / "chrome-golden" if acc_num == 1 else WORKER_DIR / f"chrome-golden-{acc_num}"
        if golden_dir.exists():
            print(f"  ✓ Account {acc_num} golden profile already exists")
        elif session_dir.exists() and (session_dir / "Default").exists():
            print(f"  Creating golden profile for account {acc_num}...")
            try:
                shutil.copytree(
                    session_dir, golden_dir,
                    ignore=shutil.ignore_patterns('SingletonLock', 'SingletonSocket', 'SingletonCookie'),
                    ignore_dangling_symlinks=True,
                )
                print(f"  ✓ Golden profile saved: {golden_dir}")
            except Exception as e:
                print(f"  ⚠ Could not create golden profile: {e}")
        else:
            print(f"  Account {acc_num}: golden will be created after first login")
    print("\n[6/6] Configuration...")
    token = get_token(args.token, admin_mode=args.admin)
    write_config(token, accounts, admin_mode=args.admin)
    create_launch_scripts()

    # Done
    print("\n" + "=" * 55)
    print("  Setup complete!")
    print("=" * 55)

    if platform.system() == "Windows":
        print(f"\n  To start your worker:")
        print(f"    Double-click: {WORKER_DIR / 'start_worker.bat'}")
        print(f"    Or run: cd {WORKER_DIR} && start_worker.bat")
    else:
        print(f"\n  To start your worker:")
        print(f"    {WORKER_DIR / 'start_worker.sh'}")

    # Offer to start now
    print()
    start = input("  Start worker now? (Y/n): ").strip().lower()
    if start in ("", "y", "yes"):
        os.chdir(WORKER_DIR)
        for line in (WORKER_DIR / ".env").read_text().splitlines():
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                os.environ[k.strip()] = v.strip()
        # Detect how many accounts are configured
        multi = os.environ.get("MULTI_ACCOUNT", "false").lower() == "true"
        enabled_accounts = []
        for n in range(1, 5):
            sess = os.environ.get(f"ACCOUNT{n}_SESSION")
            enabled = os.environ.get(f"ACCOUNT{n}_ENABLED", "false").lower() == "true"
            if sess and enabled and Path(sess).exists():
                enabled_accounts.append(str(n))

        # The child inherits this process's environment, so if the setup script
        # itself was run with its output redirected, the worker it launches
        # buffers too: flow_worker.py prints its poll line in three places and
        # only ONE flushes, which hides ~24 minutes and makes a working worker
        # read as hung. Set here rather than in the generated launchers, because
        # this is a different path from all of them — setup running the worker
        # directly instead of writing a script that will.
        os.environ["PYTHONUNBUFFERED"] = "1"

        if multi and len(enabled_accounts) > 1:
            print(f"  Detected {len(enabled_accounts)} accounts: {','.join(enabled_accounts)}")
            cmd = [sys.executable, "flow_worker.py", "--accounts", ",".join(enabled_accounts)]
        else:
            cmd = [sys.executable, "flow_worker.py", "--single"]

        print(f"  Launching: {' '.join(cmd)}")
        subprocess.call(cmd)


if __name__ == "__main__":
    main()
