#!/usr/bin/env python3
"""
Veo Flow Worker - Setup Script
===============================
One-time setup to run your own Flow worker on your machine.

Usage:
    python setup_worker.py
    python setup_worker.py --token YOUR_TOKEN   (skip interactive prompt)
    python setup_worker.py --update             (update worker script only)
    python setup_worker.py --reselect-profile   (pick a different Chrome profile)
"""

import json
import os
import platform
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================

WEB_APP_URL = "https://veo-web-app-v3.onrender.com"
WORKER_DOWNLOAD_URL = f"{WEB_APP_URL}/api/user-worker/download/flow_worker.py"
WORKER_DIR = Path.home() / "veo-worker"
MIN_PYTHON = (3, 9)

# Folders to skip when copying Chrome profile (saves ~90% of space)
SKIP_FOLDERS = {
    'Cache', 'Code Cache', 'GPUCache', 'GrShaderCache', 'ShaderCache',
    'DawnCache', 'DawnWebGPUBlobCache', 'Service Worker', 'blob_storage',
    'IndexedDB', 'File System', 'Storage', 'databases', 'Extensions',
    'Extension State', 'Extension Rules', 'Extension Scripts',
    'Local Extension Settings', 'Sync Extension Settings', 'WebStorage',
    'Platform Notifications', 'BudgetDatabase', 'Download Service',
    'Thumbnails', 'Visited Links', 'Top Sites', 'SafetyTips',
    'optimization_guide_prediction_model_downloads',
}


# ============================================================
# CHROME PROFILE DETECTION
# ============================================================

def get_chrome_user_data_dir():
    """Find Chrome's User Data directory."""
    system = platform.system()
    home = Path.home()
    candidates = {
        "Windows": [
            home / "AppData/Local/Google/Chrome/User Data",
            home / "AppData/Local/Google/Chrome Beta/User Data",
        ],
        "Darwin": [
            home / "Library/Application Support/Google/Chrome",
            home / "Library/Application Support/Google/Chrome Beta",
        ],
        "Linux": [
            home / ".config/google-chrome",
            home / ".config/google-chrome-beta",
            home / ".config/chromium",
        ],
    }
    for path in candidates.get(system, []):
        if path.exists() and (path / "Local State").exists():
            return path
    return None


def detect_chrome_profiles(user_data_dir):
    """Read Local State to find all Chrome profiles with their Google accounts."""
    local_state_path = user_data_dir / "Local State"
    if not local_state_path.exists():
        return []

    try:
        with open(local_state_path, "r", encoding="utf-8") as f:
            local_state = json.load(f)
    except (json.JSONDecodeError, OSError):
        return []

    info_cache = local_state.get("profile", {}).get("info_cache", {})
    profiles = []

    for folder_name, info in info_cache.items():
        profile_path = user_data_dir / folder_name
        if not profile_path.exists():
            continue

        email = info.get("user_name", "") or ""
        gaia_name = info.get("gaia_name", "") or ""
        display_name = info.get("name", folder_name) or folder_name

        # If no email in Local State, try Preferences
        if not email:
            email = _get_email_from_preferences(profile_path)

        has_session = _check_google_session(profile_path)

        profiles.append({
            "folder": folder_name,
            "path": profile_path,
            "name": display_name,
            "email": email,
            "gaia_name": gaia_name,
            "has_google_session": has_session,
        })

    profiles.sort(key=lambda p: (not p["has_google_session"], p["folder"]))
    return profiles


def _get_email_from_preferences(profile_path):
    prefs_path = profile_path / "Preferences"
    if not prefs_path.exists():
        return ""
    try:
        with open(prefs_path, "r", encoding="utf-8") as f:
            prefs = json.load(f)
        for acct in prefs.get("account_info", []):
            if acct.get("email"):
                return acct["email"]
        return prefs.get("google", {}).get("services", {}).get("signin", {}).get("email", "")
    except:
        return ""


def _check_google_session(profile_path):
    """Check if profile has active Google auth cookies."""
    for cookies_rel in ["Cookies", "Network/Cookies"]:
        cookies_path = profile_path / cookies_rel
        if cookies_path.exists():
            try:
                tmp = Path(tempfile.mktemp(suffix=".db"))
                shutil.copy2(cookies_path, tmp)
                conn = sqlite3.connect(str(tmp))
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM cookies 
                    WHERE host_key LIKE '%google.com%' 
                    AND name IN ('SID', 'SSID', 'HSID', '__Secure-1PSID')
                """)
                count = cursor.fetchone()[0]
                conn.close()
                tmp.unlink(missing_ok=True)
                return count > 0
            except:
                pass
    return False


def select_chrome_profile(profiles):
    """Interactive profile selection menu."""
    if not profiles:
        return None

    if len(profiles) == 1:
        p = profiles[0]
        email_str = f" ({p['email']})" if p['email'] else ""
        print(f"\n  Found one Chrome profile: {p['name']}{email_str}")
        confirm = input("  Use this profile? (Y/n): ").strip().lower()
        return p if confirm in ("", "y", "yes") else None

    print("\n" + "=" * 55)
    print("  Chrome Profiles")
    print("=" * 55)

    for i, p in enumerate(profiles, 1):
        icon = "+" if p["has_google_session"] else "-"
        email_str = p["email"] or "(no Google account)"

        print(f"\n  {i}. [{icon}] {p['name']}")
        print(f"      {email_str}")
        if p["has_google_session"]:
            print(f"      Google session active")
        else:
            print(f"      Will need to login manually")

    print(f"\n  0. Skip (login manually when worker starts)")
    print("=" * 55)

    while True:
        choice = input(f"\n  Select profile [1-{len(profiles)}] or 0: ").strip()
        if choice == "0":
            return None
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(profiles):
                return profiles[idx]
        except ValueError:
            pass
        print(f"  Enter a number between 0 and {len(profiles)}")


def copy_chrome_profile(user_data_dir, profile, dest_dir):
    """Copy a Chrome profile for worker use, skipping caches."""
    profile_src = profile["path"]
    profile_dest = dest_dir / "Default"

    print(f"\n  Copying profile: {profile['name']}...")
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Copy Local State
    local_state_src = user_data_dir / "Local State"
    if local_state_src.exists():
        shutil.copy2(local_state_src, dest_dir / "Local State")

    # Copy profile folder (skip large dirs)
    copied = 0
    profile_dest.mkdir(parents=True, exist_ok=True)

    def _copy(src, dst):
        nonlocal copied
        dst.mkdir(parents=True, exist_ok=True)
        for item in src.iterdir():
            if item.name in SKIP_FOLDERS:
                continue
            dest_item = dst / item.name
            try:
                if item.is_dir():
                    _copy(item, dest_item)
                else:
                    shutil.copy2(item, dest_item)
                    copied += 1
                    if copied % 200 == 0:
                        print(f"    {copied} files...", end="\r", flush=True)
            except (OSError, PermissionError):
                pass

    _copy(profile_src, profile_dest)
    print(f"  Copied {copied} files                     ")
    return dest_dir


# ============================================================
# SETUP FLOW
# ============================================================

def check_python():
    if sys.version_info < MIN_PYTHON:
        print(f"Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required (you have {sys.version})")
        print("Download from: https://python.org/downloads")
        sys.exit(1)
    print(f"  Python {sys.version_info.major}.{sys.version_info.minor} OK")


def install_dependencies():
    print("\n  Installing/updating Python packages...")
    
    # Check what's already installed
    missing = []
    for pkg in ["playwright", "requests"]:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)
    
    if not missing:
        print("  ✓ Python packages already installed")
    else:
        print(f"  Installing: {', '.join(missing)}")
        pip_cmd = [sys.executable, "-m", "pip", "install"] + missing + ["--quiet"]
        if sys.version_info >= (3, 11):
            pip_cmd.append("--break-system-packages")
        try:
            subprocess.check_call(pip_cmd)
        except subprocess.CalledProcessError:
            pip_cmd_fallback = [sys.executable, "-m", "pip", "install"] + missing + ["--quiet", "--user"]
            subprocess.check_call(pip_cmd_fallback)
        print("  ✓ Python packages OK")

    # Verify Chrome is available (we use system Chrome, not Playwright's Chromium)
    chrome_paths = [
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
    ]
    chrome_found = any(os.path.exists(p) for p in chrome_paths)
    if chrome_found:
        print("  ✓ Google Chrome found")
    else:
        print("  ⚠ Google Chrome not detected — please install it from https://google.com/chrome")
        print("    The worker requires Google Chrome (not Chromium)")


def download_worker():
    import requests as req
    print(f"\n  Downloading worker script...")
    try:
        resp = req.get(WORKER_DOWNLOAD_URL, timeout=30)
        resp.raise_for_status()
        worker_path = WORKER_DIR / "flow_worker.py"
        worker_path.write_bytes(resp.content)
        print(f"  Worker saved: {worker_path}")
    except Exception as e:
        print(f"  Could not download worker: {e}")
        print(f"  You can manually place flow_worker.py in {WORKER_DIR}")


def get_token(args_token=None):
    if args_token:
        return args_token
    print("\n  Get your worker token from the web app:")
    print(f"  {WEB_APP_URL} → Settings → My Worker → Generate Token")
    print()
    token = input("  Paste your worker token: ").strip()
    if not token:
        print("  Token required. Exiting.")
        sys.exit(1)
    return token


def setup_chrome(force_reselect=False, account_num=1):
    """Set up Chrome profile for a given account number."""
    if account_num == 1:
        session_dir = WORKER_DIR / "chrome-session"
        download_dir = WORKER_DIR / "chrome-download"
    else:
        session_dir = WORKER_DIR / f"chrome-session-{account_num}"
        download_dir = WORKER_DIR / f"chrome-download-{account_num}"

    # Already set up?
    if not force_reselect and session_dir.exists() and (session_dir / "Default").exists():
        print(f"\n  Chrome session {account_num} already exists")
        reuse = input("  Use existing session? (Y/n): ").strip().lower()
        if reuse in ("", "y", "yes"):
            download_dir.mkdir(parents=True, exist_ok=True)
            return session_dir, download_dir
        print("  Removing old sessions...")
        shutil.rmtree(session_dir, ignore_errors=True)
        shutil.rmtree(download_dir, ignore_errors=True)

    # Find Chrome
    user_data_dir = get_chrome_user_data_dir()
    if not user_data_dir:
        print("\n  Chrome not found. You'll login manually when the worker starts.")
        session_dir.mkdir(parents=True, exist_ok=True)
        download_dir.mkdir(parents=True, exist_ok=True)
        return session_dir, download_dir

    print(f"\n  Chrome found: {user_data_dir}")

    # Detect and select profile
    profiles = detect_chrome_profiles(user_data_dir)
    if not profiles:
        print("  No profiles found. Creating fresh session.")
        session_dir.mkdir(parents=True, exist_ok=True)
        download_dir.mkdir(parents=True, exist_ok=True)
        return session_dir, download_dir

    if account_num > 1:
        print(f"\n  Select Chrome profile for Account {account_num}:")
    selected = select_chrome_profile(profiles)
    if not selected:
        print("  Skipped. You'll login manually when the worker starts.")
        session_dir.mkdir(parents=True, exist_ok=True)
        download_dir.mkdir(parents=True, exist_ok=True)
        return session_dir, download_dir

    # Copy profile for both browsers
    print(f"\n  Setting up submit browser (account {account_num})...")
    copy_chrome_profile(user_data_dir, selected, session_dir)
    print(f"  Setting up download browser (account {account_num})...")
    copy_chrome_profile(user_data_dir, selected, download_dir)

    email_str = f" ({selected['email']})" if selected['email'] else ""
    print(f"\n  Account {account_num} ready: {selected['name']}{email_str}")
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


def write_config(token, accounts):
    """Write .env config supporting single or multi-account setup."""
    env_path = WORKER_DIR / ".env"
    
    if len(accounts) == 1:
        # Single account — simple config
        acc = accounts[0]
        env_path.write_text(
            f"WORKER_MODE=user\n"
            f"USER_WORKER_TOKEN={token}\n"
            f"WEB_APP_URL={WEB_APP_URL}\n"
            f"SESSION_FOLDER={acc['session_dir']}\n"
            f"DOWNLOAD_SESSION_FOLDER={acc['download_dir']}\n"
            f"BROWSER_MODE=stealth\n"
            f"MULTI_ACCOUNT=false\n"
            f"PROXY_TYPE=none\n"
        )
    else:
        # Multi-account config
        lines = [
            f"WORKER_MODE=user",
            f"USER_WORKER_TOKEN={token}",
            f"WEB_APP_URL={WEB_APP_URL}",
            f"BROWSER_MODE=stealth",
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
    if platform.system() == "Windows":
        # .bat file
        bat = WORKER_DIR / "start_worker.bat"
        bat.write_text(
            f'@echo off\n'
            f'cd /d "{WORKER_DIR}"\n'
            f'echo Loading config...\n'
            f'for /f "usebackq tokens=1,* delims==" %%a in (".env") do set "%%a=%%b"\n'
            f'echo Starting Veo Flow Worker...\n'
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
            f'Write-Host "Starting Veo Flow Worker..."\n'
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
            f'echo "Starting Veo Flow Worker..."\n'
            f'python3 flow_worker.py --single\n'
        )
        sh.chmod(0o755)
        print(f"  Launch script: {sh}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Veo Flow Worker Setup")
    parser.add_argument("--token", type=str, help="Worker token (skip prompt)")
    parser.add_argument("--update", action="store_true", help="Update worker script only")
    parser.add_argument("--reselect-profile", action="store_true", help="Pick a different Chrome profile")
    parser.add_argument("--add-account", action="store_true", help="Add another Google account")
    args = parser.parse_args()

    print("=" * 55)
    print("  Veo Flow Worker Setup")
    print("=" * 55)

    if args.update:
        print("\nUpdating worker script...")
        WORKER_DIR.mkdir(exist_ok=True)
        download_worker()
        print("\nDone!")
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
    elif args.reselect_profile:
        accounts = [{'num': 1, 'session_dir': s, 'download_dir': d} 
                     for s, d in [setup_chrome(force_reselect=True)]]
    else:
        accounts = setup_multiple_accounts()

    # Step 6: Token and config
    print("\n[6/6] Configuration...")
    token = get_token(args.token)
    write_config(token, accounts)
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
        subprocess.call([sys.executable, "flow_worker.py", "--single"])


if __name__ == "__main__":
    main()
