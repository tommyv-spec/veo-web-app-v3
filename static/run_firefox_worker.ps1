<#
.SYNOPSIS
  Launch the REAL flow worker on Firefox instead of Chrome, fully isolated.

.DESCRIPTION
  Tests the one hypothesis left standing on the "unusual activity" 403: the
  reCAPTCHA token class (0cA = accepted, HF = refused) is decided by the browser
  ENGINE, not by anything the worker does.

  flow_worker.py already has a Firefox branch (see flow_worker.py ~L24155): when
  BROWSER_MODE is anything other than "stealth" it calls
  p.firefox.launch_persistent_context instead of p.chromium. No code change is
  needed to run it — only environment isolation, which is what this script does.

  ISOLATION — why each variable is set:

    WORKER_BASE_DIR          Moves _BASE (flow_worker.py:4363) into the run dir, so
                             ACCOUNTS[] session folders, goldens and sidecar files
                             never point at the real Chrome worker's state.
    SESSION_FOLDER           Named *_session_firefox. get_golden_folder()
                             (flow_worker.py:3985) maps "session"->"golden", giving
                             flow_golden_firefox — which does not exist, so the
                             STARTUP golden restore (flow_worker.py:24119) is SKIPPED.
                             THIS MATTERS: that restore does rmtree(SESSION_FOLDER)
                             then copies the CHROME golden in. If it ever fired on a
                             Firefox run it would erase the login you just did.
    LAPTOP_PULL_DISABLED     Second lock on the same hazard — stops
                             _maybe_pull_laptop_profile (flow_worker.py:4407) from
                             building a golden out of your real Chrome profile.
    FLOW_API_CAPTURE_PATH    Separate jsonl, so the Firefox token tally is not mixed
                             into the production capture in %TEMP%\veo_shm.
    Run directory            OUTSIDE the git repo. flow_worker.py auto-updates itself
                             on startup (flow_worker.py:25860 os.replace) — pointed at
                             the repo copy it would overwrite a tracked file.

  KNOWN LIMITS of the Firefox path (none are fatal, all are worth watching):
    - The Firefox profile starts EMPTY. Chrome cookies cannot cross engines, so you
      must sign in to Google once in the window. Expected, not a bug.
    - No Patchright stealth. Patchright's patches are chromium-only, so Firefox runs
      on plain Playwright. If Firefox gets refused tokens this is a candidate reason.
    - Window management no-ops. _find_chrome_hwnd / _get_worker_chrome_pids match
      chrome.exe only, so the Firefox window will not be pushed behind others and may
      take focus. Cosmetic.
    - chrome_warmup does nothing useful (it syncs Chrome variations for x-client-data).
      It is wrapped in try/except at flow_worker.py:24187 and cannot abort the run.
    - The Firefox branch hardcodes viewport 1280x500 (flow_worker.py:24160) — shorter
      than the Chrome branch's 720. If the Flow UI is clipped and clicks miss, that is
      the cause, and fixing it means editing flow_worker.py and deploying.

.PARAMETER Token
  Optional. Normally leave it off — the script finds the token by itself, the same
  way send_to_platform.py does (env vars, then ~/veo-worker/.env, then ~/.kaveno/token).
  Passing it here puts a secret in your shell history, so only do that as a last resort.

.PARAMETER WebAppUrl
  API base the worker claims jobs from. Default https://kavenobuilder.com

.PARAMETER RunDir
  Isolated run directory. Default %LOCALAPPDATA%\Temp\flow_firefox_worker

.EXAMPLE
  powershell -File code\static\run_firefox_worker.ps1

.EXAMPLE
  # after the run, tally which token class Firefox minted
  python code\static\test_recaptcha_token_class.py --report `
      --out "$env:LOCALAPPDATA\Temp\flow_firefox_worker\flow_api_capture_firefox.jsonl"
#>

param(
    [string]$Token      = "",
    [string]$WebAppUrl  = "https://kavenobuilder.com",
    [string]$RunDir     = (Join-Path $env:LOCALAPPDATA "Temp\flow_firefox_worker"),
    [int]$Monitor       = 1,
    [string]$WindowSize = ""
)

$ErrorActionPreference = "Stop"

# ---- token discovery ----
# Mirrors resolve_token() in send_to_platform.py (~L128) so the operator never has
# to paste a secret. Deliberately reads the token INSIDE this script rather than
# taking it on the command line: a -Token argument would land in shell history and
# in the process list, and would have to be handled by whoever ran the command.
# Order: -Token > KAVENO_API_TOKEN > VEO_TOKEN > USER_WORKER_TOKEN
#        > ~/veo-worker/.env (the flow worker's own) > ~/.kaveno/token
function Resolve-WorkerToken {
    param([string]$CliToken)

    if ($CliToken) { return @{ Value = $CliToken; Source = "-Token argument" } }

    foreach ($k in @("KAVENO_API_TOKEN", "VEO_TOKEN", "USER_WORKER_TOKEN")) {
        $v = [Environment]::GetEnvironmentVariable($k)
        if ($v -and $v.Trim()) { return @{ Value = $v.Trim(); Source = "env $k" } }
    }

    $envFile = Join-Path $env:USERPROFILE "veo-worker\.env"
    if (Test-Path $envFile) {
        foreach ($line in (Get-Content $envFile -ErrorAction SilentlyContinue)) {
            $key, $sep, $val = $line.Trim() -split '(=)', 2
            if ($line -match '^\s*(KAVENO_API_TOKEN|VEO_TOKEN|USER_WORKER_TOKEN)\s*=\s*(.+)$') {
                $v = $matches[2].Trim().Trim('"').Trim("'")
                if ($v) { return @{ Value = $v; Source = "~\veo-worker\.env" } }
            }
        }
    }

    $savedPath = Join-Path $env:USERPROFILE ".kaveno\token"
    if (Test-Path $savedPath) {
        $v = (Get-Content $savedPath -Raw -ErrorAction SilentlyContinue)
        if ($v -and $v.Trim()) { return @{ Value = $v.Trim(); Source = "~\.kaveno\token" } }
    }

    return $null
}

$resolved = Resolve-WorkerToken -CliToken $Token
if (-not $resolved) {
    Write-Host "ERROR: no worker token found anywhere." -ForegroundColor Red
    Write-Host "  Looked in: KAVENO_API_TOKEN, VEO_TOKEN, USER_WORKER_TOKEN,"
    Write-Host "             ~\veo-worker\.env, ~\.kaveno\token"
    Write-Host "  Fix with either:"
    Write-Host "    python code\send_to_platform.py set-token <token-from-my-worker-page>"
    Write-Host "    `$env:USER_WORKER_TOKEN = '<token>'"
    Write-Host "  Token lives at $WebAppUrl/static/my-worker.html"
    exit 1
}
$Token = $resolved.Value
$tokenSource = $resolved.Source

$repoWorker  = Join-Path $PSScriptRoot "flow_worker.py"
$repoHarness = Join-Path $PSScriptRoot "run_firefox_worker_local.py"
foreach ($f in @($repoWorker, $repoHarness)) {
    if (-not (Test-Path $f)) {
        Write-Host "ERROR: $(Split-Path $f -Leaf) not found in $PSScriptRoot." -ForegroundColor Red
        exit 1
    }
}

# ---- isolated run dir, outside the repo ----
New-Item -ItemType Directory -Force -Path $RunDir | Out-Null
$runWorker  = Join-Path $RunDir "flow_worker.py"
$runHarness = Join-Path $RunDir "run_firefox_worker_local.py"
Copy-Item $repoHarness $runHarness -Force

# The harness bypasses flow_worker's own auto-update (see its docstring), so fetch
# the current worker here instead. Without this the run would silently use the repo
# copy, which lags the deployed build — the first launch here pulled
# bbd017334791 -> 6ff1e949e5a4, so that gap is real, not hypothetical.
$workerUrl = "$WebAppUrl/api/user-worker/download/flow_worker.py"
try {
    Invoke-WebRequest -Uri $workerUrl -Headers @{ Authorization = "Bearer $Token" } `
                      -OutFile $runWorker -UseBasicParsing -TimeoutSec 30
    $hash = (Get-FileHash $runWorker -Algorithm MD5).Hash.ToLower().Substring(0,12)
    Write-Host "  worker   : downloaded live build $hash"
} catch {
    Copy-Item $repoWorker $runWorker -Force
    Write-Host "  worker   : DOWNLOAD FAILED ($($_.Exception.Message))" -ForegroundColor Yellow
    Write-Host "             falling back to the repo copy - may lag production" -ForegroundColor Yellow
}

$sessionDir  = Join-Path $RunDir "flow_session_firefox"
$downloadDir = Join-Path $RunDir "flow_session_firefox_download"
$capturePath = Join-Path $RunDir "flow_api_capture_firefox.jsonl"

# Firefox refuses to open a profile written by a NEWER Firefox — it exits 0 with no
# error, and Playwright reports only "Failed to launch the browser process". That is
# a live hazard here because THREE different builds are in play:
#   patchright -> firefox-1532 (FF 151)  - breaks page.evaluate, unusable
#   playwright -> firefox-1522 (FF 150)  - evaluate works, but Google blocks login
#   camoufox   -> FF 135                 - what this harness actually launches
# Camoufox is the OLDEST, so a profile left behind by either of the others will
# refuse to open. Stamp the profile with its build and reset on mismatch — the
# stamp is what keeps a silent exit-0 from looking like a Firefox bug.
$stampFile = Join-Path $RunDir ".firefox_build_stamp"
$ffBuild = "unknown"
try {
    # Camoufox prints model-download progress to STDOUT on import ("Downloading
    # model definition files...", per-file OK!/Error lines). Capturing all of it
    # produced a garbage multi-line stamp that mismatched on EVERY launch, which
    # reset the profile every time and would have thrown away the login each run.
    # Take only the line that actually looks like a version.
    $cfRaw = python -c "from camoufox.pkgman import installed_verstr; print('VERSTR=' + installed_verstr())" 2>$null
    $cfVer = ($cfRaw | Select-String -Pattern '^VERSTR=(.+)$' | ForEach-Object { $_.Matches[0].Groups[1].Value } | Select-Object -Last 1)
    if ($cfVer) { $ffBuild = "camoufox-$($cfVer.Trim())" }
} catch { }

$priorBuild = if (Test-Path $stampFile) { (Get-Content $stampFile -Raw).Trim() } else { "" }
if ($priorBuild -and $priorBuild -ne $ffBuild -and (Test-Path $sessionDir)) {
    # Direction matters, and getting this wrong destroys the login. Firefox
    # UPGRADES a profile from an older build without complaint; it only refuses
    # one written by a NEWER build. So reset on downgrade only — a blanket reset
    # would have thrown away a hard-won Google sign-in on the 135 -> 152 upgrade.
    $priorMajor = 0; $newMajor = 0
    if ($priorBuild -match '(\d+)\.') { $priorMajor = [int]$matches[1] }
    if ($ffBuild   -match '(\d+)\.') { $newMajor   = [int]$matches[1] }

    if ($newMajor -gt 0 -and $priorMajor -gt 0 -and $newMajor -ge $priorMajor) {
        Write-Host "  profile  : built by $priorBuild, launching $ffBuild - KEEPING (forward upgrade)"
    } else {
        Write-Host "  profile  : built by $priorBuild, launching $ffBuild - RESETTING (downgrade)" -ForegroundColor Yellow
        Write-Host "             (a newer-Firefox profile makes FF exit 0 with no error)" -ForegroundColor Yellow
        Remove-Item $sessionDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}
Set-Content -Path $stampFile -Value $ffBuild -Encoding utf8

New-Item -ItemType Directory -Force -Path $sessionDir  | Out-Null
New-Item -ItemType Directory -Force -Path $downloadDir | Out-Null

# Refuse to run if the golden name this session folder derives to actually exists —
# that is the only condition under which the STARTUP restore could wipe the login.
$derivedGolden = Join-Path $RunDir "flow_golden_firefox"
if (Test-Path $derivedGolden) {
    # The startup restore (flow_worker.py:24119) rmtree's the session and copies the
    # golden in. That is CORRECT once the golden is a Firefox golden the worker built
    # from a signed-in Firefox session — it is how the login survives a restart. It is
    # only catastrophic if the golden is a CHROME profile, which Firefox cannot read:
    # the session would be replaced by files Firefox ignores and the login would be
    # gone. So identify the golden by what is inside it, not by whether it exists.
    $isFirefox = (Test-Path (Join-Path $derivedGolden "cookies.sqlite")) -or
                 (Test-Path (Join-Path $derivedGolden "prefs.js"))
    $isChrome  = (Test-Path (Join-Path $derivedGolden "Local State")) -or
                 (Test-Path (Join-Path $derivedGolden "Default"))

    if ($isChrome -and -not $isFirefox) {
        Write-Host "ERROR: $derivedGolden is a CHROME profile." -ForegroundColor Red
        Write-Host "  Restoring it would replace the Firefox session with files Firefox"
        Write-Host "  cannot read, erasing the login. Delete or rename it, then re-run."
        exit 1
    }
    if ($isFirefox) {
        Write-Host "  golden   : firefox golden present - login will be restored from it"
    } else {
        Write-Host "  golden   : present but unrecognised - leaving it to the worker" -ForegroundColor Yellow
    }
}

# ---- environment ----
$env:BROWSER_MODE            = "firefox"   # any value except "stealth" takes the Firefox branch
$env:WORKER_BASE_DIR         = $RunDir
$env:SESSION_FOLDER          = $sessionDir
$env:DOWNLOAD_SESSION_FOLDER = $downloadDir
$env:LAPTOP_PULL_DISABLED    = "1"
$env:FLOW_API_CAPTURE        = "on"
$env:FLOW_API_CAPTURE_PATH   = $capturePath
$env:USER_WORKER_TOKEN       = $Token
$env:WEB_APP_URL             = $WebAppUrl
# WORKER_MODE defaults to "admin" (flow_worker.py:3848), which routes every poll at
# /api/local-worker/* — the GLOBAL admin endpoints — and authenticates with the shared
# LOCAL_WORKER_API_KEY rather than the personal token. A user's own jobs are scoped to
# /api/user-worker/*, so an admin-mode worker cannot see them at all: it polls forever
# reporting "No pending jobs or redos" while the job sits claimable. Setting the token
# alone is NOT enough — the mode is what selects the URL prefix and the auth key.
$env:WORKER_MODE             = "user"

# ---- window sizing ----
# flow_worker pins the Firefox branch to viewport 1280x500 (flow_worker.py:24160),
# which clips the Flow UI on any real display. Size to the target monitor's WORKING
# area (bounds minus taskbar) so the whole window is reachable, and let the harness
# unpin the viewport so page content follows the window.
if (-not $WindowSize) {
    try {
        Add-Type -AssemblyName System.Windows.Forms
        $screens = [System.Windows.Forms.Screen]::AllScreens
        $idx = [Math]::Max(0, [Math]::Min($Monitor - 1, $screens.Count - 1))
        $wa = $screens[$idx].WorkingArea
        $WindowSize = "$($wa.Width)x$($wa.Height)"
        $monLabel = "monitor $($idx + 1) of $($screens.Count)"
    } catch {
        $WindowSize = "1280x720"
        $monLabel = "detection failed - fallback"
    }
} else {
    $monLabel = "explicit -WindowSize"
}
$env:FIREFOX_WINDOW = $WindowSize

Write-Host ""
Write-Host "============================================================"
Write-Host " FLOW WORKER - FIREFOX ENGINE TEST"
Write-Host "============================================================"
Write-Host "  run dir  : $RunDir"
Write-Host "  session  : $sessionDir"
Write-Host "  capture  : $capturePath"
Write-Host "  api      : $WebAppUrl"
Write-Host "  token    : found via $tokenSource (len $($Token.Length))"
Write-Host "  window   : $WindowSize ($monLabel)"
if (Test-Path $derivedGolden) {
    Write-Host "  golden   : present - the worker restores the login from it"
    Write-Host ""
    Write-Host "  Already signed in. Queue jobs in the web app; the worker claims them."
} else {
    Write-Host "  golden   : none yet - one is built after the first sign-in"
    Write-Host ""
    Write-Host "  1. a Firefox window opens - sign in to Google (profile is empty)"
    Write-Host "  2. queue jobs in the web app as usual; the worker claims them"
}
Write-Host "  Watch for [flow-api-capture] lines - those carry the reCAPTCHA token."
Write-Host "  Ctrl-C to stop, then run the --report command to tally."
Write-Host "============================================================"
Write-Host ""

Push-Location $RunDir
try {
    # Via the harness, NOT flow_worker.py directly: Patchright's chromium-only
    # patches break page.evaluate on Firefox, which strands the worker on the
    # Flow landing page. See run_firefox_worker_local.py for the measurement.
    python $runHarness
}
finally {
    Pop-Location
    Write-Host ""
    Write-Host "Tally which reCAPTCHA token class Firefox minted:" -ForegroundColor Cyan
    Write-Host "  python `"$(Join-Path $PSScriptRoot 'test_recaptcha_token_class.py')`" --report --out `"$capturePath`""
}
