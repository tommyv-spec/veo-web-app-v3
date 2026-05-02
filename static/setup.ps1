# KavenoBuilder Flow Worker - Quick Setup (Windows)
# Usage: powershell -c "irm https://kavenobuilder.com/api/user-worker/download/setup.ps1 | iex"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  KavenoBuilder Flow Worker - Quick Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Find a real Windows Python (not MSYS2/Cygwin/MinGW which lack pip)
function Find-WindowsPython {
    $badPaths = @("msys", "cygwin", "mingw", "ucrt64", "clang64")

    # 1. Try 'py' launcher first - most reliable on Windows
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        $pyPath = $py.Source
        $lower = $pyPath.ToLower()
        $isBad = $false
        foreach ($bad in $badPaths) { if ($lower -contains $bad) { $isBad = $true } }
        if (-not $isBad) {
            # Verify pip works
            $pipTest = & $pyPath -m pip --version 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Host "  Found: py launcher -> $pyPath" -ForegroundColor Green
                return $pyPath
            }
        }
    }

    # 2. Check all python/python3 in PATH, skip bad ones
    foreach ($name in @("python", "python3")) {
        $cmds = Get-Command $name -ErrorAction SilentlyContinue -All
        if ($cmds) {
            foreach ($cmd in $cmds) {
                $p = $cmd.Source
                $lower = $p.ToLower().Replace("\", "/")
                $isBad = $false
                foreach ($bad in $badPaths) { if ($lower -like "*$bad*") { $isBad = $true } }
                if ($isBad) {
                    Write-Host "  Skipping MSYS2/Cygwin Python: $p" -ForegroundColor Yellow
                    continue
                }
                $pipTest = & $p -m pip --version 2>&1
                if ($LASTEXITCODE -eq 0) {
                    Write-Host "  Found: $p" -ForegroundColor Green
                    return $p
                }
            }
        }
    }

    # 3. Check common Windows install locations
    $locations = @(
        "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python310\python.exe",
        "$env:ProgramFiles\Python313\python.exe",
        "$env:ProgramFiles\Python312\python.exe",
        "$env:ProgramFiles\Python311\python.exe"
    )
    foreach ($loc in $locations) {
        if (Test-Path $loc) {
            $pipTest = & $loc -m pip --version 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Host "  Found: $loc" -ForegroundColor Green
                return $loc
            }
        }
    }

    return $null
}

Write-Host ""
Write-Host "Looking for Python (skipping MSYS2/Cygwin)..."
$pythonPath = Find-WindowsPython

if (-not $pythonPath) {
    # Try to install via winget
    Write-Host "No suitable Python found. Installing via winget..." -ForegroundColor Yellow
    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if ($winget) {
        winget install Python.Python.3.12 --accept-package-agreements --accept-source-agreements
        $env:PATH = [System.Environment]::GetEnvironmentVariable("PATH","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("PATH","User")
        $pythonPath = Find-WindowsPython
    }
}

if (-not $pythonPath) {
    Write-Host ""
    Write-Host "Could not find a suitable Python installation." -ForegroundColor Red
    Write-Host "Please download Python from: https://python.org/downloads" -ForegroundColor Yellow
    Write-Host "Make sure to check 'Add Python to PATH' during installation." -ForegroundColor Yellow
    exit 1
}

Write-Host "Using Python launcher: $pythonPath" -ForegroundColor Green
& $pythonPath --version

# If py.EXE launcher, resolve the actual python.exe it delegates to
# This ensures pip install and script execution use the EXACT same interpreter
if ($pythonPath -like "*\py.EXE" -or $pythonPath -like "*\py.exe") {
    $resolvedPython = & $pythonPath -c "import sys; print(sys.executable)" 2>$null
    if ($resolvedPython -and (Test-Path $resolvedPython)) {
        Write-Host "Resolved actual Python: $resolvedPython" -ForegroundColor Green
        $pythonPath = $resolvedPython
    }
}
Write-Host "Python executable: $pythonPath" -ForegroundColor Green

# Install required packages BEFORE running setup_worker.py
# ROOT CAUSE of the previous hang: pip's dependency resolver for patchright takes
# 3-10 minutes even when packages are already installed, because it resolves the
# entire transitive dependency tree (patchright → playwright internals → greenlet,
# pyee, etc.) before deciding nothing needs to change. It prints zero output during
# resolution. The only fix is to NOT call pip when packages are already importable.
Write-Host ""
Write-Host "Checking required packages..."
$importCheck = & $pythonPath -c "import patchright; import requests; print('ok')" 2>$null
if ($importCheck -eq "ok") {
    Write-Host "  patchright + requests already installed" -ForegroundColor Green
} else {
    Write-Host "  Installing patchright + requests (first run, 1-3 min)..." -ForegroundColor Yellow
    # --no-input: prevents pip from waiting on keyring/prompts in non-interactive context
    # Start-Process: writes directly to console, not through PowerShell pipeline
    $pipProc = Start-Process -FilePath $pythonPath -ArgumentList "-m","pip","install","--no-input","patchright","requests" -NoNewWindow -Wait -PassThru
    if ($pipProc.ExitCode -ne 0) {
        Write-Host "  Retrying with --user flag..." -ForegroundColor Yellow
        Start-Process -FilePath $pythonPath -ArgumentList "-m","pip","install","--no-input","--user","patchright","requests" -NoNewWindow -Wait
    }
    # Verify install succeeded
    $verify = & $pythonPath -c "import patchright; import requests; print('ok')" 2>$null
    if ($verify -eq "ok") {
        Write-Host "  Done" -ForegroundColor Green
    } else {
        Write-Host "  WARNING: packages may not have installed correctly" -ForegroundColor Red
    }
}

# Download setup script (force fresh download)
$setupUrl = "https://kavenobuilder.com/api/user-worker/download/setup_worker.py"
$setupPath = "$env:TEMP\veo_setup_worker.py"

Write-Host ""
Write-Host "Downloading setup script..."
Remove-Item $setupPath -ErrorAction SilentlyContinue
Invoke-WebRequest -Uri $setupUrl -OutFile $setupPath -UseBasicParsing

# Run setup using the SAME Python we just used to install packages
# Pass --relaunched so setup_worker.py skips its own install step
if ($env:VEO_TOKEN) {
    & $pythonPath $setupPath --token="$($env:VEO_TOKEN)" --relaunched @args
} else {
    & $pythonPath $setupPath --relaunched @args
}
