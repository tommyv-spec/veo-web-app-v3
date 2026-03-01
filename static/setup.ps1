# Veo Flow Worker - Quick Setup (Windows)
# Usage: powershell -c "irm https://veo-web-app-v3.onrender.com/api/user-worker/download/setup.ps1 | iex"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Veo Flow Worker - Quick Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$MIN_PYTHON_MAJOR = 3
$MIN_PYTHON_MINOR = 9

# Check Python
$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) {
    $python = Get-Command python3 -ErrorAction SilentlyContinue
}

$needsInstall = $false
$needsUpdate = $false

if (-not $python) {
    $needsInstall = $true
    Write-Host ""
    Write-Host "Python not found." -ForegroundColor Yellow
} else {
    # Check version
    $versionOutput = & $python.Source --version 2>&1
    if ($versionOutput -match "Python (\d+)\.(\d+)\.(\d+)") {
        $pyMajor = [int]$matches[1]
        $pyMinor = [int]$matches[2]
        Write-Host "Python: $($python.Source)"
        Write-Host "Version: $($matches[0])"
        
        if ($pyMajor -lt $MIN_PYTHON_MAJOR -or ($pyMajor -eq $MIN_PYTHON_MAJOR -and $pyMinor -lt $MIN_PYTHON_MINOR)) {
            Write-Host "Python $MIN_PYTHON_MAJOR.$MIN_PYTHON_MINOR+ required. Updating..." -ForegroundColor Yellow
            $needsUpdate = $true
        }
    }
}

if ($needsInstall -or $needsUpdate) {
    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if ($winget) {
        Write-Host "Installing/updating Python via winget..." -ForegroundColor Yellow
        if ($needsUpdate) {
            winget upgrade Python.Python.3.12 --accept-package-agreements --accept-source-agreements 2>$null
            if ($LASTEXITCODE -ne 0) {
                winget install Python.Python.3.12 --accept-package-agreements --accept-source-agreements
            }
        } else {
            winget install Python.Python.3.12 --accept-package-agreements --accept-source-agreements
        }
        # Refresh PATH
        $env:PATH = [System.Environment]::GetEnvironmentVariable("PATH", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("PATH", "User")
        $python = Get-Command python -ErrorAction SilentlyContinue
    }
    
    if (-not $python) {
        Write-Host ""
        Write-Host "Could not install Python automatically." -ForegroundColor Red
        Write-Host "Please download Python from: https://python.org/downloads" -ForegroundColor Yellow
        Write-Host "Make sure to check 'Add Python to PATH' during installation." -ForegroundColor Yellow
        exit 1
    }
    
    Write-Host "Python updated: $(& $python.Source --version)" -ForegroundColor Green
}

$pythonPath = $python.Source
Write-Host "Python: $($pythonPath)"
& $pythonPath --version

# Download setup script
$setupUrl = "https://veo-web-app-v3.onrender.com/api/user-worker/download/setup_worker.py"
$setupPath = "$env:TEMP\veo_setup_worker.py"

Write-Host ""
Write-Host "Downloading setup script..."
Invoke-WebRequest -Uri $setupUrl -OutFile $setupPath -UseBasicParsing

# Run setup - pass token if provided via environment
if ($env:VEO_TOKEN) {
    & $pythonPath $setupPath --token="$($env:VEO_TOKEN)" @args
} else {
    & $pythonPath $setupPath @args
}
