$ErrorActionPreference = "Stop"

# Safe Render deploy entry point. This script never creates a commit, renames a
# branch, force-pushes, or uploads local-worker files. Prepare and review the
# commit first; this command only proves and publishes that exact commit.
$Repo = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $Repo

try {
    $Inside = git rev-parse --is-inside-work-tree 2>$null
    if ($LASTEXITCODE -ne 0 -or $Inside -ne "true") {
        throw "deploy.ps1 must run from the code repository."
    }

    $Branch = git branch --show-current
    if ($LASTEXITCODE -ne 0 -or $Branch -ne "main") {
        throw "Deploy blocked: use a clean main checkout. Current branch: $Branch"
    }

    $Dirty = git status --porcelain
    if ($LASTEXITCODE -ne 0 -or $Dirty) {
        throw "Deploy blocked: the main checkout has uncommitted or untracked files."
    }

    Write-Host "Refreshing protected main..." -ForegroundColor Yellow
    git fetch origin main
    if ($LASTEXITCODE -ne 0) {
        throw "Deploy blocked: origin/main could not be refreshed."
    }

    git merge-base --is-ancestor origin/main HEAD
    if ($LASTEXITCODE -ne 0) {
        throw "Deploy blocked: HEAD does not contain the current origin/main. Rebuild the change on fresh main."
    }

    $HeadSha = git rev-parse HEAD
    if ($LASTEXITCODE -ne 0 -or -not $HeadSha) {
        throw "Deploy blocked: HEAD is unreadable."
    }

    Write-Host "Running deploy safety gate for $($HeadSha.Substring(0, 7))..." -ForegroundColor Yellow
    python check_deploy_safety.py --ref $HeadSha --main origin/main
    if ($LASTEXITCODE -ne 0) {
        throw "Deploy blocked by check_deploy_safety.py."
    }

    Write-Host "Pushing the reviewed commit to Render..." -ForegroundColor Yellow
    git push origin HEAD:main
    if ($LASTEXITCODE -ne 0) {
        throw "Push failed. Production was not updated."
    }

    python verify_deploy.py $HeadSha
    if ($LASTEXITCODE -ne 0) {
        throw "Push completed, but the live deploy was not confirmed healthy. Check Render."
    }

    Write-Host "Render deploy confirmed live and healthy: $($HeadSha.Substring(0, 7))" -ForegroundColor Green
    Write-Host "Local and VPS workers were not changed; deploy them through their separate runbook."
}
finally {
    Pop-Location
}
