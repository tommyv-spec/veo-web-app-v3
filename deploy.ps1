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

    $Dirty = @(git status --porcelain)
    if ($LASTEXITCODE -ne 0) {
        throw "Deploy blocked: the working tree status is unreadable."
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

    # Working-tree gate, scoped to what this push can actually change.
    #
    # This used to refuse ANY dirty or untracked file. On a shared checkout
    # that blocks a correct deploy whenever another session has unrelated
    # work in flight - 2026-08-03 an untracked static/gemini_video_worker.py
    # plus a .gitignore edit blocked a fix that touched neither file.
    #
    # A push publishes HEAD, so untracked files and edits to files outside
    # the pushed range can never reach Render. The hazard worth blocking is
    # narrower: an UNCOMMITTED edit to a file this deploy DOES ship, because
    # then the commit being proved is not the code on disk. Block exactly
    # that; report the rest and continue.
    $DirtyTracked = @(git diff --name-only HEAD)
    if ($LASTEXITCODE -ne 0) {
        throw "Deploy blocked: the working-tree diff is unreadable."
    }
    $Shipping = @(git diff --name-only origin/main HEAD)
    if ($LASTEXITCODE -ne 0) {
        throw "Deploy blocked: the pushed file range is unreadable."
    }
    $Overlap = @($DirtyTracked | Where-Object { $Shipping -contains $_ })
    if ($Overlap.Count -gt 0) {
        throw ("Deploy blocked: uncommitted edits to files this deploy ships - " +
               "commit or revert them so the pushed commit matches disk: " +
               ($Overlap -join ", "))
    }
    if ($Dirty.Count -gt 0) {
        Write-Host "Working tree is not clean, but nothing dirty is in the pushed range:" -ForegroundColor Yellow
        foreach ($Line in $Dirty) { Write-Host "  $Line" -ForegroundColor DarkGray }
        Write-Host "  (none of these reach Render; only HEAD is published.)" -ForegroundColor DarkGray
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
