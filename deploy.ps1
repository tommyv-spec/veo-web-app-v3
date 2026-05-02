$ErrorActionPreference = "Stop"
$VpsIp = "185.254.206.22"
$Message = Split-Path -Leaf (Get-Location)

Write-Host "`n=== Deploying $Message ===" -ForegroundColor Cyan

# Push to Render FIRST
Write-Host "Pushing to Render..." -ForegroundColor Yellow
git init
git add -A
git commit -m $Message
git branch -M main
git remote add origin https://github.com/tommyv-spec/veo-web-app-v3.git 2>$null
git push -u origin main --force
Write-Host "Render updated" -ForegroundColor Green

# Upload to VPS LAST (overwrites whatever Render served)
Write-Host "Uploading to VPS (force overwrite)..." -ForegroundColor Yellow
scp -o StrictHostKeyChecking=no "static/flow_worker.py" "root@${VpsIp}:/root/veo-worker/flow_worker.py"
scp -o StrictHostKeyChecking=no "static/setup_worker.py" "root@${VpsIp}:/root/veo-worker/setup_worker.py"

# Stamp version on VPS so we can always verify
$hash = (git rev-parse --short HEAD 2>$null) ?? "unknown"
ssh -o StrictHostKeyChecking=no "root@${VpsIp}" "echo '$Message ($hash) deployed at $(Get-Date -Format 'yyyy-MM-dd HH:mm')' > /root/veo-worker/.deploy_version"
Write-Host "VPS updated and stamped: $hash" -ForegroundColor Green

Write-Host "`n=== Done! ===" -ForegroundColor Cyan
Write-Host "Verify on VPS: cat /root/veo-worker/.deploy_version"
