# Ai Lubricant Desktop build script (Windows / PowerShell)
#
#   powershell -ExecutionPolicy Bypass -File desktop\build.ps1
#
# Output: dist\AiLubricant\AiLubricant.exe  (onedir, with _internal dependency folder)
# The target machine needs no Python, but does need a reachable external
# PostgreSQL and Redis (>= 6.0).
#
# NOTE: keep every string in this file ASCII-only. Windows PowerShell 5.1
# decodes BOM-less files as ANSI, which corrupts non-ASCII literals and breaks
# parsing before the script ever runs.

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Write-Host "==> [1/4] Building frontend (pnpm build)" -ForegroundColor Cyan
# The portal frontend lives in the user-frontend submodule (AGPL-licensed
# vendored frontend; see its NOTICE). It must be initialized first:
#   git submodule update --init user-frontend
if (Test-Path "user-frontend\package.json") {
    if (Get-Command pnpm -ErrorAction SilentlyContinue) {
        pnpm --dir user-frontend install --frozen-lockfile
        pnpm --dir user-frontend build
    } else {
        Write-Warning "pnpm not found; skipping frontend build and reusing existing user-frontend\dist"
    }
} else {
    Write-Warning "user-frontend not found; run: git submodule update --init user-frontend"
}
if (-not (Test-Path "user-frontend\dist\index.html")) {
    throw "user-frontend\dist\index.html is missing - frontend artifacts required for packaging"
}

Write-Host "==> [2/4] Selecting Python interpreter" -ForegroundColor Cyan
$py = if (Test-Path ".venv\Scripts\python.exe") { ".venv\Scripts\python.exe" } else { "python" }
Write-Host "    using $py"

Write-Host "==> [3/4] Installing build dependencies" -ForegroundColor Cyan
& $py -m pip install -r requirements.txt -r requirements-desktop.txt

Write-Host "==> [4/4] Running PyInstaller" -ForegroundColor Cyan
& $py -m PyInstaller desktop\AiLubricant.spec --noconfirm

Write-Host ""
Write-Host "Done: dist\AiLubricant\AiLubricant.exe" -ForegroundColor Green
