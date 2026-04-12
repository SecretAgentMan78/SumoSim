# push.ps1 — SumoSim GitHub sync helper
# Usage:  .\push.ps1 "your commit message"
#         .\push.ps1            (prompts for message)
#
# Run this from your sumosim project root after local verification.

param(
    [string]$Message = ""
)

# ── Config ────────────────────────────────────────────────────────────────────
$Branch = "main"   # change to "master" if that's your default branch
# ─────────────────────────────────────────────────────────────────────────────

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step($text) {
    Write-Host "`n>>> $text" -ForegroundColor Cyan
}

function Write-OK($text) {
    Write-Host "    OK  $text" -ForegroundColor Green
}

function Write-Fail($text) {
    Write-Host "    ERR $text" -ForegroundColor Red
}

# ── 1. Confirm we're in the right directory ───────────────────────────────────
Write-Step "Checking project directory"
if (-not (Test-Path "main.py") -and -not (Test-Path "sumosim")) {
    Write-Fail "This doesn't look like the SumoSim root. Run from the project root."
    exit 1
}
Write-OK (Get-Location)

# ── 2. Show current git status ────────────────────────────────────────────────
Write-Step "Git status"
git status --short
if ($LASTEXITCODE -ne 0) {
    Write-Fail "git status failed. Is this a git repo?"
    exit 1
}

# ── 3. Prompt for commit message if not supplied ──────────────────────────────
if ($Message -eq "") {
    Write-Host ""
    $Message = Read-Host "Commit message"
}
if ($Message.Trim() -eq "") {
    Write-Fail "Commit message cannot be empty."
    exit 1
}

# ── 4. Stage all changes ──────────────────────────────────────────────────────
Write-Step "Staging changes"
git add -A
Write-OK "All changes staged"

# ── 5. Show what's about to be committed ─────────────────────────────────────
Write-Host ""
git diff --cached --stat
Write-Host ""

# ── 6. Confirm before committing ─────────────────────────────────────────────
$confirm = Read-Host "Commit and push the above? (y/N)"
if ($confirm -notmatch "^[Yy]$") {
    Write-Host "Aborted. Changes are staged but not committed." -ForegroundColor Yellow
    Write-Host "Run 'git reset HEAD' to unstage." -ForegroundColor Yellow
    exit 0
}

# ── 7. Commit ─────────────────────────────────────────────────────────────────
Write-Step "Committing"
git commit -m $Message
if ($LASTEXITCODE -ne 0) {
    Write-Fail "Commit failed."
    exit 1
}
Write-OK "Committed: $Message"

# ── 8. Push ──────────────────────────────────────────────────────────────────
Write-Step "Pushing to origin/$Branch"
git push origin $Branch
if ($LASTEXITCODE -ne 0) {
    Write-Fail "Push failed. Check your PAT and remote URL."
    exit 1
}
Write-OK "Pushed successfully"

# ── 9. Summary ───────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "Done! Latest commit:" -ForegroundColor Green
git log --oneline -1
