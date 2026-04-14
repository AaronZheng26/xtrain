$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (-not (Test-Path ".venv")) {
  python -m venv .venv
}

& ".\.venv\Scripts\python.exe" -m pip install -r ".\backend\requirements.txt"
& ".\.venv\Scripts\python.exe" -m uvicorn app.main:app --reload --app-dir ".\backend"
