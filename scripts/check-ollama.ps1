$ErrorActionPreference = "Stop"

try {
  $response = Invoke-RestMethod -Uri "http://127.0.0.1:11434/api/tags" -Method Get -TimeoutSec 3
  Write-Host "Ollama is reachable."
  $response | ConvertTo-Json -Depth 4
} catch {
  Write-Error "Ollama is not reachable on http://127.0.0.1:11434"
}
