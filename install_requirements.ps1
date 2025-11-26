# PowerShell script to install Python packages from requirements.txt

Write-Host "Installing Python packages from requirements.txt..." -ForegroundColor Cyan

# Check if requirements.txt exists
if (-not (Test-Path "requirements.txt")) {
    Write-Host "❌ Error: requirements.txt not found" -ForegroundColor Red
    exit 1
}

# Install packages
pip install -r requirements.txt

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Installation complete!" -ForegroundColor Green
} else {
    Write-Host "❌ Installation failed" -ForegroundColor Red
    exit $LASTEXITCODE
}
