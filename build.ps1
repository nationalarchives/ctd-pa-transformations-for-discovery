# Enable strict error handling
$ErrorActionPreference = "Stop"

# Path to your venv's Python executable
$pythonExe = ".\ctd_pa_lambda_discovery_py312\Scripts\python.exe"

# Validate venv Python exists
if (-Not (Test-Path $pythonExe)) {
    Write-Host "❌ Python executable not found at $pythonExe. Check your venv path."
    exit 1
}

Write-Host "Cleaning old build artifacts..."
New-Item -ItemType Directory -Force -Path .\dist | Out-Null
Get-ChildItem -Path .\dist -Filter *.zip -ErrorAction SilentlyContinue | Remove-Item -Force
Remove-Item -Path .\python -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "Detecting Python version from venv..."
$pythonVersion = (& $pythonExe --version) -replace '[^\d\.]', ''
$majorMinor = ($pythonVersion -split '\.')[0..1] -join '.'
Write-Host "Using Python version: $majorMinor"

Write-Host "Packaging Lambda function..."
Compress-Archive -Path .\run_pipeline.py -DestinationPath .\dist\lambda_function.zip

Write-Host "Preparing Lambda layer structure..."
$layerPath = ".\python\lib\python$majorMinor\site-packages"
New-Item -ItemType Directory -Force -Path $layerPath | Out-Null
Copy-Item -Path .\src\* -Destination "$layerPath\src" -Recurse

Write-Host "Installing dependencies into layer..."
& $pythonExe -m pip install -r requirements.txt --target $layerPath

Write-Host "Packaging Lambda layer..."
Get-ChildItem -Path .\python -Recurse |
Where-Object { $_.FullName -notmatch '__pycache__' } |
Compress-Archive -DestinationPath .\dist\lambda_layer.zip

Write-Host "✅ Build complete! Files are in .\dist"