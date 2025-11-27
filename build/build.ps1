
# Enable strict error handling
$ErrorActionPreference = "Stop"

# Prompt for venv name
$venvName = Read-Host "Enter the name of your virtual environment folder (e.g., venv or .venv)"

# Build path to Python executable
$pythonExe = ".\$venvName\Scripts\python.exe"

# Validate venv path
if (-Not (Test-Path $pythonExe)) {
    Write-Host "❌ Could not find Python executable at $pythonExe. Check your venv name and path."
    exit 1
}

# Detect Python version from venv
$pythonVersion = (& $pythonExe --version) -replace '[^\d\.]', ''
$majorMinor = ($pythonVersion -split '\.')[0..1] -join '.'
Write-Host "Using Python version: $majorMinor"

# Clean old artifacts
Write-Host "Cleaning old build artifacts..."
New-Item -ItemType Directory -Force -Path .\dist | Out-Null
Get-ChildItem -Path .\dist -Filter *.zip -ErrorAction SilentlyContinue | Remove-Item -Force
Remove-Item -Path .\python -Recurse -Force -ErrorAction SilentlyContinue

# Zip Lambda function
Write-Host "Packaging Lambda function..."
Compress-Archive -Path .\run_pipeline.py -DestinationPath .\dist\lambda_function.zip

# Prepare layer structure
Write-Host "Preparing Lambda layer structure..."
$layerPath = ".\python\lib\python$majorMinor\site-packages"
New-Item -ItemType Directory -Force -Path $layerPath | Out-Null

# Copy src folder into site-packages
Copy-Item -Path .\src\* -Destination "$layerPath\src" -Recurse

# Install dependencies using venv Python
Write-Host "Installing dependencies from venv..."
& $pythonExe -m pip install -r requirements.txt --target $layerPath

# Zip Lambda layer excluding __pycache__
Write-Host "Packaging Lambda layer..."
Get-ChildItem -Path .\python -Recurse |
Where-Object { $_.FullName -notmatch '__pycache__' } |
Compress-Archive -DestinationPath .\dist\lambda_layer.zip

Write-Host "✅ Build complete! Files are in .\dist"