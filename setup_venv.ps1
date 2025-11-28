param(
    [string]$EnvName = "",
    [string]$PythonExe = "python"
)

# Determine environment name (no spaces or symbols other than underscores allowed)
if ([string]::IsNullOrWhiteSpace($EnvName)) {
    $EnvName = Read-Host -Prompt "Enter the environment name (letters, numbers and underscores only)"
}

if ($EnvName -notmatch '^[A-Za-z0-9_]+$') {
    Write-Error "Invalid environment name '$EnvName'. Only letters, numbers and underscores are allowed (no spaces)."
    exit 1
}

$VenvDir = $EnvName
Write-Host "Setting up Python virtual environment named: $EnvName -> directory: $VenvDir"

# Try to discover Python and ensure it's Python 3.12
try {
    $version_out = & $PythonExe --version 2>&1
} catch {
    Write-Error "Could not run '$PythonExe'. Please install Python 3.12 or pass the full path to python as the second argument."
    exit 2
}

$version_out = $version_out -join ' '
if ($version_out -notmatch 'Python\s+3\.12(\.|$)') {
    Write-Error "Python 3.12 is required. Found: $version_out"
    exit 2
}

# Create venv
try {
    & $PythonExe -m venv $VenvDir
} catch {
    Write-Error "Failed to create virtual environment using '$PythonExe -m venv $VenvDir'. Error: $_"
    exit 3
}

$activatePath = Join-Path $VenvDir "Scripts\Activate.ps1"
if (-not (Test-Path $activatePath)) {
    Write-Error "Activation script not found at $activatePath"
    exit 4
}

# Activate the venv for this script session
Write-Host "Activating virtual environment..."
. $activatePath

# Upgrade pip and install requirements
if (-not (Test-Path "requirements.txt")) {
    Write-Warning "requirements.txt not found in the current directory. Skipping package install."
} else {
    Write-Host "Upgrading pip and installing from requirements.txt..."
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt
}

Write-Host "Virtual environment ready. To activate it in a new PowerShell session run:`n. $VenvDir\Scripts\Activate.ps1"