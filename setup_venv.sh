#!/usr/bin/env bash
set -euo pipefail

VENV_DIR=""
PYTHON="python3"

if [ "$#" -ge 1 ]; then
  VENV_DIR="$1"
fi
if [ "$#" -ge 2 ]; then
  PYTHON="$2"
fi

# Ask for environment name if not provided
if [ -z "$VENV_DIR" ]; then
  read -p "Enter the environment name (letters, numbers, underscores only): " VENV_DIR
fi

if [[ ! $VENV_DIR =~ ^[A-Za-z0-9_]+$ ]]; then
  echo "Invalid environment name '$VENV_DIR'. Only letters, numbers and underscores are allowed (no spaces)." >&2
  exit 1
fi

echo "Setting up Python virtual environment named: $VENV_DIR"

if ! command -v "$PYTHON" >/dev/null 2>&1; then
  echo "Could not run '$PYTHON'. Please install Python 3.12 or pass the full path to python as the second argument." >&2
  exit 2
fi

# Verify python version is 3.12
PYVER=$("$PYTHON" --version 2>&1 || true)
if [[ ! $PYVER =~ ^Python[[:space:]]3\.12($|\.) ]]; then
  echo "Python 3.12 is required. Found: $PYVER" >&2
  exit 2
fi

# Create venv
"$PYTHON" -m venv "$VENV_DIR"

ACTIVATE_SCRIPT="$VENV_DIR/bin/activate"
if [ ! -f "$ACTIVATE_SCRIPT" ]; then
  echo "Activation script not found at $ACTIVATE_SCRIPT" >&2
  exit 4
fi

# Source the venv in this shell session
# Note: scripts run non-interactively won't keep the venv activated after the script exits.
. "$ACTIVATE_SCRIPT"

if [ ! -f requirements.txt ]; then
  echo "requirements.txt not found in the current directory. Skipping package install." >&2
else
  echo "Upgrading pip and installing from requirements.txt..."
  python -m pip install --upgrade pip
  python -m pip install -r requirements.txt
fi

echo "Virtual environment ready. To activate it in a new shell run: source $VENV_DIR/bin/activate"