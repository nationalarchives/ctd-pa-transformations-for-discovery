#!/usr/bin/env bash
set -euo pipefail

echo "Installing Python packages from requirements.txt..."

# Check if requirements.txt exists
if [ ! -f "requirements.txt" ]; then
    echo "❌ Error: requirements.txt not found"
    exit 1
fi

# Install packages
pip install -r requirements.txt

echo "✅ Installation complete!"
