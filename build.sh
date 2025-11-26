#!/usr/bin/env bash
set -euo pipefail

# Ask for venv name
read -p "Enter the name of your virtual environment folder (e.g., venv or .venv): " VENV_NAME

# Validate venv path
if [ ! -f "./$VENV_NAME/Scripts/python.exe" ]; then
    echo "❌ Could not find Python executable in ./$VENV_NAME/Scripts/python.exe"
    exit 1
fi

# Clean old artifacts
echo "Cleaning old build artifacts..."
mkdir -p dist
rm -f dist/*.zip
rm -rf python

# Zip Lambda function
echo "Packaging Lambda function..."
zip -j dist/lambda_function.zip run_pipeline.py

# Prepare layer structure
echo "Preparing Lambda layer structure..."
mkdir -p python/lib/python3.12/site-packages

# Copy src folder into site-packages
cp -r src python/lib/python3.12/site-packages/

# Install dependencies using venv Python
echo "Installing dependencies from venv..."
./$VENV_NAME/Scripts/python.exe -m pip install -r requirements.txt --target python/lib/python3.12/site-packages

# Zip Lambda layer (exclude __pycache__)
echo "Packaging Lambda layer..."
cd python
zip -r ../dist/lambda_layer.zip . -x "*/__pycache__/*"
cd ..

echo "✅ Build complete! Files are in ./dist"
echo " - Lambda layer: dist/lambda_layer.zip"