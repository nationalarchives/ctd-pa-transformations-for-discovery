#!/usr/bin/env bash
set -euo pipefail

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
cp -r src python/lib/python3.12/site-packages/

# Zip Lambda layer (exclude __pycache__)
echo "Packaging Lambda layer..."
cd python
zip -r ../dist/lambda_layer.zip . -x "*/__pycache__/*"
cd ..

echo "✅ Build complete! Files are in ./dist"