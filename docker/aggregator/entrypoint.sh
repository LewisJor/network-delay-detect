#!/bin/bash
set -e

echo "[INFO] Starting aggregator..."
echo "[DEBUG] Environment:"
env | grep ENCRYPTION_KEY || echo "No ENCRYPTION_KEY found"
echo "[DEBUG] Starting Python script..."
python aggregator.py