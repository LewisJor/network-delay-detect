#!/bin/bash

set -e

echo "[INFO] Starting sensor node..."
echo "[DEBUG] Environment:"
env | grep ENCRYPTION_KEY || echo "No ENCRYPTION_KEY found"
env | grep SENSOR_ID || echo "No SENSOR_ID found"
echo "[DEBUG] Starting Python script..."
python sensor.py