#!/usr/bin/env bash
# ==============================================================================
# run_producer.sh - Run local Python traffic telemetry producer
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "[INFO] Installing producer dependencies..."
pip install -r "${PROJECT_ROOT}/producer/requirements.txt"

echo "[INFO] Launching traffic dirty telemetry producer..."
python "${PROJECT_ROOT}/producer/traffic_dirty_producer.py"
