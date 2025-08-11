#!/usr/bin/env bash
set -euo pipefail

# Adjust as needed for your server
export OUTPUT_DIR=${OUTPUT_DIR:-"$HOME/tariff-scraper/data"}
export DB_URL=${DB_URL:-"sqlite:///$OUTPUT_DIR/cfe.db"}

mkdir -p "$OUTPUT_DIR"

# If Chrome/Chromedriver paths are custom in your office server, expose them here:
# export PATH="/opt/chromedriver:$PATH"
# export GOOGLE_APPLICATION_CREDENTIALS=...

# Run the API
exec uvicorn app:app --host 0.0.0.0 --port 8080 --workers 1
