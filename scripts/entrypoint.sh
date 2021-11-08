#!/bin/bash

# Usage ./scripts/run.sh

set -eo pipefail

# Run ingest
echo "⏳ Exporting $BRP_PROTOCOL..."

python warehouse_project.py \
    $REDCAP_TOKEN \
    $BRP_TOKEN \
    $BRP_PROTOCOL \
    $CID_MAGIC_NUMBER \
    $D3B_WAREHOUSE_DB_URL \
    $OPTIONAL_ARGUMENTS

echo "✅ $BRP_PROTOCOL has been exported!"
