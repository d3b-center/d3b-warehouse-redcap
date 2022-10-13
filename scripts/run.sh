#!/bin/bash

# Usage ./scripts/run.sh

set -eo pipefail

echo "⏳ Loading REDCAP records into warehouse"

# Load REDCAP records
if ls secrets.json 1> /dev/null 2>&1; then

    # Loop over secrets
    jq -c '.[]' secrets.json | while read secrets; do
        # Parse study secrets
        REDCAP_TOKEN=$(echo $secrets | jq -r '.REDCAP_TOKEN_ENV_KEY')
        declare $REDCAP_TOKEN=$(echo $secrets | jq -r ".$REDCAP_TOKEN")
        BRP_TOKEN=$(echo $secrets | jq -r '.BRP_TOKEN_ENV_KEY')
        BRP_TOKEN_VALUE=$(echo $secrets | jq -r '.BRP_TOKEN_VALUE')
        BRP_PROTOCOL=$(echo $secrets | jq -r '.BRP_PROTOCOL')
        CID_MAGIC_NUMBER=$(echo $secrets | jq -r '.CID_MAGIC_NUMBER_ENV_KEY')
        CID_MAGIC_NUMBER_VALUE=$(echo $secrets | jq -r '.CID_MAGIC_NUMBER_VALUE')
        D3B_WAREHOUSE_DB_URL=$(echo $secrets | jq -r '.D3B_WAREHOUSE_DB_URL_ENV_KEY')
        D3B_WAREHOUSE_DB_URL_VALUE=$(echo $secrets | jq -r '.D3B_WAREHOUSE_DB_URL_VALUE')
        OPTIONAL_ARGUMENTS=$(echo $(echo $secrets | jq -r '.OPTIONAL_ARGUMENTS') | jq -r '.[]')

        # Run ingest
        echo "Exporting $BRP_PROTOCOL..."

	python3 warehouse_project.py \
    		$REDCAP_TOKEN \
    		$BRP_TOKEN \
    		$BRP_PROTOCOL \
    		$CID_MAGIC_NUMBER \
    		$D3B_WAREHOUSE_DB_URL \
    		$OPTIONAL_ARGUMENTS

	echo "✅ $BRP_PROTOCOL has been exported!"

        echo "$BRP_PROTOCOL has been exported!"

    done
else
    echo "🐛 Could not find secrets.json files"
fi

echo "✅ REDCAP records has been loaded into warehouse!"
