#!/bin/bash
set -e

# Update runtime.image_tag in Keboola component configuration(s)

# Validate required environment variables
: "${KBC_HOST:?}"
: "${KBC_STORAGE_TOKEN:?}"
: "${KBC_CONFIG_IDS:?}"
: "${IMAGE_TAG:?}"
: "${COMPONENT_ID:?}"

AUTH_HEADER="X-StorageApi-Token: ${KBC_STORAGE_TOKEN}"

# Split config IDs by space or comma and iterate
IFS=', ' read -ra CONFIG_ARRAY <<< "$KBC_CONFIG_IDS"

FAILED_CONFIGS=()
SUCCESS_COUNT=0

for CONFIG_ID in "${CONFIG_ARRAY[@]}"; do
    # Trim whitespace
    CONFIG_ID=$(echo "$CONFIG_ID" | xargs)
    
    [ -z "$CONFIG_ID" ] && continue
    
    API_URL="https://${KBC_HOST}/v2/storage/components/${COMPONENT_ID}/configs/${CONFIG_ID}"
    
    echo "Updating ${COMPONENT_ID}/${CONFIG_ID} to tag: ${IMAGE_TAG}"
    
    # Fetch config, update tag, prepare payload, and push back
    set +e
    UPDATE_PAYLOAD=$(curl -sf -H "${AUTH_HEADER}" "${API_URL}" | \
        jq --arg tag "${IMAGE_TAG}" '
            .configuration.runtime.image_tag = $tag |
            {configuration, description, name}
        ')
    FETCH_EXIT=$?
    set -e
    
    if [ $FETCH_EXIT -ne 0 ]; then
        echo "✗ Failed to fetch configuration ${CONFIG_ID}"
        FAILED_CONFIGS+=("$CONFIG_ID")
        continue
    fi
    
    # Push updated configuration
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT \
        -H "${AUTH_HEADER}" \
        -H "Content-Type: application/json" \
        -d "${UPDATE_PAYLOAD}" \
        "${API_URL}")
    
    if [ "$HTTP_CODE" -ge 200 ] && [ "$HTTP_CODE" -lt 300 ]; then
        echo "✓ Successfully updated ${CONFIG_ID} to tag: ${IMAGE_TAG}"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        echo "✗ Failed to update configuration ${CONFIG_ID} (HTTP ${HTTP_CODE})"
        FAILED_CONFIGS+=("$CONFIG_ID")
    fi
done

echo ""
echo "Summary: ${SUCCESS_COUNT}/${#CONFIG_ARRAY[@]} configurations updated successfully"

if [ ${#FAILED_CONFIGS[@]} -gt 0 ] && [ -n "${FAILED_CONFIGS[0]}" ]; then
    echo "Failed configs: ${FAILED_CONFIGS[*]}"
    exit 1
fi

exit 0
