#!/bin/bash
set -eo pipefail

# Update runtime.image_tag in Keboola component configuration

# Validate required environment variables
: "${KBC_HOST:?}"
: "${KBC_STORAGE_TOKEN:?}"
: "${KBC_CONFIG_ID:?}"
: "${IMAGE_TAG:?}"
: "${COMPONENT_ID:?}"

API_URL="https://${KBC_HOST}/v2/storage/components/${COMPONENT_ID}/configs/${KBC_CONFIG_ID}"
AUTH_HEADER="X-StorageApi-Token: ${KBC_STORAGE_TOKEN}"

echo "Updating ${COMPONENT_ID}/${KBC_CONFIG_ID} to tag: ${IMAGE_TAG}"

# Fetch config, update tag, prepare payload, and push back - all in one pipeline
UPDATE_PAYLOAD=$(curl -sf -H "${AUTH_HEADER}" "${API_URL}" | \
    jq --arg tag "${IMAGE_TAG}" '
        .configuration.runtime.image_tag = $tag |
        {configuration, description, name}
    ')

# Push updated configuration
HTTP_CODE=$(curl -s -o /dev/stderr -w "%{http_code}" -X PUT \
    -H "${AUTH_HEADER}" \
    -H "Content-Type: application/json" \
    -d "${UPDATE_PAYLOAD}" \
    "${API_URL}")

if [ "$HTTP_CODE" -ge 200 ] && [ "$HTTP_CODE" -lt 300 ]; then
    echo "✓ Successfully updated to tag: ${IMAGE_TAG}"
else
    echo "✗ Failed to update configuration (HTTP ${HTTP_CODE})"
    exit 1
fi
