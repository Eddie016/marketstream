#!/usr/bin/env bash

set -euo pipefail

api_url="${MARKETSTREAM_API_URL:-http://127.0.0.1:8000}"

curl --fail --silent --show-error "${api_url}/health/live" >/dev/null
curl --fail --silent --show-error "${api_url}/health/ready" >/dev/null

echo "MarketStream API is live and ready at ${api_url}"
