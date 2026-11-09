#!/usr/bin/env bash
# Runs the image as production runs it: non-root, read-only root filesystem, no
# capabilities. Starting at all proves the binary is static, because the base
# has no libc. Outside a cluster the service must exit on the Kubernetes client,
# which also proves it loaded its configuration first.
set -euo pipefail

image=${1:?usage: smoke-test-image.sh IMAGE}

status=0
output=$(docker run --rm --read-only --user 65532:65532 --cap-drop ALL \
  --security-opt no-new-privileges \
  --volume "$PWD/config.example.json:/etc/site-status/config.json:ro" \
  --env SITE_STATUS_CONFIG=/etc/site-status/config.json \
  "$image" 2>&1) || status=$?
echo "$output"

test "$status" -ne 0
grep -q 'kubernetes client configuration' <<<"$output"
