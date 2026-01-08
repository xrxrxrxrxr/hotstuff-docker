#!/usr/bin/env bash
set -euo pipefail

KEY_NAME= # your key name
PUBLIC_KEY_FILE= # your public key path
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGIONS_FILE="$SCRIPT_DIR/regions.txt"

if [[ ! -f "$PUBLIC_KEY_FILE" ]]; then
  echo "Public key not found: $PUBLIC_KEY_FILE" >&2
  exit 1
fi

if [[ ! -f "$REGIONS_FILE" ]]; then
  echo "Regions file not found: $REGIONS_FILE" >&2
  exit 1
fi

mapfile -t REGIONS < <(sed -e 's/#.*//' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' "$REGIONS_FILE" | awk 'NF > 0')

if (( ${#REGIONS[@]} == 0 )); then
  echo "Regions list is empty (check $REGIONS_FILE)" >&2
  exit 1
fi

for region in "${REGIONS[@]}"; do
  echo "→ Uploading $KEY_NAME to $region"
  if aws ec2 import-key-pair \
      --region "$region" \
      --key-name "$KEY_NAME" \
      --public-key-material "fileb://$PUBLIC_KEY_FILE" >/dev/null; then
    echo "   $region: key imported"
  else
    echo "   $region: import failed (maybe it already exists)" >&2
  fi
done
