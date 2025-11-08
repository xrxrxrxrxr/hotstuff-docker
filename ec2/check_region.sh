#!/usr/bin/env bash
set -euo pipefail

regions=$(aws ec2 describe-regions --query 'Regions[].RegionName' --output text)

for region in $regions; do
  count=$(aws ec2 describe-instances \
    --region "$region" \
    --filters "Name=instance-state-name,Values=running" \
    --query 'length(Reservations[].Instances[])' \
    --output text 2>/dev/null || echo "0")

  if [[ "$count" != "0" && "$count" != "None" ]]; then
    echo "$region: $count running"
  fi
 done
