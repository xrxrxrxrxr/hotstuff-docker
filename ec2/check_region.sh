#!/usr/bin/env bash
set -euo pipefail

regions=$(aws ec2 describe-regions --query 'Regions[].RegionName' --output text)

normalize_count() {
  local value="$1"
  if [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "$value"
  else
    echo "0"
  fi
}

SUMMARY_FILE=$(mktemp)
cleanup() {
  rm -f "$SUMMARY_FILE"
}
trap cleanup EXIT

check_region() {
  local region="$1"
  local running pending stopping

  running=$(aws ec2 describe-instances \
    --region "$region" \
    --filters "Name=instance-state-name,Values=running" \
    --query 'length(Reservations[].Instances[])' \
    --output text 2>/dev/null || echo "0")

  pending=$(aws ec2 describe-instances \
    --region "$region" \
    --filters "Name=instance-state-name,Values=pending" \
    --query 'length(Reservations[].Instances[])' \
    --output text 2>/dev/null || echo "0")

  stopping=$(aws ec2 describe-instances \
    --region "$region" \
    --filters "Name=instance-state-name,Values=stopping" \
    --query 'length(Reservations[].Instances[])' \
    --output text 2>/dev/null || echo "0")

  running=$(normalize_count "$running")
  pending=$(normalize_count "$pending")
  stopping=$(normalize_count "$stopping")

  if [[ "$running" -ne 0 ]] || [[ "$pending" -ne 0 ]] || [[ "$stopping" -ne 0 ]]; then
    echo "$region: ${running} running, ${pending} pending, ${stopping} stopping"
  fi

  printf "%s\t%d\t%d\t%d\n" "$region" "$running" "$pending" "$stopping" >>"$SUMMARY_FILE"
}

pids=()
for region in $regions; do
  check_region "$region" &
  pids+=("$!")
done

status=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    status=1
  fi
done

total_running=0
total_pending=0
total_stopping=0
if [[ -s "$SUMMARY_FILE" ]]; then
  while IFS=$'\t' read -r region running pending stopping; do
    total_running=$((total_running + running))
    total_pending=$((total_pending + pending))
    total_stopping=$((total_stopping + stopping))
  done <"$SUMMARY_FILE"
fi

echo "Summary: ${total_running} running, ${total_pending} pending, ${total_stopping} stopping"
exit $status
