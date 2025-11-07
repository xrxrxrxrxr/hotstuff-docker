#!/usr/bin/env bash
set -euo pipefail

REGION="us-east-1"
PROFILE=""
COUNT=""
DRY_RUN=false

export AWS_PAGER=""

usage() {
  cat <<'USAGE'
Usage: ./start_instances.sh [--region REGION] [--profile PROFILE] [--count N] [--dry-run]

Starts stopped EC2 instances:
  - Always starts 'client' if it's stopped
  - Starts N other stopped instances (excluding 'client')
  - If --count is omitted, starts all stopped (client + others)
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region) REGION="$2"; shift 2;;
    --profile) PROFILE="$2"; shift 2;;
    --count) COUNT="$2"; shift 2;;
    --dry-run) DRY_RUN=true; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg $1"; usage; exit 1;;
  esac
done

aws_cmd=(aws)
[[ -n "$REGION" ]] && aws_cmd+=(--region "$REGION")
[[ -n "$PROFILE" ]] && aws_cmd+=(--profile "$PROFILE")

# 获取所有 stopped 实例的 Id 和 Name
mapfile -t lines < <("${aws_cmd[@]}" ec2 describe-instances \
  --filters "Name=instance-state-name,Values=stopped" \
  --query "Reservations[].Instances[].{Id:InstanceId,Name:Tags[?Key=='Name']|[0].Value}" \
  --output text)

declare -a pool_ids=()
client_id=""

for line in "${lines[@]}"; do
  [[ -z "$line" ]] && continue
  id=$(awk '{print $1}' <<<"$line")
  name=$(awk '{print $2}' <<<"$line")
  if [[ "$name" == "client" ]]; then
    client_id="$id"
  else
    pool_ids+=("$id")
  fi
done

if [[ -z "$client_id" ]]; then
  echo "⚠️  No stopped 'client' instance found (will skip it)."
fi

if [[ -z "$COUNT" ]]; then
  COUNT=${#pool_ids[@]}
fi

selected_ids=("${pool_ids[@]:0:$COUNT}")
final_ids=()
[[ -n "$client_id" ]] && final_ids+=("$client_id")
final_ids+=("${selected_ids[@]}")

client_count=0
[[ -n "$client_id" ]] && client_count=1
non_client_count=${#selected_ids[@]}
total_count=${#final_ids[@]}

if [[ $total_count -eq 0 ]]; then
  echo "Nothing to start."
  exit 0
fi

if $DRY_RUN; then
  echo "✨ Dry run: would start ${total_count} instance(s) (clients: ${client_count}, non-clients: ${non_client_count}):"
  printf '  %s\n' "${final_ids[@]}"
  exit 0
fi

"${aws_cmd[@]}" ec2 start-instances --instance-ids "${final_ids[@]}"
echo "Started instances (${total_count} total, clients: ${client_count}, non-clients: ${non_client_count}):"
printf '  %s\n' "${final_ids[@]}"
