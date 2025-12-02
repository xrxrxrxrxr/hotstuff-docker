#!/usr/bin/env bash
set -euo pipefail

REGION=""
PROFILE=""
NAME_VALUES=""
DRY_RUN=false

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGIONS_FILE_PATH="$SCRIPT_DIR/regions.txt"

SUMMARY_FILE=$(mktemp)
STOP_PIDS=()

cleanup_summary() {
  rm -f "$SUMMARY_FILE"
}
trap cleanup_summary EXIT

# Disable AWS CLI pager to avoid interactive prompts while piping output.
export AWS_PAGER=""

usage() {
  cat <<'USAGE'
Usage: ./stop_instances.sh [--region REGION] [--profile PROFILE] [--names name1,name2,...] [--dry-run]

Stops every running EC2 instance in the selected region. When --names is
provided, only instances whose Name tag matches are stopped. Region defaults to
AWS_DEFAULT_REGION or the AWS CLI configuration files.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region)
      [[ $# -lt 2 ]] && { echo "--region requires an argument" >&2; exit 1; }
      REGION="$2"
      shift 2
      ;;
    --profile)
      [[ $# -lt 2 ]] && { echo "--profile requires an argument" >&2; exit 1; }
      PROFILE="$2"
      shift 2
      ;;
    --names)
      [[ $# -lt 2 ]] && { echo "--names requires a comma-separated list" >&2; exit 1; }
      NAME_VALUES="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

load_regions_from_file() {
  local file="$1"
  mapfile -t REGIONS_FROM_FILE < <(sed -e 's/#.*//' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' "$file" | awk 'NF > 0')
}

determine_regions() {
  if [[ -n "$REGION" ]]; then
    TARGET_REGIONS=("$REGION")
    return
  fi

  if [[ -f "$REGIONS_FILE_PATH" ]]; then
    load_regions_from_file "$REGIONS_FILE_PATH"
    if (( ${#REGIONS_FROM_FILE[@]} > 0 )); then
      mapfile -t TARGET_REGIONS < <(printf '%s\n' "${REGIONS_FROM_FILE[@]}" | awk '!seen[$0]++')
      return
    fi
  fi

  if [[ -n "${AWS_DEFAULT_REGION:-}" ]]; then
    TARGET_REGIONS=("$AWS_DEFAULT_REGION")
  else
    TARGET_REGIONS=("us-east-1")
  fi
}

stop_region_instances() {
  local region="$1"
  local filters=("Name=instance-state-name,Values=running")
  if [[ -n "$NAME_VALUES" ]]; then
    filters+=("Name=tag:Name,Values=${NAME_VALUES}")
  fi

  local describe_cmd=(aws)
  [[ -n "$region" ]] && describe_cmd+=(--region "$region")
  [[ -n "$PROFILE" ]] && describe_cmd+=(--profile "$PROFILE")
  describe_cmd+=(ec2 describe-instances)
  for filter in "${filters[@]}"; do
    describe_cmd+=(--filters "$filter")
  done
  describe_cmd+=(--query "Reservations[].Instances[].InstanceId" --output text)

  local instance_ids
  instance_ids=$("${describe_cmd[@]}") || instance_ids=""
  instance_ids=$(echo "$instance_ids" | xargs)

  if [[ -z "$instance_ids" || "$instance_ids" == "None" ]]; then
    if [[ -n "$NAME_VALUES" ]]; then
      echo "[${region:-default}] No running instances match the provided names"
    else
      echo "[${region:-default}] No running instances found"
    fi
    return
  fi

  read -r -a instance_array <<< "$instance_ids"

  if $DRY_RUN; then
    echo "[${region:-default}] Dry run: would stop instances: ${instance_array[*]}"
    return
  fi

  local stop_cmd=(aws)
  [[ -n "$region" ]] && stop_cmd+=(--region "$region")
  [[ -n "$PROFILE" ]] && stop_cmd+=(--profile "$PROFILE")
  stop_cmd+=(ec2 stop-instances --instance-ids "${instance_array[@]}")

  "${stop_cmd[@]}"
  echo "[${region:-default}] Stopped instances: ${instance_array[*]}"

  printf "%s\t%d\n" "${region:-default}" "${#instance_array[@]}" >> "$SUMMARY_FILE"
}

declare -a TARGET_REGIONS=()
determine_regions

stop_summary() {
  if [[ ! -s "$SUMMARY_FILE" ]]; then
    echo "Summary: nothing to stop"
    return
  fi

  local total=0
  local regions=0

  while IFS=$'\t' read -r region count; do
    [[ -z "$region" ]] && continue
    total=$((total + count))
    regions=$((regions + 1))
  done < "$SUMMARY_FILE"

  if $DRY_RUN; then
    echo "Dry run summary: would stop ${total} instance(s) across ${regions} region(s)."
  else
    echo "Summary: stopped ${total} instance(s) across ${regions} region(s)."
  fi
}

for region in "${TARGET_REGIONS[@]}"; do
  stop_region_instances "$region" &
  STOP_PIDS+=("$!")
done

status=0
for pid in "${STOP_PIDS[@]}"; do
  if ! wait "$pid"; then
    status=1
  fi
done

stop_summary

exit $status
