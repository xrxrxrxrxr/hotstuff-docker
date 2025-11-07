#!/usr/bin/env bash
set -euo pipefail

REGION="us-east-1"
PROFILE=""
NAME_VALUES=""
DRY_RUN=false

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

filters=("Name=instance-state-name,Values=running")
if [[ -n "$NAME_VALUES" ]]; then
  filters+=("Name=tag:Name,Values=${NAME_VALUES}")
fi

describe_cmd=(aws)
[[ -n "$REGION" ]] && describe_cmd+=(--region "$REGION")
[[ -n "$PROFILE" ]] && describe_cmd+=(--profile "$PROFILE")
describe_cmd+=(ec2)
describe_cmd+=(describe-instances)
for filter in "${filters[@]}"; do
  describe_cmd+=(--filters "$filter")
done
describe_cmd+=(--query)
describe_cmd+=("Reservations[].Instances[].InstanceId")
describe_cmd+=(--output)
describe_cmd+=(text)

instance_ids=$("${describe_cmd[@]}")
instance_ids=$(echo "$instance_ids" | xargs)

if [[ -z "$instance_ids" || "$instance_ids" == "None" ]]; then
  if [[ -n "$NAME_VALUES" ]]; then
    echo "No running instances match the provided names"
  else
    echo "No running instances found in the selected region"
  fi
  exit 0
fi

read -r -a instance_array <<< "$instance_ids"

if $DRY_RUN; then
  echo "Dry run: would stop instances: ${instance_array[*]}"
  exit 0
fi

stop_cmd=(aws)
[[ -n "$REGION" ]] && stop_cmd+=(--region "$REGION")
[[ -n "$PROFILE" ]] && stop_cmd+=(--profile "$PROFILE")
stop_cmd+=(ec2)
stop_cmd+=(stop-instances)
stop_cmd+=(--instance-ids)
stop_cmd+=("${instance_array[@]}")

"${stop_cmd[@]}"

echo "Stopped instances: ${instance_array[*]}"
