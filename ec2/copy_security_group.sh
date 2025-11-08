#!/usr/bin/env bash
set -euo pipefail

SOURCE_REGION="us-east-1"
SOURCE_SECURITY_GROUP_ID="sg-0952bba51a1ff3866"
TARGET_GROUP_NAME="smrol"
TARGET_GROUP_DESC="Cloned from ${SOURCE_SECURITY_GROUP_ID}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGIONS_FILE="$ROOT_DIR/regions.txt"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

if [[ ! -f "$REGIONS_FILE" ]]; then
  echo "Regions file not found: $REGIONS_FILE" >&2
  exit 1
fi

account_id=$(aws --region "$SOURCE_REGION" sts get-caller-identity --query 'Account' --output text)

template_json=$(aws ec2 describe-security-groups \
  --region "$SOURCE_REGION" \
  --group-ids "$SOURCE_SECURITY_GROUP_ID" \
  --output json | jq '.SecurityGroups[0]')

template_ingress=$(jq '.IpPermissions' <<<"$template_json")
template_egress=$(jq '.IpPermissionsEgress' <<<"$template_json")

is_allow_all_egress() {
  local file="$1"
  jq -e 'length == 1 and 
         .[0].IpProtocol == "-1" and
         ((.[0].IpRanges // []) | length == 1) and
         .[0].IpRanges[0].CidrIp == "0.0.0.0/0"' "$file" >/dev/null 2>&1
}

mapfile -t TARGET_REGIONS < <(sed -e 's/#.*//' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' "$REGIONS_FILE" | awk 'NF>0')

if (( ${#TARGET_REGIONS[@]} == 0 )); then
  echo "No regions listed in $REGIONS_FILE" >&2
  exit 1
fi

for region in "${TARGET_REGIONS[@]}"; do
  echo "=== Region: $region ==="

  vpc_id=$(aws ec2 describe-vpcs \
    --region "$region" \
    --filters "Name=isDefault,Values=true" \
    --query 'Vpcs[0].VpcId' \
    --output text)

  if [[ -z "$vpc_id" || "$vpc_id" == "None" ]]; then
    echo "  ! No default VPC found in $region, skipping" >&2
    continue
  fi

  sg_id=$(aws ec2 create-security-group \
    --region "$region" \
    --group-name "$TARGET_GROUP_NAME" \
    --description "$TARGET_GROUP_DESC" \
    --vpc-id "$vpc_id" \
    --query 'GroupId' \
    --output text)
  echo "  Created $sg_id in $region"

  fix_permissions() {
    local permissions_json="$1"
    local outfile="$2"
    jq --arg sg "$sg_id" --arg account "$account_id" '
      map(
        if (.UserIdGroupPairs // [] | length) > 0 then
          .UserIdGroupPairs = [ { GroupId: $sg, UserId: $account } ]
        else
          .
        end
      )
    ' <<<"$permissions_json" > "$outfile"
  }

  ingress_file="$TMP_DIR/ingress-$region.json"
  egress_file="$TMP_DIR/egress-$region.json"
  fix_permissions "$template_ingress" "$ingress_file"
  fix_permissions "$template_egress" "$egress_file"

  if [[ $(jq length "$ingress_file") -gt 0 ]]; then
    aws ec2 authorize-security-group-ingress \
      --region "$region" \
      --group-id "$sg_id" \
      --ip-permissions "file://$ingress_file"
    echo "  Ingress rules cloned"
  fi

  if [[ $(jq length "$egress_file") -gt 0 ]]; then
    if is_allow_all_egress "$egress_file"; then
      echo "  Skipping default allow-all egress"
    else
      aws ec2 authorize-security-group-egress \
        --region "$region" \
        --group-id "$sg_id" \
        --ip-permissions "file://$egress_file"
      echo "  Egress rules cloned"
    fi
  fi

done

echo "All requested regions processed."
