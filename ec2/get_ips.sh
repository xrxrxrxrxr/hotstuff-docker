#!/usr/bin/env bash
# Retrieve public and private IPs (across one or more regions) and update hosts.txt/envs
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"
HOSTS_FILE="$ROOT_DIR/hosts.txt"

REGION_INPUT="${REGIONS:-}"
REGION_FILE_INPUT="${REGIONS_FILE:-}"
DEFAULT_REGIONS_FILE="$ROOT_DIR/regions.txt"

load_regions_from_file() {
  local file="$1"
  if [[ ! -f "$file" ]]; then
    echo "Regions file '$file' does not exist" >&2
    exit 1
  fi
  mapfile -t REGION_LIST < <(sed -e 's/#.*//' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' "$file" | awk 'NF > 0')
  if (( ${#REGION_LIST[@]} == 0 )); then
    echo "Regions file '$file' does not contain any regions" >&2
    exit 1
  fi
}

if [[ -n "$REGION_INPUT" ]]; then
  read -r -a REGION_LIST <<< "$REGION_INPUT"
elif [[ -n "$REGION_FILE_INPUT" ]]; then
  load_regions_from_file "$REGION_FILE_INPUT"
elif [[ -f "$DEFAULT_REGIONS_FILE" ]]; then
  load_regions_from_file "$DEFAULT_REGIONS_FILE"
elif [[ -n "${AWS_DEFAULT_REGION:-}" ]]; then
  REGION_LIST=("$AWS_DEFAULT_REGION")
else
  REGION_LIST=("us-west-2")
fi

echo "Discovering running instances in: ${REGION_LIST[*]}"

FILTERS=(--filters "Name=instance-state-name,Values=running")
if [[ -n "${EXTRA_FILTERS:-}" ]]; then
  FILTERS+=(${EXTRA_FILTERS})
fi

QUERY='Reservations[*].Instances[*].[PublicIpAddress,PrivateIpAddress,Tags[?Key==`Name`].Value | [0]]'

declare -A NODE_PUB=()
declare -A NODE_PRIV=()
client_pub=""
client_priv=""
NEXT_NODE_INDEX=0

for region in "${REGION_LIST[@]}"; do
  echo "  → $region"
  while IFS=$'\t' read -r pub priv name; do
    [[ -z "$name" || "$name" == "None" ]] && continue
    name=${name//$'\r'/}
    pub=${pub//$'\r'/}
    priv=${priv//$'\r'/}
    [[ "$pub" == "None" ]] && pub=""
    [[ "$priv" == "None" ]] && priv=""

    if [[ "$name" == "client" ]]; then
      [[ -n "$pub" ]] && client_pub="$pub"
      [[ -n "$priv" ]] && client_priv="$priv"
    else
      idx="node${NEXT_NODE_INDEX}"
      NEXT_NODE_INDEX=$((NEXT_NODE_INDEX + 1))
      [[ -n "$pub" ]] && NODE_PUB[$idx]="$pub"
      [[ -n "$priv" ]] && NODE_PRIV[$idx]="$priv"
    fi
  done < <(aws ec2 describe-instances --region "$region" "${FILTERS[@]}" --query "$QUERY" --output text)
done

if (( ${#NODE_PUB[@]} == 0 && ${#NODE_PRIV[@]} == 0 )); then
  echo "No node entries found (check filters, regions, or tags)." >&2
  exit 1
fi

: > "$HOSTS_FILE"

mapfile -t ORDERED_IDX < <(printf '%s\n' "${!NODE_PUB[@]}" | sort -V)

for idx in "${ORDERED_IDX[@]}"; do
  if [[ -n "${NODE_PUB[$idx]:-}" ]]; then
    printf "%s %s\n" "${NODE_PUB[$idx]}" "$idx" >> "$HOSTS_FILE"
  fi
done
if [[ -n "$client_pub" ]]; then
  printf "%s client\n" "$client_pub" >> "$HOSTS_FILE"
fi

for idx in "${ORDERED_IDX[@]}"; do
  if [[ -n "${NODE_PRIV[$idx]:-}" ]]; then
    printf "%s %s-private\n" "${NODE_PRIV[$idx]}" "$idx" >> "$HOSTS_FILE"
  fi
done
if [[ -n "$client_priv" ]]; then
  printf "%s client-private\n" "$client_priv" >> "$HOSTS_FILE"
fi

if [[ ! -s "$HOSTS_FILE" ]]; then
  echo "hosts.txt is empty; aborting" >&2
  exit 1
fi

echo "Generated hosts.txt:"
cat "$HOSTS_FILE"

echo "Running generate-node-envs.sh (NODE_IP_SOURCE=${NODE_IP_SOURCE:-private})"
NODE_IP_SOURCE="${NODE_IP_SOURCE:-}" ./generate-node-envs.sh
# NODE_IP_SOURCE="${NODE_IP_SOURCE:-}" ./generate-node-envs-attack-mode.sh

echo "Updated envs/"

make deploy
