#!/usr/bin/env bash
# generate-node-envs.sh
# Generate ec2/envs/node{X}.env files in bulk.
# By default NODE_HOSTS uses private IPs; set NODE_IP_SOURCE=public for cross-region tests.

set -euo pipefail

IP_SOURCE_INPUT="${NODE_IP_SOURCE:-}"
NODE_IP_SOURCE="${IP_SOURCE_INPUT:-private}"
if [[ "$NODE_IP_SOURCE" != "private" && "$NODE_IP_SOURCE" != "public" ]]; then
  echo "NODE_IP_SOURCE must be either 'private' or 'public'" >&2
  exit 1
fi
NODE_IP_SOURCE_WAS_DEFAULT=true
[[ -n "$IP_SOURCE_INPUT" ]] && NODE_IP_SOURCE_WAS_DEFAULT=false

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOSTS_FILE="$ROOT_DIR/hosts.txt"
ENVS_DIR="$ROOT_DIR/envs"

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "hosts.txt not found: $HOSTS_FILE" >&2
  exit 1
fi

# Determine node count: prefer nodeX-private entries, otherwise use nodeX entries
NODE_COUNT_INPUT="${1:-}"
if [[ -z "$NODE_COUNT_INPUT" ]]; then
  if [[ "$NODE_IP_SOURCE" == "private" ]]; then
    NODE_COUNT=$(grep -E 'node[0-9]+-private$' "$HOSTS_FILE" | wc -l | tr -d ' ')
    if (( NODE_COUNT == 0 )); then
      if $NODE_IP_SOURCE_WAS_DEFAULT; then
        echo "Warning: no 'nodeX-private' entries found; switching to NODE_IP_SOURCE=public" >&2
        NODE_IP_SOURCE="public"
      else
        echo "Error: NODE_IP_SOURCE=private but hosts.txt has no 'nodeX-private' entries" >&2
        exit 1
      fi
    fi
  fi

  if [[ "$NODE_IP_SOURCE" == "public" ]]; then
    NODE_COUNT=$(grep -E 'node[0-9]+$' "$HOSTS_FILE" | wc -l | tr -d ' ')
  fi
else
  if ! [[ "$NODE_COUNT_INPUT" =~ ^[0-9]+$ ]] || (( NODE_COUNT_INPUT <= 0 )); then
    echo "Node count must be a positive integer" >&2
    exit 1
  fi
  NODE_COUNT="$NODE_COUNT_INPUT"
fi

if (( NODE_COUNT == 0 )); then
  echo "No node entries found in hosts.txt" >&2
  exit 1
fi

declare -A NODE_IPS_PRIV   # nodeX -> private IP
declare -A NODE_IPS_PUB    # nodeX -> public IP (only for validation, not written to NODE_HOSTS)

# Read hosts.txt (example format:
#  54.193.104.199 node0
#  ...
#  172.31.21.161 node0-private
#  ...)
while read -r ip name; do
  [[ -z "$ip" || "$ip" =~ ^# ]] && continue

  if [[ "$name" =~ ^node([0-9]+)-private$ ]]; then
    idx="${BASH_REMATCH[1]}"
    NODE_IPS_PRIV[$idx]="$ip"
  elif [[ "$name" =~ ^node([0-9]+)$ ]]; then
    idx="${BASH_REMATCH[1]}"
    NODE_IPS_PUB[$idx]="$ip"
  fi
done < "$HOSTS_FILE"

# Decide which IP set to use (private for intra-region, public for cross-region)
declare -A NODE_IPS_SELECTED

if [[ "$NODE_IP_SOURCE" == "public" ]]; then
  for (( i = 0; i < NODE_COUNT; ++i )); do
    if [[ -z "${NODE_IPS_PUB[$i]:-}" ]]; then
      echo "hosts.txt is missing a public IP for node${i}" >&2
      exit 1
    fi
    NODE_IPS_SELECTED[$i]="${NODE_IPS_PUB[$i]}"
  done
else
  # Default to private IPs; enforce presence for every node
  if (( ${#NODE_IPS_PRIV[@]} == 0 )); then
    echo "hosts.txt is missing 'nodeX-private' entries" >&2
    exit 1
  fi
  for (( i = 0; i < NODE_COUNT; ++i )); do
    if [[ -z "${NODE_IPS_PRIV[$i]:-}" ]]; then
      echo "hosts.txt is missing a private IP for node${i}-private" >&2
      exit 1
    fi
    NODE_IPS_SELECTED[$i]="${NODE_IPS_PRIV[$i]}"
  done
fi

# Build NODE_HOSTS using the selected IP family
NODE_HOSTS=""
for (( i = 0; i < NODE_COUNT; ++i )); do
  entry="node${i}:${NODE_IPS_SELECTED[$i]}"
  if [[ -z "$NODE_HOSTS" ]]; then
    NODE_HOSTS="$entry"
  else
    NODE_HOSTS+=",$entry"
  fi
done

mkdir -p "$ENVS_DIR"

for (( i = 0; i < NODE_COUNT; ++i )); do
  env_file="$ENVS_DIR/node${i}.env"
  cat > "$env_file" <<EOF
NODE_ID=${i}
NODE_LEAST_ID=0
NODE_NUM=${NODE_COUNT}
NODE_PORT=10000
NODE_HOSTS=${NODE_HOSTS}
EOF
  echo "Generated ${env_file}"
done

# Generate client env file
client_env_file="$ENVS_DIR/client.env"
cat > "$client_env_file" <<EOF
CLIENT_ID=client
CLIENT_MODE=load_test
CLIENT_ORDERING_MODE=pompe
NODE_LEAST_ID=0
NODE_NUM=${NODE_COUNT}
NODE_HOSTS=${NODE_HOSTS}
TEST_DURATION=180
POMPE_ENABLE=true
POMPE_BATCH_SIZE=1
POMPE_LEADER_NODE_ID=0
RUST_LOG=warn
EOF
echo "Generated ${client_env_file}"

echo "Generated ${NODE_COUNT} node configuration files (NODE_HOSTS uses ${NODE_IP_SOURCE} IPs)."
