#!/usr/bin/env bash
# generate-node-envs.sh
# Generate ec2/envs/node{X}.env files in bulk (NODE_HOSTS uses private IPs)

set -euo pipefail

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
  NODE_COUNT=$(grep -E 'node[0-9]+-private$' "$HOSTS_FILE" | wc -l | tr -d ' ')
  if (( NODE_COUNT == 0 )); then
    NODE_COUNT=$(grep -E 'node[0-9]+$' "$HOSTS_FILE" | wc -l | tr -d ' ')
  fi
else
  if ! [[ "$NODE_COUNT_INPUT" =~ ^[0-9]+$ ]] || (( NODE_COUNT_INPUT <= 0 )); then
    echo "Node count must be a positive integer" >&2
    exit 1
  fi
  NODE_COUNT="$NODE_COUNT_INPUT"
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

# If there are no private entries, fall back to public entries to build NODE_HOSTS (not recommended but works)
if (( ${#NODE_IPS_PRIV[@]} == 0 )); then
  echo "Warning: no 'nodeX-private' entries found; falling back to public IPs for NODE_HOSTS" >&2
  for (( i = 0; i < NODE_COUNT; ++i )); do
    if [[ -z "${NODE_IPS_PUB[$i]:-}" ]]; then
      echo "hosts.txt is missing an IP for node${i} (neither node${i} nor node${i}-private exists)" >&2
      exit 1
    fi
    NODE_IPS_PRIV[$i]="${NODE_IPS_PUB[$i]}"
  done
else
  # When private entries exist, ensure 0..NODE_COUNT-1 are all present
  for (( i = 0; i < NODE_COUNT; ++i )); do
    if [[ -z "${NODE_IPS_PRIV[$i]:-}" ]]; then
      echo "hosts.txt is missing a private IP for node${i}-private" >&2
      exit 1
    fi
  done
fi

# Build NODE_HOSTS using private IPs
NODE_HOSTS=""
for (( i = 0; i < NODE_COUNT; ++i )); do
  entry="node${i}:${NODE_IPS_PRIV[$i]}"
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
POMPE_ENABLE=true
POMPE_BATCH_SIZE=1
POMPE_STABLE_PERIOD_MS=500
POMPE_LIVENESS_DELTA_MS=400
POMPE_LEADER_NODE_ID=0
SMROL_PNFIFO_THRESHOLD=3
SMROL_DISABLE_MULTISIG=0
SMROL_TOKIO_CORES=0-1
SMROL_RT_CORES=0-1
TOKIO_NETWORK_RT_CORES=0-1
POMPE_RT_CORES=0-1
SMROL_PNFIFO_THRESHOLD_WORKERS=2
SMROL_PNFIFO_THRESHOLD_CORES=0-1
SMROL_SEQ_OFFLOAD_WORKERS=2
SMROL_SEQ_OFFLOAD_CORES=0-1
SMROL_MULTISIG_COMBINE_CORES=0-1
SMROL_MULTISIG_VERIFY_MEDIAN_CORES=2-3
SMROL_MULTISIG_VERIFY_MEDIAN_WORKERS=1
SMROL_MULTISIG_VERIFY_COMBINED_CORES=2-3
SMROL_MULTISIG_VERIFY_COMBINED_WORKERS=1
SMROL_MEDIAN_INFLIGHT=3
SMROL_ORDER_FINALIZE_INFLIGHT=3
SMROL_FINAL_SIGN_INFLIGHT=3
SMROL_FINAL_VERIFY_INFLIGHT=3
RUST_LOG=warn
EOF
  echo "Generated ${env_file}"
done

# Generate client env file
client_env_file="$ENVS_DIR/client.env"
cat > "$client_env_file" <<EOF
CLIENT_ID=client
CLIENT_MODE=load_test
CLIENT_ORDERING_MODE=smrol
NODE_LEAST_ID=0
NODE_NUM=${NODE_COUNT}
NODE_HOSTS=${NODE_HOSTS}
TARGET_TPS=400
TEST_DURATION=180
POMPE_ENABLE=true
POMPE_BATCH_SIZE=1
POMPE_STABLE_PERIOD_MS=500
POMPE_LIVENESS_DELTA_MS=400
POMPE_LEADER_NODE_ID=0
RUST_LOG=warn
EOF
echo "Generated ${client_env_file}"

echo "Generated ${NODE_COUNT} node configuration files (NODE_HOSTS uses private IPs)."
