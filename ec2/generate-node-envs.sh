#!/usr/bin/env bash
# generate-node-envs.sh
# 用于批量生成 ec2/envs/node{X}.env 配置文件（NODE_HOSTS 使用私网 IP）

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOSTS_FILE="$ROOT_DIR/hosts.txt"
ENVS_DIR="$ROOT_DIR/envs"

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "❌ hosts.txt 不存在: $HOSTS_FILE" >&2
  exit 1
fi

# ── 计算节点数量：优先按照 nodeX-private 行计数；否则按 nodeX 行计数 ──
NODE_COUNT_INPUT="${1:-}"
if [[ -z "$NODE_COUNT_INPUT" ]]; then
  NODE_COUNT=$(grep -E 'node[0-9]+-private$' "$HOSTS_FILE" | wc -l | tr -d ' ')
  if (( NODE_COUNT == 0 )); then
    NODE_COUNT=$(grep -E 'node[0-9]+$' "$HOSTS_FILE" | wc -l | tr -d ' ')
  fi
else
  if ! [[ "$NODE_COUNT_INPUT" =~ ^[0-9]+$ ]] || (( NODE_COUNT_INPUT <= 0 )); then
    echo "❌ 节点数量必须是正整数" >&2
    exit 1
  fi
  NODE_COUNT="$NODE_COUNT_INPUT"
fi

declare -A NODE_IPS_PRIV   # nodeX -> private IP
declare -A NODE_IPS_PUB    # nodeX -> public IP（仅用于兜底/校验，不写入 NODE_HOSTS）

# 读取 hosts.txt（格式示例：
#  54.193.104.199 node0
#  ...
#  172.31.21.161 node0-private
#  ...）
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

# 如果没有任何私网行，回退用公网行生成 NODE_HOSTS（不推荐，但保证可用）
if (( ${#NODE_IPS_PRIV[@]} == 0 )); then
  echo "⚠️  未找到 'nodeX-private' 行，回退使用公网 IP 生成 NODE_HOSTS" >&2
  for (( i = 0; i < NODE_COUNT; ++i )); do
    if [[ -z "${NODE_IPS_PUB[$i]:-}" ]]; then
      echo "❌ hosts.txt 中缺少 node${i} 的 IP（既无 node${i} 也无 node${i}-private）" >&2
      exit 1
    fi
    NODE_IPS_PRIV[$i]="${NODE_IPS_PUB[$i]}"
  done
else
  # 有私网行时，确保 0..NODE_COUNT-1 全都有
  for (( i = 0; i < NODE_COUNT; ++i )); do
    if [[ -z "${NODE_IPS_PRIV[$i]:-}" ]]; then
      echo "❌ hosts.txt 中缺少 node${i}-private 的私网 IP" >&2
      exit 1
    fi
  done
fi

# ── 拼接 NODE_HOSTS（使用私网 IP） ──
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
SMROL_DISABLE_THRESHOLD_SIG=1
RUST_LOG=warn
EOF
  echo "✅ 生成 ${env_file}"
done

echo "🎉 共生成 ${NODE_COUNT} 个节点配置文件（NODE_HOSTS 使用私网 IP）。"