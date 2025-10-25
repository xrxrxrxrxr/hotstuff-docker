#!/bin/bash
# init-ec2.sh - 安装 Docker 和 Docker Compose

SSH_KEY=~/.ssh/xrui.pem
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HOSTS_FILE="$SCRIPT_DIR/hosts.txt"

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "hosts.txt not found at $HOSTS_FILE" >&2
  exit 1
fi

mapfile -t NODES < <(awk '/node[0-9]+$/ {print "ubuntu@"$1}' "$HOSTS_FILE")

if [[ ${#NODES[@]} -eq 0 ]]; then
  echo "No node entries found in hosts.txt" >&2
  exit 1
fi

echo "🔧 初始化 EC2 实例（安装 Docker）..."

for node in "${NODES[@]}"; do
  echo "→ 初始化 $node..."
  ssh $SSH_OPTS $node "
    set -e
    # 更新系统
    sudo apt-get update -y

    # 安装 Docker
    sudo apt-get install -y docker.io

    # 启动 Docker
    sudo systemctl start docker
    sudo systemctl enable docker

    # 添加当前用户到 docker 组
    sudo usermod -aG docker ubuntu

    # 移除旧版 docker-compose（如果存在）
    sudo rm -f /usr/local/bin/docker-compose

    # 安装依赖
    sudo apt-get install -y ca-certificates curl gnupg lsb-release

    # 添加 Docker 官方 GPG key
    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

    # 添加 Docker 官方 apt 仓库
    echo \"deb [arch=\$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \$(lsb_release -cs) stable\" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    # 更新 apt 并安装 docker compose 插件
    sudo apt-get update -y
    sudo apt-get install -y docker-compose-plugin

    # 验证安装
    docker --version
    docker compose version
  " &
done

wait
echo ""
echo "✅ 所有实例初始化完成！"
echo "⚠️  注意：需要重新登录才能使 docker 组权限生效"
echo ""
echo "下一步："
echo "  make deploy   # 部署配置文件"
echo "  make start    # 启动实验"