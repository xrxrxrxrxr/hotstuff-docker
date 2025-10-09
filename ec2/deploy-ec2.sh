#!/usr/bin/env bash
# deploy-ec2.sh

SSH_KEY=~/.ssh/xrui.pem
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

# 读取 hosts.txt
declare -A PUBLIC_IPS PRIVATE_IPS

echo "📖 读取 hosts.txt..."
while IFS=' ' read -r ip name; do
  # 跳过注释和空行
  [[ $ip =~ ^#.*$ || -z $ip ]] && continue
  
  if [[ ! $name =~ -private$ ]]; then
    PUBLIC_IPS[$name]=$ip
    echo "  ✓ $name = $ip"
  else
    base_name=${name%-private}
    PRIVATE_IPS[$base_name]=$ip
  fi
done < hosts.txt

echo ""

# 检查是否读取成功
if [[ ${#PUBLIC_IPS[@]} -eq 0 ]]; then
  echo "❌ 错误：未能从 hosts.txt 读取任何 IP"
  echo "请检查 hosts.txt 文件格式"
  exit 1
fi

echo "🚀 开始部署到 EC2..."
echo ""

# 部署节点 0-3
for i in {0..3}; do
  ip="${PUBLIC_IPS[node$i]}"
  
  if [[ -z "$ip" ]]; then
    echo "❌ 错误：node$i 的 IP 未找到"
    exit 1
  fi
  
  echo "📦 部署 node$i ($ip)..."
  
  ssh $SSH_OPTS ubuntu@$ip "mkdir -p ~/hotstuff/logs" || {
    echo "❌ node$i 创建目录失败"
    exit 1
  }

  scp $SSH_OPTS docker-compose-node.yml ubuntu@$ip:~/hotstuff/docker-compose.yml || {
    echo "❌ node$i 上传 docker-compose.yml 失败"
    exit 1
  }
  # adversary node1 uses a different compose file
  # if [[ "$i" -eq 1 ]]; then
  #   scp $SSH_OPTS docker-compose-adv.yml ubuntu@$ip:~/hotstuff/docker-compose.yml || {
  #     echo "❌ node$i 上传 docker-compose-node-adv.yml 失败"
  #     exit 1
  #   }
  # else
  #   scp $SSH_OPTS docker-compose-node.yml ubuntu@$ip:~/hotstuff/docker-compose.yml || {
  #     echo "❌ node$i 上传 docker-compose-node.yml 失败"
  #     exit 1
  #   }
  # fi
  
  scp $SSH_OPTS envs/node$i.env ubuntu@$ip:~/hotstuff/.env || {
    echo "❌ node$i 上传 .env 失败"
    exit 1
  }
  
  echo "✅ node$i 部署完成"
  echo ""
done

# 部署客户端
client_ip="${PUBLIC_IPS[client]}"
if [[ -z "$client_ip" ]]; then
  echo "❌ 错误：client 的 IP 未找到"
  exit 1
fi

echo "📦 部署 client ($client_ip)..."
ssh $SSH_OPTS ubuntu@$client_ip "mkdir -p ~/hotstuff/logs"
scp $SSH_OPTS docker-compose-client.yml ubuntu@$client_ip:~/hotstuff/docker-compose.yml
scp $SSH_OPTS envs/client.env ubuntu@$client_ip:~/hotstuff/.env

echo ""
echo "✅ 所有文件部署完成！"