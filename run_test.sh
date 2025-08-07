#!/bin/bash
# run_test.sh - 快速启动HotStuff Docker集群

set -e

echo "🚀 HotStuff Docker集群快速启动"
echo "================================"

# 检查是否在正确的目录
if [ ! -f "docker-compose.yml" ]; then
    echo "❌ 请在包含docker-compose.yml的目录中运行此脚本"
    exit 1
fi

if [ ! -d "hotstuff_runner" ]; then
    echo "❌ 找不到hotstuff_runner目录"
    exit 1
fi

rm -rf ./logs
mkdir -p ./logs

# echo "✅ 目录结构检查通过"

# 快速构建和启动
echo "🏗️ 构建并启动集群..."
docker-compose up --build -d

echo "⏳ 等待节点初始化..."
sleep 15

set -a
source .env
set +a

end_id=$((NODE_LEAST_ID + NODE_NUM - 1))
# 检查健康状态
echo "🏥 检查节点健康状态..."
for i in $(seq $NODE_LEAST_ID $end_id); do
    echo -n "  节点$i: "
    if docker ps --filter "name=hotstuff_node$i" --filter "status=running" | grep -q "hotstuff_node$i"; then
        echo "✅ 运行中"
    else
        echo "❌ 异常"
    fi
done

echo "🏥 检查客户端健康状态..."
echo -n "  客户端: "
    if docker-compose ps client | grep -q "Up"; then
        echo "✅ 运行中"
    else
        echo "❌ 异常"
    fi

echo ""
echo "🎉 集群启动完成！"
echo ""
echo "💡 常用命令:"
echo "  查看实时日志: docker-compose logs -f"
echo "  查看特定节点: docker-compose logs -f node0"
echo "  重启集群:     docker-compose restart"
echo "  停止集群:     docker-compose down"
echo "  查看状态:     docker-compose ps"