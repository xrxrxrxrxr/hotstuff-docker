#!/bin/bash
# run_test.sh - 快速启动HotStuff Docker集群

set -e

# 默认启动客户端
CLIENT_MODE="interactive"

# 检查命令行参数
if [ "$1" = "load_test" ] || [ "$1" = "load" ]; then
    CLIENT_MODE="load_test"
elif [ "$1" = "perf_test" ] || [ "$1" = "perf" ]; then
    CLIENT_MODE="perf_test"
elif [ "$1" = "interactive" ] || [ "$1" = "client" ]; then
    CLIENT_MODE="interactive"
elif [ -n "$1" ]; then
    echo "❌ 无效的客户端模式: $1"
    echo "使用方法: $0 [interactive|load_test|perf_test]"
    echo "  interactive (默认): 启动交互式客户端"
    echo "  load_test: 启动负载测试客户端 (400 TPS, 5分钟)"
    echo "  perf_test: 启动性能测试客户端 (400 TPS, 5分钟)"
    exit 1
fi

echo "🚀 开始 Pompe BFT 功能测试 - $CLIENT_MODE 模式"
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

# 根据模式选择不同的启动命令
echo "🏗️ 构建并启动集群..."
case $CLIENT_MODE in
    "interactive")
        docker-compose --profile interactive up --build -d
        CLIENT_SERVICE="client"
        ;;
    "load_test")
        docker-compose --profile load_test up --build -d
        CLIENT_SERVICE="load_tester"
        ;;
    "perf_test")
        docker-compose --profile perf_test up --build -d
        CLIENT_SERVICE="perf_tester"
        ;;
esac

echo "⏳ 等待节点初始化..."
sleep 15

set -a
source .env
set +a

end_id=$((NODE_LEAST_ID + NODE_NUM - 1))
# 检查健康状态
echo "🏥 检查节点健康状态..."
for i in $(seq $NODE_LEAST_ID $end_id); do
    echo -n "  Pompe node $i is: "
    if docker ps --filter "name=hotstuff_node$i" --filter "status=running" | grep -q "hotstuff_node$i"; then
        echo "✅ running"
    else
        echo "❌ down"
    fi
done

echo "🏥 检查客户端健康状态..."
echo -n "  客户端($CLIENT_SERVICE): "
if docker-compose ps $CLIENT_SERVICE | grep -q "Up"; then
    echo "✅ 运行中"
else
    echo "❌ 异常"
fi

echo ""
echo "🎉 集群启动完成！"
echo ""

# 根据客户端模式显示不同的提示
case $CLIENT_MODE in
    "interactive")
        echo "💡 交互式客户端已启动，你可以手动发送交易"
        ;;
    "load_test")
        echo "📊 负载测试已开始 ($TARGET_TPS, 持续 5 分钟)"
        echo "   查看测试进度: docker-compose logs -f load_tester"
        ;;
    "perf_test")
        echo "🚀 性能测试已开始 (400 TPS, 持续 5 分钟)"
        echo "   查看测试进度: docker-compose logs -f perf_tester"
        ;;
esac

# echo ""
# echo "💡 常用命令:"
# echo "  查看实时日志: docker-compose logs -f"
# echo "  查看客户端:   docker-compose logs -f $CLIENT_SERVICE"
# echo "  查看特定节点: docker-compose logs -f node0"
# echo "  重启集群:     docker-compose restart"
# echo "  停止集群:     docker-compose down"
# echo "  停止集群:     docker-compose --profile \"*\" down"
# echo "  查看状态:     docker-compose ps"


echo "⏱️ 运行 30 秒后检查结果..."
sleep 30

echo "📊 检查 Pompe 处理结果..."
docker-compose logs | grep "进入共识" | head -10

echo "🎯 检查交易排序结果..."
docker-compose logs | grep "pompe:.*:" | head -5

echo "📈 显示节点统计..."
for i in {0..3}; do
    echo "--- Node $i 统计 ---"
    docker-compose logs node$i | grep "Pompe状态" | tail -3
done

echo ""
echo "🎉 Pompe 功能测试完成!"
echo "📋 检查详细日志: docker-compose logs | grep Pompe"
echo "🛑 停止测试: docker-compose --profile \"*\" down"