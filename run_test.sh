#!/bin/bash
# run_test.sh - 快速启动HotStuff Docker集群

set -e

CLIENT_MODE="load_test"
PROFILE_MODE=false

if [ "$1" = "profile_node0" ] || [ "$1" = "profile" ]; then
    PROFILE_MODE=true
elif [ "$1" = "load_test" ] || [ "$1" = "load" ]; then
    CLIENT_MODE="load_test"
elif [ "$1" = "perf_test" ] || [ "$1" = "perf" ]; then
    CLIENT_MODE="perf_test"
elif [ "$1" = "interactive" ] || [ "$1" = "client" ]; then
    CLIENT_MODE="interactive"
elif [ -n "$1" ]; then
    echo "❌ 无效的客户端模式: $1"
    echo "使用方法: $0 [interactive|load_test|perf_test|profile_node0]"
    exit 1
fi

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

set -a
source .env
set +a

PROFILE_CONTAINER="smrol_profile_node0"
CLIENT_SERVICE=""

echo "🏗️ 构建并启动集群..."
if $PROFILE_MODE; then
    # start other nodes (node1..)
    other_nodes=()
    for idx in $(seq 1 $((NODE_NUM - 1))); do
        other_nodes+=("node${idx}")
    done
    if [ ${#other_nodes[@]} -gt 0 ]; then
        docker-compose up --build -d "${other_nodes[@]}"
    fi
else
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
fi

if $PROFILE_MODE; then
    NETWORK_NAME=$(docker network ls --format '{{.Name}}' | grep '_hotstuff_network$' | head -n1)
    if [ -z "$NETWORK_NAME" ]; then
        echo "❌ 未找到 Docker 网络 (hotstuff_network)"
        exit 1
    fi

    docker rm -f "$PROFILE_CONTAINER" >/dev/null 2>&1 || true

    echo "🚀 启动 node0 profiling 容器 (cargo profiler)..."
    docker run -d \
        --name "$PROFILE_CONTAINER" \
        --network "$NETWORK_NAME" \
        -v "$(pwd)":/workspace \
        -v "${HOME}/.cargo":/root/.cargo \
        -v "$(pwd)/hotstuff_runner/target":/workspace/hotstuff_runner/target \
        -w /workspace/hotstuff_runner \
        -e NODE_ID=0 \
        -e NODE_PORT=${NODE_PORT} \
        -e NODE_LEAST_ID=${NODE_LEAST_ID} \
        -e NODE_NUM=${NODE_NUM} \
        -e NODE_HOSTS=${NODE_HOSTS} \
        -e POMPE_ENABLE=${POMPE_ENABLE} \
        -e POMPE_BATCH_SIZE=${POMPE_BATCH_SIZE} \
        -e POMPE_STABLE_PERIOD_MS=${POMPE_STABLE_PERIOD_MS} \
        -e POMPE_LEADER_NODE_ID=${POMPE_LEADER_NODE_ID} \
        -e HS_MAX_VIEW_TIME_MS=${HS_MAX_VIEW_TIME_MS} \
        -e RUST_LOG=${LOG_LEVEL} \
        rust:latest \
        bash -lc "apt-get update >/dev/null && apt-get install -y --no-install-recommends linux-perf >/dev/null 2>&1 || true; \
                  cargo install cargo-profiler --force >/dev/null 2>&1; \
                  cargo profiler time --release --bin docker_node -- --node-id 0"

    # give profiler container time to compile/start
    sleep 20

    docker-compose up --build -d load_tester
    CLIENT_SERVICE="load_tester"
fi

echo "⏳ 等待节点初始化..."
sleep 15

end_id=$((NODE_LEAST_ID + NODE_NUM - 1))
echo "🏥 检查节点健康状态..."
for node_id in $(seq $NODE_LEAST_ID $end_id); do
    echo -n "  Pompe node $node_id is: "
    if $PROFILE_MODE && [ "$node_id" -eq 0 ]; then
        if docker ps --filter "name=$PROFILE_CONTAINER" --filter "status=running" | grep -q "$PROFILE_CONTAINER"; then
            echo "✅ profiling (container: $PROFILE_CONTAINER)"
        else
            echo "❌ profiler未运行"
        fi
        continue
    fi
    if docker ps --filter "name=hotstuff_node$node_id" --filter "status=running" | grep -q "hotstuff_node$node_id"; then
        echo "✅ running"
    else
        echo "❌ down"
    fi
done

echo "🏥 检查客户端健康状态..."
if [ -n "$CLIENT_SERVICE" ]; then
    echo -n "  客户端($CLIENT_SERVICE): "
    if docker-compose ps $CLIENT_SERVICE | grep -q "Up"; then
        echo "✅ 运行中"
    else
        echo "❌ 异常"
    fi
else
    echo "  客户端: (未启动)"
fi

echo ""
echo "🎉 集群启动完成！"
if $PROFILE_MODE; then
    echo "🔬 Profiling 模式: node0 由 cargo profiler time 运行"
    echo "   查看 profiler 输出: docker logs -f $PROFILE_CONTAINER"
fi

echo "🛰️ Tokio Console: 对应节点端口 = node0:6660, node1:6661, node2:6662, node3:6663"
echo "   示例: tokio-console --connect 127.0.0.1:6660"

echo ""
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

if $PROFILE_MODE; then
    echo "📈 Profiling 报告会生成在 hotstuff_runner/target/perf/ 目录"
    echo "   运行结束后可用浏览器打开 time-*.html（例如 target/perf/time-report.html）"
fi

echo "⏱️ 运行 30 秒后检查结果..."
sleep 30

echo "📊 检查 Pompe 处理结果..."
docker-compose logs | grep "到HotStuff队列" | head -10 || true

echo "🎯 检查交易排序结果..."
docker-compose logs | grep "pompe:.*:" | head -5 || true

echo ""
echo "🎉 Pompe 功能测试完成!"
if $PROFILE_MODE; then
    echo "📝 Profiling container ($PROFILE_CONTAINER) 可在分析后手动移除: docker rm -f $PROFILE_CONTAINER"
    echo "🔍 Profiling 报告路径: hotstuff_runner/target/perf/"
fi
echo "🛑 停止测试: docker-compose --profile \"*\" down"
echo "- 2 分钟后自动停止 -"

sleep 180
docker-compose --profile "*" down
