#!/bin/bash
# run_test.sh - Quickly launch the HotStuff Docker cluster

set -e

CLIENT_MODE="load_test"
PROFILE_MODE=false
REBUILD_MODE=true

POSITIONAL_ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --reuse|--no-build|--skip-build)
            REBUILD_MODE=false
            shift
            ;;
        --rebuild)
            REBUILD_MODE=true
            shift
            ;;
        *)
            POSITIONAL_ARGS+=("$1")
            shift
            ;;
    esac
done

if [ ${#POSITIONAL_ARGS[@]} -gt 0 ]; then
    set -- "${POSITIONAL_ARGS[@]}"
else
    set --
fi

if [ "$1" = "profile_node0" ] || [ "$1" = "profile" ]; then
    PROFILE_MODE=true
elif [ "$1" = "load_test" ] || [ "$1" = "load" ]; then
    CLIENT_MODE="load_test"
elif [ "$1" = "perf_test" ] || [ "$1" = "perf" ]; then
    CLIENT_MODE="perf_test"
elif [ "$1" = "interactive" ] || [ "$1" = "client" ]; then
    CLIENT_MODE="interactive"
elif [ -n "$1" ]; then
    echo "Invalid client mode: $1"
    echo "Usage: $0 [interactive|load_test|perf_test|profile_node0] [--rebuild|--reuse]"
    exit 1
fi

if [ ! -f "docker-compose.yml" ]; then
    echo "Please run this script from the directory containing docker-compose.yml"
    exit 1
fi
if [ ! -d "hotstuff_runner" ]; then
    echo "hotstuff_runner directory not found"
    exit 1
fi

rm -rf ./logs
mkdir -p ./logs

set -a
source .env
set +a

PROFILE_CONTAINER="smrol_profile_node0"
CLIENT_SERVICE=""

COMPOSE_UP_FLAGS=(-d)
if $REBUILD_MODE; then
    COMPOSE_UP_FLAGS=(--build -d)
fi
if $REBUILD_MODE; then
    echo "Building and starting the cluster..."
else
    echo "Starting the cluster by reusing existing images..."
fi
if $PROFILE_MODE; then
    # start other nodes (node1..)
    other_nodes=()
    for idx in $(seq 1 $((NODE_NUM - 1))); do
        other_nodes+=("node${idx}")
    done
    if [ ${#other_nodes[@]} -gt 0 ]; then
        docker-compose up "${COMPOSE_UP_FLAGS[@]}" "${other_nodes[@]}"
    fi
else
    case $CLIENT_MODE in
        "interactive")
            docker-compose --profile interactive up "${COMPOSE_UP_FLAGS[@]}"
            CLIENT_SERVICE="client"
            ;;
        "load_test")
            docker-compose --profile load_test up "${COMPOSE_UP_FLAGS[@]}"
            CLIENT_SERVICE="load_tester"
            ;;
        "perf_test")
            docker-compose --profile perf_test up "${COMPOSE_UP_FLAGS[@]}"
            CLIENT_SERVICE="perf_tester"
            ;;
    esac
fi

if $PROFILE_MODE; then
    NETWORK_NAME=$(docker network ls --format '{{.Name}}' | grep '_hotstuff_network$' | head -n1)
    if [ -z "$NETWORK_NAME" ]; then
        echo "Docker network (hotstuff_network) not found"
        exit 1
    fi

    docker rm -f "$PROFILE_CONTAINER" >/dev/null 2>&1 || true

    echo "Starting node0 profiling container (cargo profiler)..."
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

    docker-compose up "${COMPOSE_UP_FLAGS[@]}" load_tester
    CLIENT_SERVICE="load_tester"
fi

echo "Waiting for nodes to initialize..."
sleep 15

end_id=$((NODE_LEAST_ID + NODE_NUM - 1))
echo "Checking node health..."
for node_id in $(seq $NODE_LEAST_ID $end_id); do
    echo -n "  node $node_id is: "
    if $PROFILE_MODE && [ "$node_id" -eq 0 ]; then
        if docker ps --filter "name=$PROFILE_CONTAINER" --filter "status=running" | grep -q "$PROFILE_CONTAINER"; then
            echo "✅ profiling (container: $PROFILE_CONTAINER)"
        else
            echo "Profiler is not running"
        fi
        continue
    fi
    if docker ps --filter "name=hotstuff_node$node_id" --filter "status=running" | grep -q "hotstuff_node$node_id"; then
        echo "✅ running"
    else
        echo "❌ down"
    fi
done

echo "Checking client health..."
if [ -n "$CLIENT_SERVICE" ]; then
    echo -n "  Client ($CLIENT_SERVICE): "
    if docker-compose ps $CLIENT_SERVICE | grep -q "Up"; then
        echo "running"
    else
        echo "error"
    fi
else
    echo "  Client: (not started)"
fi

echo ""
echo "Cluster startup complete!"
if $PROFILE_MODE; then
    echo "Profiling mode: node0 runs via cargo profiler time"
    echo "   View profiler output: docker logs -f $PROFILE_CONTAINER"
fi

echo "Tokio Console: node port mapping = node0:6660, node1:6661, node2:6662, node3:6663"
echo "   Example: tokio-console --connect 127.0.0.1:6660"

echo ""
case $CLIENT_MODE in
    "interactive")
        echo "Interactive client started; you can submit transactions manually"
        ;;
    "load_test")
        echo "Load test started ($TARGET_TPS, runs for 5 minutes)"
        echo "   View test progress: docker-compose logs -f load_tester"
        ;;
    "perf_test")
        echo "Performance test started (400 TPS, runs for 5 minutes)"
        echo "   View test progress: docker-compose logs -f perf_tester"
        ;;
esac

if $PROFILE_MODE; then
    echo "Profiling reports will be stored in hotstuff_runner/target/perf/"
    echo "   After completion open time-*.html in a browser (for example target/perf/time-report.html)"
fi

echo "Checking results after running for 30 seconds..."
sleep 30

echo "Checking Pompe processing output..."
docker-compose logs | grep "to HotStuff queue" | head -10 || true

echo "Checking transaction ordering output..."
docker-compose logs | grep "pompe:.*:" | head -5 || true

echo ""
echo "Pompe functional test completed!"
if $PROFILE_MODE; then
    echo "Profiling container ($PROFILE_CONTAINER) can be removed manually after analysis: docker rm -f $PROFILE_CONTAINER"
    echo "Profiling report path: hotstuff_runner/target/perf/"
fi
echo "Stopping the test: docker-compose --profile \"*\" down"
echo "- Stops automatically after 2 minutes -"

sleep 180
# docker-compose --profile "*" down
./stop.sh
