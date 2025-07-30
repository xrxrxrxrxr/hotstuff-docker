#!/bin/bash

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting HotStuff Docker Test${NC}"

# 检查必要的文件是否存在
echo "Checking project structure..."

if [ ! -d "hotstuff_rs" ]; then
    echo -e "${RED}Error: hotstuff_rs directory not found${NC}"
    exit 1
fi

if [ ! -d "hotstuff_runner" ]; then
    echo -e "${RED}Error: hotstuff_runner directory not found${NC}"
    exit 1
fi

if [ ! -f "hotstuff_runner/Cargo.toml" ]; then
    echo -e "${RED}Error: hotstuff_runner/Cargo.toml not found${NC}"
    exit 1
fi

if [ ! -f "hotstuff_runner/src/main.rs" ]; then
    echo -e "${RED}Error: hotstuff_runner/src/main.rs not found${NC}"
    exit 1
fi

echo -e "${GREEN}Project structure looks good!${NC}"

# 创建日志目录
mkdir -p logs

# 清理之前的容器
echo "Cleaning up previous containers..."
docker-compose -f docker-compose.test.yml down

# 构建并运行测试
echo -e "${YELLOW}Building and running test container...${NC}"
docker-compose -f docker-compose.test.yml up --build

echo -e "${GREEN}Test completed!${NC}"