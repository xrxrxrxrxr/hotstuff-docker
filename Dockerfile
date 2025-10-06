# Dockerfile
FROM rust:latest as builder

# 安装必要的系统依赖
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制整个项目（包括hotstuff_runner子目录）
COPY . .

# 进入hotstuff_runner目录并构建应用
WORKDIR /app/hotstuff_runner
RUN cargo build --release --bin docker_node
# RUN cargo build --release --bin docker_node_adversary
RUN cargo build --release --bin client

# 运行时镜像
FROM ubuntu:22.04

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    ca-certificates \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# 创建应用用户
RUN useradd -r -s /bin/false hotstuff

# 复制构建好的二进制文件
COPY --from=builder /app/hotstuff_runner/target/release/docker_node /usr/local/bin/docker_node
COPY --from=builder /app/hotstuff_runner/target/release/client /usr/local/bin/client
# COPY --from=builder /app/hotstuff_runner/target/release/docker_node_adversary /usr/local/bin/docker_node_adversary

# 切换到应用用户
# USER hotstuff

# # 设置默认环境变量
# ENV NODE_ID=0
# ENV NODE_PORT=8000

# # 暴露端口
# EXPOSE 8000

# 启动命令
CMD ["docker_node"]