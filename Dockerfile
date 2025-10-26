# Dockerfile
FROM rust:latest as builder

ARG ENABLE_TOKIO_CONSOLE=0
ENV ENABLE_TOKIO_CONSOLE=${ENABLE_TOKIO_CONSOLE}

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Step 1: copy Cargo manifests first (for dependency caching)
COPY hotstuff_runner/Cargo.toml hotstuff_runner/Cargo.toml
COPY hotstuff_runner/Cargo.lock hotstuff_runner/Cargo.lock
COPY hotstuff_rs/Cargo.toml     hotstuff_rs/Cargo.toml
COPY hotstuff_rs/Cargo.lock     hotstuff_rs/Cargo.lock
COPY hotstuff_rs/src            hotstuff_rs/src

# Step 2: prebuild dependencies
RUN mkdir -p hotstuff_runner/src/bin \
    && for bin in docker_node client docker_node_adversary; do \
        printf 'fn main() {}\n' > "hotstuff_runner/src/bin/${bin}.rs"; \
    done

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    if [ "$ENABLE_TOKIO_CONSOLE" = "1" ]; then \
        export RUSTFLAGS="--cfg tokio_unstable"; \
    else \
        unset RUSTFLAGS; \
    fi; \
    cargo build --manifest-path hotstuff_runner/Cargo.toml --release --locked --bins

# Copy the entire project (including the hotstuff_runner subdirectory)
COPY . .

# Enter hotstuff_runner and build the binaries
WORKDIR /app/hotstuff_runner
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    if [ "$ENABLE_TOKIO_CONSOLE" = "1" ]; then \
        export RUSTFLAGS="--cfg tokio_unstable"; \
    else \
        unset RUSTFLAGS; \
    fi; \
    cargo build --release --bin docker_node && \
    cargo build --release --bin client && \
    cargo build --release --bin docker_node_adversary

# Runtime image
FROM ubuntu:22.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Create application user
RUN useradd -r -s /bin/false hotstuff

# Copy the compiled binaries
COPY --from=builder /app/hotstuff_runner/target/release/docker_node /usr/local/bin/docker_node
COPY --from=builder /app/hotstuff_runner/target/release/client /usr/local/bin/client
COPY --from=builder /app/hotstuff_runner/target/release/docker_node_adversary /usr/local/bin/docker_node_adversary


# Switch to the application user
# USER hotstuff

# # Set default environment variables
# ENV NODE_ID=0
# ENV NODE_PORT=8000

# # Expose ports
# EXPOSE 8000

# Startup command
CMD ["docker_node"]
