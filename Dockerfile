# Use the official Rust image as the base image
FROM rust:latest

# Set the working directory inside the container
WORKDIR /xr

# Copy the Cargo.toml and Cargo.lock files for both `hotstuff_rs` and `hotstuff_runner`
COPY hotstuff_rs/Cargo.toml hotstuff_rs/Cargo.lock ./hotstuff_rs/
COPY hotstuff_runner/Cargo.toml hotstuff_runner/Cargo.lock ./hotstuff_runner/

# Create a dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Pre-build dependencies
RUN cargo build --release && rm -rf src

# Copy the source code into the container
COPY . .

# Build the `hotstuff_runner` binary
WORKDIR /xr/hotstuff_runner
RUN cargo build --release

# Set the command to run the application
CMD ["./target/release/hotstuff_runner"]
