# Use the official Rust image as the base image
FROM rust:latest

# Set the working directory inside the container
WORKDIR /app

# # Copy the Cargo.toml and Cargo.lock files for both `hotstuff_rs` and `hotstuff_runner`
# COPY hotstuff_rs/Cargo.toml hotstuff_rs/Cargo.lock ./hotstuff_rs/
# COPY hotstuff_runner/Cargo.toml hotstuff_runner/Cargo.lock ./hotstuff_runner/


# Copy the source code into the container
COPY . .

# Build the `hotstuff_runner` binary
RUN cd hotstuff_runner && cargo build --release

# Set the command to run the application
CMD ["./hotstuff_runner/target/release/node"]
