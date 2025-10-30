#!/bin/bash
set -e

RUSTFLAGS="-C opt-level=3 -C target-cpu=native -C target-feature=+avx2,+avx512f,+bmi2,+adx -C lto=fat -C codegen-units=1" \
docker buildx build --platform linux/amd64 -t georgiaxr/hs-test:latest --push .