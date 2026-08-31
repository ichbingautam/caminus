# Stage 1: Multi-stage Rust release builder
FROM rust:slim-bookworm AS builder

WORKDIR /usr/src/caminus

# Install RocksDB & native C++ build toolchain
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang \
    llvm-dev \
    libclang-dev \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

# Stage 2: Production runtime image
FROM debian:bookworm-slim AS runner

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/caminus/target/release/caminus /app/caminus

EXPOSE 9000

ENV RUST_LOG=info

ENTRYPOINT ["/app/caminus"]
