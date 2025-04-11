FROM rust:1.86-slim as builder

WORKDIR /usr/src/app

# Some dependencies might need build tools for native components
# (Arrow, Parquet, compression libraries)
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . .

# Cache Cargo deps and build with optimal caching
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release


# Runtime image - use distroless for smallest secure image
FROM gcr.io/distroless/cc-debian12

# Copy binaries to a directory in PATH
COPY --from=builder /usr/src/app/target/release/s3-fast-list /usr/bin/
COPY --from=builder /usr/src/app/target/release/ks-tool /usr/bin/
