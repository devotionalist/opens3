# syntax=docker/dockerfile:1.6

FROM rust:1.89-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY proto ./proto
COPY admin ./admin
RUN cargo build --release -p opens3 && strip target/release/opens3

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 -g root opens3
COPY --from=builder /src/target/release/opens3 /usr/local/bin/opens3
USER 10001
EXPOSE 9333 8080 8333 8888
ENTRYPOINT ["/usr/local/bin/opens3"]
CMD ["--help"]
