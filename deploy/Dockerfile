# Beacon: one image with every role and the built UI.

# --- UI: static assets ---------------------------------------------------
FROM node:24.11-bookworm-slim AS ui
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY ui/ ./
RUN npm run build

# --- Rust: release binaries ----------------------------------------------
FROM rust:1.91-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p beacon -p beacon-loadgen \
    && mkdir -p /out && cp target/release/beacon target/release/beacon-loadgen /out/

# --- Runtime ---------------------------------------------------------------
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /app beacon
WORKDIR /app
COPY --from=build /out/beacon /out/beacon-loadgen /usr/local/bin/
COPY --from=ui /ui/dist /app/ui/dist
COPY config /app/config
COPY samples /app/samples
USER beacon
ENV BEACON_CONFIG_DIR=/app/config BEACON_UI_DIR=/app/ui/dist RUST_LOG=info,sqlx=warn,librdkafka=warn,rdkafka=warn
EXPOSE 8080 8090
ENTRYPOINT ["beacon"]
CMD ["--migrate", "--role", "ingest,sources,matcher,aggregator,relay,api,router,delivery,gateway"]
