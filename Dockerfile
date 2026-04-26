# ---- Build stage ----
    FROM rust:1.89-slim-bookworm AS build
    WORKDIR /app
    
    # (optional) prime dependency cache
    COPY Cargo.toml Cargo.lock ./
    RUN mkdir -p src && echo "fn main(){}" > src/main.rs
    RUN --mount=type=cache,target=/usr/local/cargo/registry \
        --mount=type=cache,target=/app/target \
        cargo build --release || true
    
    # real sources
    COPY . .
    
    # If your binary target is named `cass` (change if different):
    #   - add --bin <name> if your package defines multiple binaries or you're in a workspace
    RUN --mount=type=cache,target=/usr/local/cargo/registry \
        --mount=type=cache,target=/app/target \
        cargo install --path . --bin cass --locked --root /out
    
    # ---- Runtime stage ----
    FROM debian:bookworm-slim
    # If needed: RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
    COPY --from=build /out/bin/cass /usr/local/bin/cass
    CMD ["cass"]
