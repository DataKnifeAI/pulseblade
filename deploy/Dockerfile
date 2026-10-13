# Pulseblade image. The default command is the hub; override it with `node`.
# Dependency crates compile from a cargo-chef recipe, so a source change
# reuses that layer. CARGO_BUILD_JOBS matches the GitLab job CPU limit.
# BuildKit otherwise sees the node CPU count and oversubscribes the cgroup.
FROM lukemathwalker/cargo-chef:latest-rust-1.99.0-bookworm@sha256:2ee6e8edf0b91b5a7295071299596601710bccb243bd9735e0b9abf6e114582b AS chef
WORKDIR /src

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
ENV CARGO_BUILD_JOBS=2
COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json -p pulseblade
COPY . .
RUN cargo chef cook --release --locked --recipe-path recipe.json -p pulseblade

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
# k8s and proxmox enable ureq's native-tls feature, so the binary needs libssl3.
# ca-certificates covers TLS when no PEM bundle is configured.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --home-dir /var/lib/pulseblade pulseblade
COPY --from=build /src/target/release/pulseblade /usr/local/bin/pulseblade
USER pulseblade
WORKDIR /var/lib/pulseblade
EXPOSE 7170
ENTRYPOINT ["pulseblade"]
CMD ["hub"]
