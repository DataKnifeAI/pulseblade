# Pulseblade image. The default command is the hub; override it with `node`.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p pulseblade

FROM debian:bookworm-slim
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
