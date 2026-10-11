# Pulseblade image. The default command is the hub; override it with `node`.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p pulseblade

FROM debian:bookworm-slim
RUN useradd --system --create-home --home-dir /var/lib/pulseblade pulseblade
COPY --from=build /src/target/release/pulseblade /usr/local/bin/pulseblade
USER pulseblade
WORKDIR /var/lib/pulseblade
EXPOSE 7170
ENTRYPOINT ["pulseblade"]
CMD ["hub"]
