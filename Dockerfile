# syntax=docker/dockerfile:1.7
# Imagine unică cu toate binarele: webapp, cache-node, loadgen.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --workspace \
 && mkdir /out && cp target/release/webapp target/release/cache-node target/release/loadgen /out/

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 app
COPY --from=build /out/ /usr/local/bin/
USER app
WORKDIR /work
EXPOSE 8080 7000
CMD ["webapp"]
