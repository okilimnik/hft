# Using the `rust-musl-builder` as base image, instead of 
# the official Rust toolchain
FROM clux/muslrust:stable AS chef
USER root
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Notice that we are specifying the --target flag!
RUN cargo chef cook --release --target x86_64-unknown-linux-musl --recipe-path recipe.json
COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl --bin neusa

FROM alpine AS runtime
RUN addgroup -S myuser && adduser -S myuser -G myuser
RUN apk add --update openssl \
    && apk --no-cache -U -a upgrade
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/neusa /usr/local/bin/
COPY . .
#USER myuser
CMD RUST_LOG=debug /usr/local/bin/neusa --task collect

# docker build -t neusa .
# docker tag neusa asia-northeast1-docker.pkg.dev/neusa-a919b/neusa/neusa-hft:latest
# docker push asia-northeast1-docker.pkg.dev/neusa-a919b/neusa/neusa-hft
# docker run -it --rm --name neusatest neusa

# RUST_LOG=debug target/release/neusa --task collect
# RUST_LOG=debug cargo run -- --task collect
# RUST_LOG=debug cargo run -- --task split
# RUST_LOG=debug cargo run -- --task train
# RUST_LOG=debug cargo run -- --task predict