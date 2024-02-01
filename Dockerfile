# Using the `rust-musl-builder` as base image, instead of 
# the official Rust toolchain
FROM clux/muslrust:1.75.0-stable AS chef
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

FROM alpine:3.19.0 AS runtime
#RUN addgroup -S myuser && adduser -S myuser -G myuser
RUN apk add --update openssl \
    && apk --no-cache -U -a upgrade
RUN apk add git gcc g++ make cmake && \
    export CXX=g++ CC=gcc && \
    # lightgbm
    git clone --recursive --branch stable --depth 1 https://github.com/Microsoft/LightGBM && \
    cd ./LightGBM && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make -j4 && \
    make install && \
    cd "${HOME}" && \
    rm -rf LightGBM
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/neusa /usr/local/bin/
COPY . .
#USER myuser

CMD RUST_LOG=debug /usr/local/bin/neusa

# docker build -t neusa .
# docker tag neusa asia-northeast1-docker.pkg.dev/neusa-a919b/neusa/neusa-hft:latest
# docker push asia-northeast1-docker.pkg.dev/neusa-a919b/neusa/neusa-hft

# cargo run
# cargo run -- --task split
# cargo run -- --task train

# https://api.binance.com California, USA
# https://api-gcp.binance.com Missouri, USA

# asia-northeast1 (Tokyo)
# https://api1.binance.com Tokyo, Japan
# https://api2.binance.com Tokyo, Japan
# https://api3.binance.com Tokyo, Japan
# https://api4.binance.com Tokyo, Japan