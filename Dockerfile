FROM rust:1 as builder
WORKDIR /usr/src/app
COPY . .
RUN rustup component add rustfmt
RUN cargo install --path .

FROM debian:bullseye-slim
#RUN apt-get update && apt-get install -y extra-runtime-dependencies && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/neusa /usr/local/bin/neusa
RUN mkdir datasets
EXPOSE 8080
CMD ["neusa"]

# docker build -t neusa .
# docker tag neusa asia-northeast1-docker.pkg.dev/neusa-a919b/neusa/neusa-rs:latest
# docker push asia-northeast1-docker.pkg.dev/neusa-a919b/neusa/neusa-rs
# docker run -it --rm --name neusatest neusa