FROM rust:1.91.0-trixie AS builder

RUN apt-get update && apt-get install -y clang cmake

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

RUN mkdir src && echo "fn main() {println!(\"Hello\");}" > src/main.rs
RUN cargo build --release
RUN rm -rf target/release/deps/kafka_influx_lvc* src

COPY src/ ./src

RUN find . | grep -v target
RUN cargo build --release

FROM debian:trixie-slim

WORKDIR /app

COPY --from=builder /app/target/release/kafka-influx-lvc ./kafka-influx-lvc

EXPOSE 3005

CMD ["./kafka-influx-lvc"]
