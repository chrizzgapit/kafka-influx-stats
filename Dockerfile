FROM rust:1.93.0-trixie AS builder

RUN apt-get update && apt-get install -y clang cmake

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

RUN mkdir src && echo "fn main() {println!(\"Hello\");}" > src/main.rs
RUN cargo build --release
RUN rm -rf target/release/deps/kafka_influx_stats* src

COPY src/ ./src

RUN find . | grep -v target
RUN cargo build --release

FROM debian:trixie-slim

WORKDIR /app

COPY --from=builder /app/target/release/kafka-influx-stats ./kafka-influx-stats

EXPOSE 3005

CMD ["./kafka-influx-stats"]
