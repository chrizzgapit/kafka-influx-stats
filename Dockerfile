FROM rust:1.93.0-trixie AS builder

RUN apt-get update && apt-get install --no-install-recommends -y clang cmake && apt-get clean && rm -rf /var/lib/apt/lists

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

RUN mkdir src && echo "fn main() {println!(\"Hello\");}" > src/main.rs && cargo build --release && rm -rf target/release/deps/kafka_influx_stats* src
#RUN cargo build --release
#RUN rm -rf target/release/deps/kafka_influx_stats* src

COPY src/ ./src

#RUN find . | grep -v target
RUN cargo build --release

FROM debian:trixie-slim

USER 1000

WORKDIR /app

COPY --from=builder /app/target/release/kafka-influx-stats ./kafka-influx-stats

EXPOSE 3005
HEALTHCHECK --start-period=30s CMD ["curl", "--fail", "http://localhost:3005", "||", "exit", "1"]
CMD ["./kafka-influx-stats"]
