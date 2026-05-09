# Kafka InfluxDB statistics

## Environment variables

| Variable | Purpose |
| --- | --- |
| GAPIT_KAFKASTATS_BROKERS | Brokers to connect to, separated by comma (default: "localhost:9092"") |
| GAPIT_KAFKASTATS_TOPICS | Kafka topics to subscribe to (default: "instruments") |
| GAPIT_KAFKASTATS_GROUP_ID | Kafka consumer group name (default: "kafkastats_1") |
| GAPIT_KAFKASTATS_PRECISION | Precision if timestamp in Kafka message (s or ns) |
| GAPIT_KAFKASTATS_INACTIVITY_THRESHOLD | How old data must be to be considered inactive (default: 60) |
| GAPIT_KAFKASTATS_OUTPUT_REPORTED_HOST | Hostname to set as tag (default: none, Telegraf can set default) |
| GAPIT_KAFKASTATS_BIND_IP | IP to bind to (default: "0.0.0.0") |
| GAPIT_KAFKASTATS_PORT | Port to bind to (default: 3005) |
| GAPIT_KAFKASTATS_TLS_ENABLE | Enable TLS |
| GAPIT_KAFKASTATS_TLS_KEYFILE | Path to file containing TLS key |
| GAPIT_KAFKASTATS_TLS_CERTFILE | Path to file containing TLS certificate |
| GAPIT_KAFKASTATS_LOG_CONF | Logging level for rdkafka (default: None) |

It's usually required to set the following environment variables in Docker:

```yaml
environment:
  GAPIT_KAFKASTATS_BROKERS: "localhost:9092"
  GAPIT_KAFKASTATS_TOPICS: "instruments"
  GAPIT_KAFKASTATS_GROUP_ID: "kafkastats"
  GAPIT_KAVKASTATS_INACTIVITY_THRESHOLD: 120
```
