# Kafka InfluxDB LVC and statistics

## Environment variables

| Variable | Purpose |
| --- | --- |
| GAPIT_KAFKALVC_BROKERS | Brokers to connect to, separated by comma (default: "localhost:9092"") |
| GAPIT_KAFKALVC_TOPICS | Kafka topics to subscribe to (default: "instruments") |
| GAPIT_KAFKALVC_GROUP_ID | Kafka consumer group name (default: "stats_lvc_1") |
| GAPIT_KAFKALVC_INACTIVITY_THRESHOLD | How old data must be to be considered inactive (default: 60) |
| GAPIT_KAFKALVC_OUTPUT_REPORTED_HOST | Hostname to set as tag (default: none, Telegraf can set default) |
| GAPIT_KAFKALVC_BIND_IP | IP to bind to (default: "0.0.0.0") |
| GAPIT_KAFKALVC_PORT | Port to bind to (default: 3005) |
| GAPIT_KAFKALVC_LOG_CONF | Logging level for rdkafka (default: None) |

It's usually required to set the following environment variables in Docker:

```yaml
environment:
  GAPIT_KAFKALVC_BROKERS: "localhost:9092"
  GAPIT_KAFKALVC_TOPICS: "instruments"
  GAPIT_KAFKALVC_GROUP_ID: "stats_lvc_1"
  GAPIT_KAVKAVLC_INACTIVITY_THRESHOLD: 120
```
