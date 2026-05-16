#![warn(clippy::pedantic)]
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::message::{Headers, Message};
use rdkafka::{Offset, TopicPartitionList};

use influxdb_line_protocol::ParsedLine;

use crate::Precision;
use crate::ValueCache;
use crate::curtime;
use log::{info, warn};
use std::sync::{Arc, Mutex};

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
pub(crate) struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("ConsumerContext Pre rebalance {rebalance:?}");
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("ConsumerContext Post rebalance {rebalance:?}");
    }
}

// A type alias with your custom consumer can be created for convenience.
pub(crate) type LoggingConsumer = StreamConsumer<CustomContext>;

#[allow(dead_code)]
#[allow(clippy::too_many_lines)]
pub(crate) async fn consume_and_process(
    brokers: String,
    group_id: String,
    topics: Vec<String>,
    stats: Arc<Mutex<ValueCache>>,
) {
    let context = CustomContext;

    let mut config = ClientConfig::new();

    config
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "30000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "latest")
        .set("client.id", "kafka-stats")
        //.set("statistics.interval.ms", "30000")
        .set_log_level(RDKafkaLogLevel::Warning);

    let topics = &topics
        .iter()
        .map(std::string::String::as_str)
        .collect::<Vec<_>>();

    let consumer: LoggingConsumer = config
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(topics)
        .expect("Can't subscribe to specified topics");

    // Reset offsets to latest.
    for topic in topics {
        let metadata = consumer
            .fetch_metadata(Some(topic), std::time::Duration::from_secs(5))
            .unwrap();
        let mut tpl = TopicPartitionList::new();
        for partition in metadata.topics()[0].partitions() {
            tpl.add_partition_offset(topic, partition.id(), Offset::End)
                .unwrap();
        }
        consumer.assign(&tpl).unwrap();
        // consumer.commit(&tpl, CommitMode::Sync).unwrap();
    }

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {e}"),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {e:?}");
                        ""
                    }
                };
                // Resetting offsets needs to be done after partitions have been assigned, that's why
                // this is inside the receiving loop.
                let timestamp_ms = curtime() * 1000;
                let parsed_lines = influxdb_line_protocol::parse_lines(payload);

                // Grab the lock and keep it for all lines in the message
                let mut valuecache = stats.lock().unwrap();
                valuecache.kafka_message_count += 1;

                // Set fallback timestamp from Kafka message timestamp, or system timestamp in
                // correct precision.
                let kafka_or_sys_timestamp = if valuecache.precision == Precision::Second {
                    timestamp_ms / 1000
                } else if valuecache.precision == Precision::Nanosecond {
                    timestamp_ms * 1_000_000
                } else {
                    panic!()
                };
                for parsed_line in parsed_lines {
                    match parsed_line {
                        Err(e) => println!("Skipping unparseable line: {e}"),
                        Ok(parsed_line) => {
                            let ParsedLine {
                                series,
                                field_set,
                                timestamp,
                            } = parsed_line;

                            // We must be able to read uid from tags, empty tag_set => no uid tag
                            if series.tag_set.is_none() {
                                continue;
                            }
                            let tags = series.tag_set.unwrap();

                            let uid = tags.iter().find(|&x| x.0 == "uid");
                            if uid.is_none() {
                                println!("Skipping line without uid tag");
                                continue;
                            }
                            let equipment_tag = tags
                                .iter()
                                .find(|&x| x.0 == "Equipment-tag")
                                .map(|x| x.1.to_string());

                            valuecache.add_or_update_fields(
                                &uid.unwrap().1,
                                equipment_tag,
                                &field_set,
                                timestamp.unwrap_or(kafka_or_sys_timestamp),
                                &series.measurement,
                            );
                            valuecache.ilp_line_count += 1;
                        }
                    }
                }
                std::mem::drop(valuecache);
                if let Some(headers) = m.headers() {
                    for header in headers.iter() {
                        info!("XXX  Header {:#?}: {:?}", header.key, header.value);
                    }
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        }
    }
}
