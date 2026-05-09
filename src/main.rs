#![warn(clippy::pedantic)]
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Debug, Display, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use log::{info, warn};
use rdkafka::{Offset, TopicPartitionList};
use serde::Deserialize;
use smallvec::SmallVec;
use tokio::signal;

use axum::{Router, extract::Path, extract::State, http::StatusCode, routing::get};
use axum_server::tls_rustls::RustlsConfig;

use influxdb_line_protocol::{EscapedStr, FieldValue, ParsedLine};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::message::{Headers, Message};
use rdkafka::util::get_rdkafka_version;

use crate::stats_utils::setup_logger;
mod stats_utils;

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
struct CustomContext;

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
type LoggingConsumer = StreamConsumer<CustomContext>;

#[derive(Debug)]
#[allow(dead_code)]
struct ValueCache {
    output_measurement: String,
    precision: Precision,
    created_timestamp: i64,
    first_value_ts: i64,
    last_value_ts: i64,
    minimum_inactivity_seconds: u64,
    kafka_message_count: usize,
    ilp_line_count: usize,
    fields_seen_count: usize,
    fields_changed_count: usize,
    uids: HashMap<String, UidInfo>,
    // field_names: HashSet<String>,
}

#[derive(Debug)]
struct UidInfo {
    first_seen_ts: i64,
    last_seen_ts: i64,
    seen_count: usize,
    equipment_tag: Option<String>,
    fields: HashMap<(String, String), FieldInfo>,
}

impl UidInfo {
    fn is_inactive(&self, inactivity_added_seconds: u64) -> bool {
        self.fields
            .iter()
            .filter(|f| !f.1.is_inactive(inactivity_added_seconds))
            .count()
            == 0
    }
}

#[derive(Debug)]
struct FieldInfo {
    measurement: String,
    value: InfluxValue,
    first_seen_ts: i64,
    last_seen_ts: i64,
    last_changed_ts: i64,
    last_5_ts: Last5Timestamps,
    seen_count: usize,
}

#[derive(Debug, Clone)]
struct Last5Timestamps {
    timestamps: VecDeque<i64>,
}

#[allow(dead_code)]
impl Last5Timestamps {
    fn new() -> Self {
        Self {
            timestamps: VecDeque::with_capacity(5),
        }
    }
    fn new_with_val(timestamp: i64) -> Self {
        let mut ret = Self {
            timestamps: VecDeque::with_capacity(5),
        };
        ret.timestamps.push_back(timestamp);
        ret
    }
    fn push(&mut self, val: i64) {
        let mut cur_count = self.timestamps.len();
        while cur_count >= 5 {
            let _ = self.timestamps.pop_front();
            cur_count -= 1;
        }
        self.timestamps.push_back(val);
    }
    fn pop(&mut self) -> Option<i64> {
        self.timestamps.pop_front()
    }
    fn drop_front(&mut self) {
        if self.timestamps.is_empty() {
            return;
        }
        let _ = self.timestamps.pop_front();
    }
    fn len(&self) -> usize {
        self.timestamps.len()
    }
    fn differences(&self) -> Vec<i64> {
        if self.timestamps.len() < 2 {
            return vec![];
        }
        let mut diffs = Vec::with_capacity(self.timestamps.len() - 1);
        for i in 0..self.timestamps.len() - 1 {
            diffs.push(self.timestamps[i + 1] - self.timestamps[i]);
        }
        diffs
    }
    #[allow(clippy::cast_possible_wrap)]
    fn differences_mean(&self) -> i64 {
        let timestamp_count = self.timestamps.len();
        if timestamp_count < 2 {
            return 0;
        }
        let mut idx = 0;
        let mut diffsum = 0;
        while idx < (timestamp_count - 1) {
            diffsum += self.timestamps[idx + 1] - self.timestamps[idx];
            idx += 1;
        }
        diffsum / (timestamp_count - 1) as i64
    }
    #[allow(clippy::cast_precision_loss)]
    fn differences_stddev(&self) -> f64 {
        let mean = self.differences_mean();
        let mut sum_deviations = 0;
        let differences = self.differences();
        if differences.len() < 2 {
            return -1.0;
        }
        for val in differences {
            let deviation = (val - mean).pow(2);
            sum_deviations += deviation;
        }
        let variance = sum_deviations as f64 / (self.timestamps.len() - 2) as f64;
        variance.sqrt()
    }
}

#[derive(Debug, Clone, PartialEq)]
enum InfluxValue {
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Boolean(bool),
}
impl FieldInfo {
    #[allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
    fn is_inactive(&self, inactivity_added_seconds: u64) -> bool {
        let curtime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let estimated_frequency = if self.seen_count > 1 {
            (self.last_seen_ts - self.first_seen_ts) / (self.seen_count as i64 - 1)
        } else {
            -1
        };

        let threshold = curtime - estimated_frequency as u64 - inactivity_added_seconds;

        self.seen_count > 1 && self.last_seen_ts < threshold as i64
    }
}
impl std::fmt::Display for InfluxValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InfluxValue::I64(val) => write!(f, "{val}"),
            InfluxValue::U64(val) => write!(f, "{val}"),
            InfluxValue::F64(val) => write!(f, "{val}"),
            InfluxValue::String(val) => write!(f, "{val}"),
            InfluxValue::Boolean(val) => write!(f, "{val}"),
        }
    }
}

impl<'a> From<FieldValue<'a>> for InfluxValue {
    fn from(value: FieldValue<'a>) -> Self {
        match value {
            FieldValue::I64(val) => Self::I64(val),
            FieldValue::U64(val) => Self::U64(val),
            FieldValue::F64(val) => Self::F64(val),
            FieldValue::String(escaped_str) => Self::String(escaped_str.to_string()),
            FieldValue::Boolean(val) => Self::Boolean(val),
        }
    }
}

impl ValueCache {
    fn new(output_measurement: String, inactivity_threshold: u64, precision: Precision) -> Self {
        Self {
            output_measurement,
            created_timestamp: curtime(),
            precision,
            minimum_inactivity_seconds: inactivity_threshold,
            uids: HashMap::new(),
            // field_names: HashSet::new(),
            fields_seen_count: 0,
            fields_changed_count: 0,
            first_value_ts: i64::MAX,
            last_value_ts: i64::MIN,
            kafka_message_count: 0,
            ilp_line_count: 0,
        }
    }

    fn add_or_update_fields(
        &mut self,
        uid: &str,
        equipment_tag: Option<String>,
        field_set: &SmallVec<[(EscapedStr<'_>, FieldValue<'_>); 4]>,
        timestamp: i64,
        measurement: &EscapedStr<'_>,
    ) {
        self.fields_seen_count += field_set.len();
        self.first_value_ts = self.first_value_ts.min(timestamp);
        self.last_value_ts = self.last_value_ts.max(timestamp);
        self.uids
            .entry(uid.to_string())
            .and_modify(|e| {
                e.last_seen_ts = e.last_seen_ts.max(timestamp);
                e.seen_count += 1;
                for (field_name, field_value) in field_set {
                    e.fields
                        .entry((field_name.to_string(), measurement.to_string()))
                        .and_modify(|f| {
                            let new_val: InfluxValue = field_value.to_owned().into();
                            if f.value != new_val {
                                self.fields_changed_count += 1;
                                f.last_changed_ts = timestamp;
                            }
                            f.value = new_val;
                            f.last_seen_ts = f.last_seen_ts.max(timestamp);
                            f.seen_count += 1;
                            f.last_5_ts.push(timestamp);
                        })
                        .or_insert({
                            //self.field_names.insert(field_name.to_string());
                            FieldInfo {
                                value: field_value.to_owned().into(),
                                first_seen_ts: timestamp,
                                last_seen_ts: timestamp,
                                last_changed_ts: 0,
                                seen_count: 1,
                                measurement: measurement.to_string(),
                                last_5_ts: Last5Timestamps::new_with_val(timestamp),
                            }
                        });
                }
            })
            .or_insert(UidInfo {
                first_seen_ts: timestamp,
                last_seen_ts: timestamp,
                seen_count: 1,
                equipment_tag,
                fields: {
                    let mut hm = HashMap::new();
                    for (field_name, field_value) in field_set {
                        //self.field_names.insert(field_name.to_string());
                        hm.insert(
                            (field_name.to_string(), measurement.to_string()),
                            FieldInfo {
                                value: field_value.to_owned().into(),
                                first_seen_ts: timestamp,
                                last_seen_ts: timestamp,
                                last_changed_ts: 0,
                                seen_count: 1,
                                measurement: measurement.to_string(),
                                last_5_ts: Last5Timestamps::new_with_val(timestamp),
                            },
                        );
                    }
                    hm
                },
            });
    }

    fn uid_count_last_seconds(&self, seconds: i64) -> usize {
        let oldest_allowed_timestamp = curtime() - seconds;
        self.uids
            .iter()
            .filter(|u| u.1.last_seen_ts >= oldest_allowed_timestamp)
            .count()
    }

    fn total_uid_count(&self) -> usize {
        self.uids.len()
    }

    /// This does not take expected frequency in consideration, only seconds since last value.
    fn inactive_uid_count(&self, added_seconds: Option<u64>) -> usize {
        let inactivity_added_seconds = added_seconds.unwrap_or(self.minimum_inactivity_seconds);
        self.uids
            .iter()
            .filter(|u| u.1.is_inactive(inactivity_added_seconds))
            .count()
    }

    fn field_count_last_seconds(&self, seconds: i64) -> usize {
        let oldest_allowed_timestamp = curtime() - seconds;
        let mut field_count = 0;
        for uid_cache in self.uids.values() {
            if uid_cache.last_seen_ts < oldest_allowed_timestamp {
                continue;
            }
            for fieldinfo in uid_cache.fields.values() {
                if fieldinfo.last_seen_ts < oldest_allowed_timestamp {
                    continue;
                }
                field_count += 1;
            }
        }
        field_count
    }

    fn total_field_count(&self) -> usize {
        self.uids.values().map(|u| u.fields.len()).sum()
    }

    // fn unique_field_names(&self) -> HashSet<String> {
    //     let mut field_names = HashSet::new();
    //     for uid in self.uids.values() {
    //         for fields in uid.fields.keys() {
    //             field_names.insert(fields.1.to_owned());
    //         }
    //     }
    //     field_names
    // }

    fn inactive_field_count(&self, added_seconds: Option<u64>) -> usize {
        let inactivity_added_seconds = added_seconds.unwrap_or(self.minimum_inactivity_seconds);
        self.uids
            .iter()
            .map(|u| {
                if u.1.seen_count <= 1 {
                    0
                } else {
                    u.1.fields
                        .iter()
                        .filter(|f| f.1.is_inactive(inactivity_added_seconds))
                        .count()
                }
            })
            .sum()
    }

    fn uids_and_fields_to_string(&self) -> String {
        let mut result = String::new();
        for (uid_name, uid_cache) in &self.uids {
            let fields = uid_cache
                .fields
                .keys()
                .map(|f| f.0.clone())
                .collect::<Vec<_>>();
            let fields = fields.join(", ");
            let _ = write!(
                result,
                "Uid: \"{}\", equipment-tag: \"{}\"\nFields: {}\n\n",
                uid_name,
                uid_cache
                    .equipment_tag
                    .clone()
                    .unwrap_or("unknown".to_string()),
                fields
            );
        }
        result
    }

    #[allow(clippy::cast_possible_wrap)]
    fn everything_to_string(&self) -> String {
        let mut result = String::new();
        for (uid_name, uid_cache) in &self.uids {
            let _ = writeln!(
                result,
                "Uid: \"{}\", equipment-tag \"{}\", first seen: \"{}\", last seen: \"{}\"",
                uid_name,
                uid_cache
                    .equipment_tag
                    .clone()
                    .unwrap_or("unknown".to_string()),
                uid_cache.first_seen_ts,
                uid_cache.last_seen_ts
            );
            for field in &uid_cache.fields {
                let estimated_frequency = if field.1.seen_count < 2 {
                    -1
                } else {
                    (field.1.last_seen_ts - field.1.first_seen_ts) / (field.1.seen_count as i64 - 1)
                };
                let _ = writeln!(
                    result,
                    "Field: \"{}\", measurement: \"{}\", first seen: {}, last seen: {}, seen count: {}, estimated frequency: {}, ts_differences_mean: {}, differences_stddev: {:.2}, value: \"{}\"\n",
                    field.0.0,
                    field.1.measurement,
                    field.1.first_seen_ts,
                    field.1.last_seen_ts,
                    field.1.seen_count,
                    estimated_frequency,
                    field.1.last_5_ts.differences_mean(),
                    field.1.last_5_ts.differences_stddev(),
                    field.1.value
                );
            }
            result.push('\n');
        }
        result
    }

    // Check if all fields have the same last_seen timestamp as the uid
    #[allow(dead_code)]
    fn get_field_and_uid_ts_mismatches(&self) {
        for (uid_name, uid_cache) in &self.uids {
            let ts = uid_cache.last_seen_ts;
            for field in &uid_cache.fields {
                if field.1.last_seen_ts != ts {
                    println!(
                        "Uid \"{}\" (equipment-tag {}) last seen {}, but field \"{}\" last seen {}",
                        uid_name,
                        uid_cache
                            .equipment_tag
                            .clone()
                            .unwrap_or("unknown".to_string()),
                        uid_cache.last_seen_ts,
                        field.0.0,
                        field.1.last_seen_ts
                    );
                }
            }
        }
    }

    fn list_inactive(&self, inactivity_added_seconds: Option<u64>) -> String {
        let inactivity_added_seconds =
            inactivity_added_seconds.unwrap_or(self.minimum_inactivity_seconds);
        let current_timestamp = curtime();
        let mut result = String::new();
        for (uid_name, uid_cache) in &self.uids {
            let mut uid_inactive = false;
            if uid_cache.is_inactive(inactivity_added_seconds) {
                let _ = writeln!(
                    result,
                    "Inactive Uid (no data at all for more than {} seconds, current age is {} seconds): uid {} equipment-tag {}",
                    inactivity_added_seconds,
                    current_timestamp - uid_cache.last_seen_ts,
                    &uid_name,
                    uid_cache
                        .equipment_tag
                        .clone()
                        .unwrap_or("unknown".to_string())
                );
                uid_inactive = true;
            }
            let mut inactive_fields = Vec::new();
            for field in &uid_cache.fields {
                if field.1.is_inactive(inactivity_added_seconds) {
                    // if field.1.last_seen < limit {
                    inactive_fields.push((
                        field.0,
                        field.1.measurement.clone(),
                        field.1.last_seen_ts,
                    ));
                }
            }
            if !inactive_fields.is_empty() {
                inactive_fields.sort_unstable_by(|a, b| a.0.cmp(b.0));

                if uid_inactive {
                    let _ = writeln!(result, "Fields expected for Uid \"{uid_name}\":");
                } else {
                    let _ = writeln!(
                        result,
                        "Inactive fields (no field data for more than {} seconds) for Uid \"{}\" equipment-tag \"{}\":",
                        inactivity_added_seconds,
                        uid_name,
                        uid_cache
                            .equipment_tag
                            .clone()
                            .unwrap_or("unknown".to_string())
                    );
                }
                for field in &inactive_fields {
                    let _ = writeln!(
                        result,
                        "\"{}\" (measurement: \"{}\", age: {})",
                        field.0.0,
                        field.1,
                        current_timestamp - field.2
                    );
                }
                result.push('\n');
            }
        }
        result
    }

    fn unique_field_name_count(&self) -> (usize, usize) {
        let mut total_count = 0;
        let mut fieldset = HashSet::new();
        for info in self.uids.values() {
            for (field, _) in info.fields.keys() {
                fieldset.insert(field.to_owned());
                total_count += 1;
            }
        }
        (fieldset.len(), total_count)
    }

    fn remove_inactive_uids_and_fields(&mut self, inactivity_added_seconds: Option<u64>) -> String {
        let inactivity_added_seconds =
            inactivity_added_seconds.unwrap_or(self.minimum_inactivity_seconds);
        let mut count_whole_uids = 0;
        let mut count_whole_uids_fields = 0;
        let mut count_single_fields = 0;

        let uids = self.uids.iter().map(|u| u.0.clone()).collect::<Vec<_>>();
        for uid_name in &uids {
            let uid_is_inactive = {
                self.uids
                    .get(uid_name)
                    .unwrap()
                    .is_inactive(inactivity_added_seconds)
            };
            if uid_is_inactive {
                count_whole_uids_fields += self.uids.get(uid_name).unwrap().fields.keys().len();
                count_whole_uids += 1;
                let _ = self.uids.remove(uid_name);
                continue;
            }

            let mut inactive_fields = Vec::new();
            let uid_cache = self.uids.get(uid_name).unwrap();
            for field in uid_cache.fields.keys() {
                let field_is_inactive = {
                    uid_cache
                        .fields
                        .get(field)
                        .unwrap()
                        .is_inactive(inactivity_added_seconds)
                };
                if field_is_inactive {
                    inactive_fields.push(field.to_owned());
                }
            }
            for field in &inactive_fields {
                self.uids.entry(uid_name.clone()).and_modify(|u| {
                    let _ = u.fields.remove(field);
                });
                count_single_fields += 1;
            }
        }
        let result = format!(
            "Inactive uid removed: {count_whole_uids}, fields count for these uids: {count_whole_uids_fields}\nInactive fields in active uids removed: {count_single_fields}"
        );
        result
    }
    fn remove_uid_if_inactive(
        &mut self,
        uid: &str,
        inactivity_added_seconds: Option<u64>,
    ) -> Result<(), String> {
        let inactivity_added_seconds =
            inactivity_added_seconds.unwrap_or(self.minimum_inactivity_seconds);

        if !self.uids.contains_key(uid) {
            return Err("UID not found".to_string());
        }

        let uid_is_inactive = {
            self.uids
                .get(uid)
                .unwrap()
                .is_inactive(inactivity_added_seconds)
        };
        if uid_is_inactive {
            let _ = self.uids.remove(uid);
            return Ok(());
        }
        Err(format!("Uid {uid} was not inactive"))
    }

    fn get_uid_age_stats(&self) -> Ages {
        let mut uid_count = 0;
        let mut sum = 0;
        let mut maxage = 0;
        let mut minage = i64::MAX;

        for uid_cache in self.uids.values() {
            let uid_age = curtime() - uid_cache.last_seen_ts;
            uid_count += 1;
            sum += uid_age;
            maxage = maxage.max(uid_age);
            minage = minage.min(uid_age);
        }
        if uid_count == 0 {
            return Ages {
                min: 0,
                max: 0,
                average: 0,
            };
        }
        Ages {
            min: minage,
            max: maxage,
            average: sum / uid_count,
        }
    }

    #[allow(clippy::cast_possible_wrap)]
    fn get_uid_info(&self, uid: &str) -> String {
        let mut result = String::new();
        if let Some(uid_cache) = self.uids.get(uid) {
            for ((fieldname, measurement), fieldinfo) in &uid_cache.fields {
                let estimated_frequency = if fieldinfo.seen_count < 2 {
                    -1
                } else {
                    (fieldinfo.last_seen_ts - fieldinfo.first_seen_ts)
                        / (fieldinfo.seen_count as i64 - 1)
                };
                let _ = writeln!(
                    result,
                    "Field: \"{}\", measurement: \"{}\", first seen: {}, last seen: {}, seen count: {}, estimated frequency: {}, ts_differences_mean: {}, ts_differences_stddev: {:.2}, value: \"{}\"",
                    fieldname,
                    measurement,
                    fieldinfo.first_seen_ts,
                    fieldinfo.last_seen_ts,
                    fieldinfo.seen_count,
                    estimated_frequency,
                    fieldinfo.last_5_ts.differences_mean(),
                    fieldinfo.last_5_ts.differences_stddev(),
                    fieldinfo.value
                );
            }
            result.push('\n');
        } else {
            result = "Uid not found".to_string();
        }
        result
    }

    fn get_uid_field_info(&self, uid: &str, measurement: &str, fieldname: &str) -> String {
        let mut result = String::new();
        if let Some(uid_cache) = self.uids.get(uid) {
            if let Some(field) = uid_cache
                .fields
                .get(&(fieldname.to_string(), measurement.to_string()))
            {
                let values: String = field
                    .last_5_ts
                    .timestamps
                    .iter()
                    .map(|&v| format!("{v}"))
                    .collect::<Vec<String>>()
                    .join(", ");
                let _ = write!(
                    result,
                    "Uid: \"{uid}\", measurement: \"{measurement}\", last seen: {}, seen_count: {}, ts_differences_mean: {}, ts_differences_stddev: {:.2}, last_timestamps: {}",
                    field.last_seen_ts,
                    field.seen_count,
                    field.last_5_ts.differences_mean(),
                    field.last_5_ts.differences_stddev(),
                    values
                );
            } else {
                result = format!("No fields found for uid {uid}").to_string();
            }
        } else {
            result = "Uid not found".to_string();
        }
        result
    }
    fn changed_fields_last_seconds(&self, seconds: i64) -> usize {
        let mut count = 0;
        for uid in &self.uids {
            for info in uid.1.fields.values() {
                if info.last_changed_ts >= curtime() - seconds {
                    count += 1;
                }
            }
        }
        count
    }
}

#[allow(clippy::cast_possible_wrap)]
fn curtime() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[derive(Debug)]
struct Ages {
    min: i64,
    max: i64,
    average: i64,
}
#[allow(clippy::too_many_lines)]
async fn consume_and_process(
    brokers: String,
    group_id: String,
    topics: Vec<String>,
    stats: Arc<Mutex<ValueCache>>,
) {
    let mut offsets_have_been_reset = false;

    let context = CustomContext;

    let mut config = ClientConfig::new();

    config
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "30000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "largest")
        .set("client.id", "kafka-stats")
        //.set("statistics.interval.ms", "30000")
        .set_log_level(RDKafkaLogLevel::Debug);

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
                if !offsets_have_been_reset {
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
                    offsets_have_been_reset = true;
                    continue;
                }
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

#[derive(Parser)]
#[command(version)]
struct CliArgs {
    #[arg(short, long, env = "GAPIT_KAFKASTATS_PORT", default_value_t = 3005)]
    port: u16,
    #[arg(
        short,
        long,
        env = "GAPIT_KAFKASTATS_BIND_IP",
        default_value = "0.0.0.0"
    )]
    bind_ip: IpAddr,
    #[arg(
        long,
        env = "GAPIT_KAFKASTATS_OUTPUT_MEASUREMENT",
        default_value = "kafka_influx_statistics"
    )]
    output_reported_measurement: String,
    #[arg(long, env = "GAPIT_KAFKASTATS_OUTPUT_REPORTED_HOST")]
    output_reported_host: Option<String>,
    #[arg(
        long,
        env = "GAPIT_KAFKASTATS_BROKERS",
        default_value = "localhost:9092"
    )]
    brokers: String,
    #[arg(
        long,
        short,
        env = "GAPIT_KAFKASTATS_INACTIVITY_THRESHOLD",
        default_value_t = 60
    )]
    inactivity_threshold: u64,
    #[arg(
        short,
        long,
        env = "GAPIT_KAFKASTATS_GROUP_ID",
        default_value = "kafkastats_1"
    )]
    group_id: String,
    #[arg(
        short,
        long,
        env = "GAPIT_KAFKASTATS_TOPICS",
        default_value = "instruments"
    )]
    topics: String,
    #[arg(long, env = "GAPIT_KAFKASTATS_PRECISION", default_value_t = Precision::Second)]
    precision: Precision,
    #[arg(long, env = "GAPIT_KAFKASTATS_LOG_CONF")]
    log_conf: Option<String>,
    #[arg(long, env = "GAPIT_KAFKASTATS_TLS_ENABLE", default_value_t = false)]
    tls_enabled: bool,
    #[arg(long, env = "GAPIT_KAFKASTATS_TLS_KEYFILE")]
    tls_key_file: Option<String>,
    #[arg(long, env = "GAPIT_KAFKASTATS_TLS_CERTFILE")]
    tls_cert_file: Option<String>,
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum Precision {
    Second,
    Millisecond,
    Nanosecond,
}

impl From<String> for Precision {
    fn from(value: String) -> Self {
        match value.as_str() {
            "s" => Self::Second,
            "ms" => Self::Millisecond,
            "ns" => Self::Nanosecond,
            _ => panic!(),
        }
    }
}

impl Display for Precision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Precision::Second => write!(f, "s"),
            Precision::Millisecond => write!(f, "ms"),
            Precision::Nanosecond => write!(f, "ns"),
        }
    }
}

#[tokio::main]
async fn main() {
    println!(
        "{} version {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );
    let args = CliArgs::parse();

    assert!(
        args.tls_enabled && (args.tls_key_file.is_none() || args.tls_cert_file.is_none()),
        "TLS enabled but certificate file or certificate key file not specified."
    );
    setup_logger(true, args.log_conf.as_ref());
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{version_n:08x}, {version_s}");

    let topics = args
        .topics
        .split(',')
        .map(std::borrow::ToOwned::to_owned)
        .collect();
    let brokers = args.brokers;
    let group_id = args.group_id;

    let kafkastats = Arc::new(Mutex::new(ValueCache::new(
        args.output_reported_measurement,
        args.inactivity_threshold,
        args.precision,
    )));
    let state = AppState {
        // data_cache: Arc::clone(stats),
        data_cache: Arc::clone(&kafkastats),
        config_output_reported_host: args.output_reported_host,
    };

    // Start Kafka reader
    let statsclone = Arc::clone(&kafkastats);
    tokio::spawn(async {
        consume_and_process(brokers, group_id, topics, statsclone).await;
    });

    // Set up web service
    let app = Router::new()
        .route("/", get(root))
        .route("/uidcount", get(uid_count_last_minute))
        .route("/fieldcount", get(field_count_last_minute))
        .route("/uniquefieldcount", get(unique_field_names))
        .route("/everything", get(everything))
        .route("/uidinfo/{uid}", get(uidinfo))
        .route(
            "/uidinfo/{uid}/{measurement}/{field_name}",
            get(uidfieldinfo),
        )
        .route("/stats", get(stats))
        .route("/inactive", get(list_inactive))
        .route("/inactive/remove", get(remove_inactive))
        .route("/inactive/remove/uid/{uid}", get(remove_inactive_uid))
        .route(
            "/changed_fields_last_seconds/{seconds}",
            get(count_changed_fields_last_seconds),
        )
        .with_state(state);

    if args.tls_enabled && args.tls_cert_file.is_some() && args.tls_key_file.is_some() {
        println!(
            "Starting HTTPS server listening on {}:{}",
            args.bind_ip, args.port
        );
        tokio::spawn(async move {
            let port = args.port;
            https_server(
                app,
                &args.bind_ip.to_string(),
                port,
                args.tls_cert_file.unwrap().into(),
                args.tls_key_file.unwrap().into(),
            )
            .await;
        });
    } else {
        println!(
            "Starting HTTP server listening on {}:{}",
            args.bind_ip, args.port
        );
        let listener = tokio::net::TcpListener::bind(format!("{}:{}", args.bind_ip, args.port))
            .await
            .unwrap();
        tokio::spawn(async {
            axum::serve(listener, app).await.unwrap();
        });
    }

    let _ = signal::ctrl_c().await;
}

async fn https_server(
    app: Router,
    address: &str,
    port: u16,
    tls_cert_file: PathBuf,
    tls_key_file: PathBuf,
) {
    let config = match RustlsConfig::from_pem_file(tls_cert_file, tls_key_file).await {
        Ok(config) => config,
        Err(e) => panic!("Could not configure TLS: {e}"),
    };
    let address = match Ipv4Addr::from_str(address) {
        Ok(address) => IpAddr::V4(address),
        Err(e) => panic!("Couldn't parse IP address from {address}: {e}"),
    };
    let socketaddr = SocketAddr::new(address, port);

    axum_server::bind_rustls(socketaddr, config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn root(State(state): State<AppState>) -> (StatusCode, String) {
    match state.data_cache.lock() {
        Ok(valuecache) => (StatusCode::OK, valuecache.uids_and_fields_to_string()),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Couldn't lock mutex".to_string(),
        ),
    }
}
async fn uid_count_last_minute(State(state): State<AppState>) -> (StatusCode, String) {
    match state.data_cache.lock() {
        Ok(valuecache) => (
            StatusCode::OK,
            format!("{}", valuecache.uid_count_last_seconds(60)),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Couldn't lock mutex".to_string(),
        ),
    }
}
async fn field_count_last_minute(State(state): State<AppState>) -> (StatusCode, String) {
    match state.data_cache.lock() {
        Ok(valuecache) => (
            StatusCode::OK,
            format!("{}", valuecache.field_count_last_seconds(60)),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Couldn't lock mutex".to_string(),
        ),
    }
}
async fn everything(State(state): State<AppState>) -> (StatusCode, String) {
    match state.data_cache.lock() {
        Ok(valuecache) => (StatusCode::OK, valuecache.everything_to_string()),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Couldn't lock mutex".to_string(),
        ),
    }
}
async fn uidinfo(Path(uid): Path<String>, State(state): State<AppState>) -> (StatusCode, String) {
    match state.data_cache.lock() {
        Ok(valuecache) => (StatusCode::OK, valuecache.get_uid_info(&uid)),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Couldn't lock mutex".to_string(),
        ),
    }
}

async fn uidfieldinfo(
    Path(FieldInfoParams {
        uid,
        measurement,
        field_name,
    }): Path<FieldInfoParams>,
    State(state): State<AppState>,
) -> (StatusCode, String) {
    match state.data_cache.lock() {
        Ok(valuecache) => (
            StatusCode::OK,
            valuecache.get_uid_field_info(&uid, &measurement, &field_name),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Couldn't lock mutex".to_string(),
        ),
    }
}

async fn stats(State(state): State<AppState>) -> (StatusCode, String) {
    match state.data_cache.lock() {
        Ok(valuecache) => {
            if valuecache.last_value_ts < valuecache.first_value_ts {
                return (StatusCode::OK, "Not enough data collected".to_string());
            }
            let ages = valuecache.get_uid_age_stats();
            let host_tag = if let Some(host) = state.config_output_reported_host {
                format!(",host={host}")
            } else {
                String::new()
            };

            let ret = format!(
                "{}{} timeperiod_length_seconds={},uid_all_count={},uid_inactive_count={},uid_plus_field_combination_count={},uid_field_inactive_count={},uid_age_min={},uid_age_max={},uid_age_mean={},kafka_message_count={},ilp_line_count={},field_count={},changed_fields_count={},unique_field_name_count={}\n",
                valuecache.output_measurement,
                host_tag,
                valuecache.last_value_ts - valuecache.first_value_ts,
                valuecache.total_uid_count(),
                valuecache.inactive_uid_count(None),
                valuecache.total_field_count(),
                valuecache.inactive_field_count(None),
                ages.min,
                ages.max,
                ages.average,
                valuecache.kafka_message_count,
                valuecache.ilp_line_count,
                valuecache.fields_seen_count,
                valuecache.fields_changed_count,
                valuecache.unique_field_name_count().0
            );
            (StatusCode::OK, ret)
            //
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Couldn't lock mutex".to_string(),
        ),
    }
}
async fn list_inactive(State(state): State<AppState>) -> (StatusCode, String) {
    match state.data_cache.lock() {
        Ok(valuecache) => {
            let ret = valuecache.list_inactive(None);
            (StatusCode::OK, ret)
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Couldn't lock mutex".to_string(),
        ),
    }
}
async fn remove_inactive(State(state): State<AppState>) -> (StatusCode, String) {
    match state.data_cache.lock() {
        Ok(mut valuecache) => {
            let ret = valuecache.remove_inactive_uids_and_fields(None);
            (StatusCode::OK, ret)
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Couldn't lock mutex".to_string(),
        ),
    }
}
async fn remove_inactive_uid(
    Path(uid): Path<String>,
    State(state): State<AppState>,
) -> (StatusCode, String) {
    match state.data_cache.lock() {
        Ok(mut valuecache) => {
            let ret = match valuecache.remove_uid_if_inactive(&uid, None) {
                Ok(()) => format!("Removed inactive UID: {}", &uid),
                Err(err) => format!("UID was not inactive and was not removed: {}", &err),
            };
            (StatusCode::OK, ret)
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Couldn't lock mutex".to_string(),
        ),
    }
}
async fn unique_field_names(State(state): State<AppState>) -> (StatusCode, String) {
    match state.data_cache.lock() {
        Ok(valuecache) => {
            let counts = valuecache.unique_field_name_count();
            let ret = format!("total_count={},unique_count={}", counts.1, counts.0);
            (StatusCode::OK, ret)
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Couldn't lock mutex".to_string(),
        ),
    }
}
async fn count_changed_fields_last_seconds(
    Path(seconds): Path<String>,
    State(state): State<AppState>,
) -> (StatusCode, String) {
    match seconds.parse::<i64>() {
        Ok(secs) => match state.data_cache.lock() {
            Ok(valuecache) => {
                let count = valuecache.changed_fields_last_seconds(secs);
                let ret = format!("changed_fields_last_{secs}_seconds_count={count}");
                (StatusCode::OK, ret)
            }
            Err(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Couldn't lock mutex".to_string(),
            ),
        },
        Err(_) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("Couldn't parse {seconds} into i64"),
        ),
    }
}

#[derive(Clone)]
struct AppState {
    data_cache: Arc<Mutex<ValueCache>>,
    config_output_reported_host: Option<String>,
}

impl Debug for AppState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        <Arc<Mutex<ValueCache>> as Debug>::fmt(&self.data_cache, f)
    }
}

#[derive(Deserialize)]
struct FieldInfoParams {
    uid: String,
    measurement: String,
    field_name: String,
}

#[cfg(test)]
mod test {
    use super::Last5Timestamps;

    #[test]
    fn test_last_5_timestamps_differences_mean() {
        let mut last = Last5Timestamps::new();
        last.push(-1000); // Should be removed when 6th value is pushed
        last.push(-500); // Should be removed by the drop_front
        last.push(3);
        last.push(5);
        last.push(7);
        last.push(9);
        last.push(11);
        last.drop_front();
        assert_eq!(2, last.differences_mean());
    }
    #[test]
    fn test_last_5_timestamps_differences_mean2() {
        let mut last = Last5Timestamps::new();
        last.push(2);
        last.push(2);
        last.push(2);
        last.push(2);
        last.push(2);
        assert_eq!(0, last.differences_mean());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_last_5_timestamps_stddev() {
        let mut last = Last5Timestamps::new();
        last.push(10);
        last.push(20);
        last.push(40);
        assert_eq!(7.071_067_811_865_475_5, last.differences_stddev());
    }
}
