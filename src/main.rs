use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, command};
use log::{info, warn};
use rdkafka::{Offset, TopicPartitionList};
use smallvec::SmallVec;
use tokio::signal;
use tokio::sync::RwLock;

use axum::{Router, extract::Path, extract::State, http::StatusCode, routing::get};
use axum_server::tls_rustls::RustlsConfig;

use influxdb_line_protocol::{EscapedStr, FieldValue, ParsedLine};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::message::{Headers, Message};
use rdkafka::util::get_rdkafka_version;

use crate::lvc_utils::setup_logger;
mod lvc_utils;

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("ConsumerContext Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("ConsumerContext Post rebalance {:?}", rebalance);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

#[derive(Debug)]
#[allow(dead_code)]
struct ValueCache {
    output_measurement: String,
    created_timestamp: u64,
    first_value_ts: i64,
    last_value_ts: i64,
    minimum_inactivity_seconds: u64,
    ilp_line_count: usize,
    fields_seen_count: usize,
    uids: HashMap<String, UidInfo>,
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
    last_5_ts: Last5Timestamps,
    seen_count: usize,
}

#[derive(Debug, Clone)]
struct Last5Timestamps {
    values: VecDeque<i64>,
}

#[allow(dead_code)]
impl Last5Timestamps {
    fn new() -> Self {
        Self {
            values: VecDeque::with_capacity(5),
        }
    }
    fn new_with_val(timestamp: i64) -> Self {
        let mut ret = Self {
            values: VecDeque::with_capacity(5),
        };
        ret.values.push_back(timestamp);
        ret
    }
    fn push(&mut self, val: i64) {
        let mut cur_count = self.values.len();
        while cur_count >= 5 {
            let _ = self.values.pop_front();
            cur_count -= 1;
        }
        self.values.push_back(val);
    }
    fn pop(&mut self) -> Option<i64> {
        self.values.pop_front()
    }
    fn drop_front(&mut self) {
        if self.values.is_empty() {
            return;
        }
        let _ = self.values.pop_front();
    }
    fn len(&self) -> usize {
        self.values.len()
    }
    fn differences(&self) -> Vec<i64> {
        if self.values.len() < 2 {
            return vec![];
        }
        let mut diffs = Vec::with_capacity(self.values.len() - 1);
        for i in 0..self.values.len() - 1 {
            diffs.push(self.values[i + 1] - self.values[i]);
        }
        diffs
    }
    fn differences_mean(&self) -> i64 {
        let ts_count = self.values.len();
        if ts_count < 2 {
            return 0;
        }
        let mut idx = 0;
        let mut diffsum = 0;
        while idx < (ts_count - 1) {
            diffsum += self.values[idx + 1] - self.values[idx];
            idx += 1;
        }
        diffsum / (ts_count - 1) as i64
    }
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
        let variance = sum_deviations as f64 / (self.values.len() - 2) as f64;
        variance.sqrt()
    }
}

#[derive(Debug, Clone)]
enum InfluxValue {
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Boolean(bool),
}
impl FieldInfo {
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
            InfluxValue::I64(val) => write!(f, "{}", val),
            InfluxValue::U64(val) => write!(f, "{}", val),
            InfluxValue::F64(val) => write!(f, "{}", val),
            InfluxValue::String(val) => write!(f, "{}", val),
            InfluxValue::Boolean(val) => write!(f, "{}", val),
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
    fn new(output_measurement: String, inactivity_threshold: u64) -> Self {
        let curtime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Self {
            output_measurement,
            created_timestamp: curtime,
            minimum_inactivity_seconds: inactivity_threshold,
            uids: HashMap::new(),
            fields_seen_count: 0,
            first_value_ts: i64::MAX,
            last_value_ts: i64::MIN,
            ilp_line_count: 0,
        }
    }

    fn update_fields(
        &mut self,
        uid: &str,
        equipment_tag: Option<String>,
        field_set: &SmallVec<[(EscapedStr<'_>, FieldValue<'_>); 4]>,
        timestamp: i64,
        measurement: EscapedStr<'_>,
    ) -> Result<(), String> {
        self.fields_seen_count += field_set.len();
        self.first_value_ts = self.first_value_ts.min(timestamp);
        self.last_value_ts = self.last_value_ts.max(timestamp);
        self.uids
            .entry(uid.to_string())
            .and_modify(|e| {
                e.last_seen_ts = e.last_seen_ts.max(timestamp);
                e.seen_count += 1;
                for field in field_set {
                    e.fields
                        .entry((field.0.to_string(), measurement.to_string()))
                        .and_modify(|f| {
                            f.value = (field.1).to_owned().into();
                            f.last_seen_ts = f.last_seen_ts.max(timestamp);
                            f.seen_count += 1;
                            f.last_5_ts.push(timestamp);
                        })
                        .or_insert({
                            FieldInfo {
                                value: (field.1).to_owned().into(),
                                first_seen_ts: timestamp,
                                last_seen_ts: timestamp,
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
                        hm.insert(
                            (field_name.to_string(), measurement.to_string()),
                            FieldInfo {
                                value: field_value.to_owned().into(),
                                first_seen_ts: timestamp,
                                last_seen_ts: timestamp,
                                seen_count: 1,
                                measurement: measurement.to_string(),
                                last_5_ts: Last5Timestamps::new_with_val(timestamp),
                            },
                        );
                    }
                    hm
                },
            });
        Ok(())
    }

    fn uid_count_last_seconds(&self, seconds: u64) -> usize {
        let oldest_allowed_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - seconds;
        self.uids
            .iter()
            .filter(|u| u.1.last_seen_ts >= oldest_allowed_timestamp as i64)
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

    fn field_count_last_seconds(&self, seconds: u64) -> usize {
        let oldest_allowed_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - seconds;
        let mut field_count = 0;
        for uid_cache in self.uids.values() {
            if uid_cache.last_seen_ts < oldest_allowed_timestamp as i64 {
                continue;
            }
            for fieldinfo in uid_cache.fields.values() {
                if fieldinfo.last_seen_ts < oldest_allowed_timestamp as i64 {
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

    fn all_uids_and_fields(&self) -> String {
        let mut result = "".to_string();
        for (uid_name, uid_cache) in &self.uids {
            let fields = uid_cache
                .fields
                .keys()
                .map(|f| f.0.to_string())
                .collect::<Vec<_>>();
            let fields = fields.join(", ");
            result.push_str(&format!(
                "Uid: \"{}\", equipment-tag: \"{}\"\nFields: {}\n\n",
                uid_name,
                uid_cache
                    .equipment_tag
                    .clone()
                    .unwrap_or("unknown".to_string()),
                fields
            ));
        }
        result
    }

    fn everything(&self) -> String {
        let mut result = "".to_string();
        for (uid_name, uid_cache) in self.uids.iter() {
            result.push_str(&format!(
                "Uid: \"{}\", equipment-tag \"{}\", first seen: \"{}\", last seen: \"{}\"\n",
                uid_name,
                uid_cache
                    .equipment_tag
                    .clone()
                    .unwrap_or("unknown".to_string()),
                uid_cache.first_seen_ts,
                uid_cache.last_seen_ts
            ));
            for field in &uid_cache.fields {
                let estimated_frequency = if field.1.seen_count < 2 {
                    -1
                } else {
                    (field.1.last_seen_ts - field.1.first_seen_ts) / (field.1.seen_count as i64 - 1)
                };
                result.push_str(&format!(
                    "Field: \"{}\", measurement: \"{}\", first seen: {}, last seen: {}, seen count: {}, estimated frequency: {}, differences_mean: {}, differences_stddev: {}, value: \"{}\"\n",
                    field.0.0, field.1.measurement, field.1.first_seen_ts, field.1.last_seen_ts, field.1.seen_count, estimated_frequency, field.1.last_5_ts.differences_mean(), field.1.last_5_ts.differences_stddev(), field.1.value
                ));
            }
            result.push('\n');
        }
        result
    }

    /// Check if all fields have the same last_seen timestamp as the uid
    #[allow(dead_code)]
    fn check_ts(&self) {
        for (uid_name, uid_cache) in self.uids.iter() {
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
        let current_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut result = "".to_string();
        for (uid_name, uid_cache) in self.uids.iter() {
            let mut uid_inactive = false;
            if uid_cache.is_inactive(inactivity_added_seconds) {
                result.push_str(&format!(
                    "Inactive Uid (no data at all for more than {} seconds, current age is {} seconds): uid {} equipment-tag {}\n",
                    inactivity_added_seconds, current_timestamp as i64-uid_cache.last_seen_ts, &uid_name, uid_cache.equipment_tag.clone().unwrap_or("unknown".to_string())
                ));
                uid_inactive = true;
            }
            let mut inactive_fields = Vec::new();
            for field in &uid_cache.fields {
                if field.1.is_inactive(inactivity_added_seconds) {
                    // if field.1.last_seen < limit {
                    inactive_fields.push((
                        field.0,
                        field.1.measurement.to_owned(),
                        field.1.last_seen_ts,
                    ));
                }
            }
            if !inactive_fields.is_empty() {
                inactive_fields.sort_unstable_by(|a, b| a.0.cmp(b.0));

                if uid_inactive {
                    result.push_str(&format!("Fields expected for Uid \"{}\":\n", uid_name));
                } else {
                    result.push_str(&format!(
                        "Inactive fields (no field data for more than {} seconds) for Uid \"{}\" equipment-tag \"{}\":\n",
                        inactivity_added_seconds, uid_name, uid_cache.equipment_tag.clone().unwrap_or("unknown".to_string())
                    ));
                }
                for field in &inactive_fields {
                    result.push_str(&format!(
                        "\"{}\" (measurement: \"{}\", age: {})",
                        field.0.0,
                        field.1,
                        current_timestamp as i64 - field.2
                    ));
                    result.push('\n');
                }
                result.push('\n');
            }
        }
        result
    }

    fn remove_inactive(&mut self, inactivity_added_seconds: Option<u64>) -> String {
        let inactitity_added_seconds =
            inactivity_added_seconds.unwrap_or(self.minimum_inactivity_seconds);
        let mut count_whole_uids = 0;
        let mut count_whole_uids_fields = 0;
        let mut count_single_fields = 0;

        let uids = self
            .uids
            .iter()
            .map(|u| u.0.to_string())
            .collect::<Vec<_>>();
        for uid_name in &uids {
            let uid_is_inactive = {
                self.uids
                    .get(uid_name)
                    .unwrap()
                    .is_inactive(inactitity_added_seconds)
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
                        .is_inactive(inactitity_added_seconds)
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
            "Inactive uid removed: {}, fields count for these uids: {}\nInactive fields in active uids removed: {}",
            count_whole_uids, count_whole_uids_fields, count_single_fields
        );
        result
    }

    fn uid_ages(&self) -> Ages {
        let mut uid_count = 0;
        let mut sum = 0;
        let mut maxage = 0;
        let mut minage = i64::MAX;
        let curtime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        for uid_cache in self.uids.values() {
            let uid_age = curtime - uid_cache.last_seen_ts;
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

    fn uidinfo(&self, uid: &str) -> String {
        let mut result = "".to_string();
        if let Some(uid_cache) = self.uids.get(uid) {
            for field in &uid_cache.fields {
                let estimated_frequency = if field.1.seen_count < 2 {
                    -1
                } else {
                    (field.1.last_seen_ts - field.1.first_seen_ts) / (field.1.seen_count as i64 - 1)
                };
                result.push_str(&format!(
                    "Field: \"{}\", measurement: \"{}\", first seen: {}, last seen: {}, seen count: {}, estimated frequency: {}, differences_mean: {}, differences_stddev: {}, value: \"{}\"\n",
                    field.0.0, field.1.measurement, field.1.first_seen_ts, field.1.last_seen_ts, field.1.seen_count, estimated_frequency, field.1.last_5_ts.differences_mean(), field.1.last_5_ts.differences_stddev(), field.1.value
                ));
            }
            result.push('\n');
        } else {
            result = "Uid not found".to_string();
        }
        result
    }
}

#[derive(Debug)]
struct Ages {
    min: i64,
    max: i64,
    average: i64,
}
async fn consume_and_print(
    brokers: String,
    group_id: String,
    topics: Vec<String>,
    lvc: Arc<RwLock<ValueCache>>,
) {
    let mut offsets_have_been_reset = false;

    let context = CustomContext;

    let mut config = ClientConfig::new();

    config
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "largest")
        .set("client.id", "lvc-stats")
        //.set("statistics.interval.ms", "30000")
        .set_log_level(RDKafkaLogLevel::Debug);

    let topics = &topics.iter().map(|v| v.as_str()).collect::<Vec<_>>();
    let consumer: LoggingConsumer = config
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(topics)
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
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
                let parsed_lines = influxdb_line_protocol::parse_lines(payload);
                // Grab the lock and keep it for all lines in the message
                let mut lvcguard = lvc.write().await;
                for parsed_line in parsed_lines {
                    match parsed_line {
                        Err(e) => println!("Skipping unparseable line: {}", e),
                        Ok(parsed_line) => {
                            let ParsedLine {
                                series,
                                field_set,
                                timestamp,
                            } = parsed_line;
                            if series.tag_set.is_none() {
                                // We must be able to read uid from tags, empty tag_set => no uid tag
                                continue;
                            }
                            let tags = series.tag_set.unwrap();
                            let uid = tags.iter().find(|&x| x.0 == "uid");
                            let equipment_tag = tags
                                .iter()
                                .find(|&x| x.0 == "Equipment-tag")
                                .map(|x| x.1.to_string());
                            if uid.is_none() {
                                println!("Skipping line withoud uid tag");
                                continue;
                            }
                            let _ = lvcguard.update_fields(
                                &uid.unwrap().1.to_string(),
                                equipment_tag,
                                &field_set,
                                timestamp.unwrap(),
                                series.measurement,
                            );
                            lvcguard.ilp_line_count += 1;
                        }
                    }
                }
                std::mem::drop(lvcguard);
                if let Some(headers) = m.headers() {
                    for header in headers.iter() {
                        info!("XXX  Header {:#?}: {:?}", header.key, header.value);
                    }
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}

#[derive(Parser)]
#[command(version)]
struct CliArgs {
    #[arg(short, long, env = "GAPIT_KAFKALVC_PORT", default_value_t = 3005)]
    port: u16,
    #[arg(short, long, env = "GAPIT_KAFKALVC_BIND_IP", default_value = "0.0.0.0")]
    bind_ip: IpAddr,
    #[arg(
        long,
        env = "GAPIT_KAFKALVC_OUTPUT_MEASUREMENT",
        default_value = "kafka_influx_statistics"
    )]
    output_reported_measurement: String,
    #[arg(long, env = "GAPIT_KAFKALVC_OUTPUT_REPORTED_HOST")]
    output_reported_host: Option<String>,
    #[arg(long, env = "GAPIT_KAFKALVC_BROKERS", default_value = "localhost:9092")]
    brokers: String,
    #[arg(
        long,
        short,
        env = "GAPIT_KAFKALVC_INACTIVITY_THRESHOLD",
        default_value_t = 60
    )]
    inactivity_threshold: u64,
    #[arg(
        short,
        long,
        env = "GAPIT_KAFKALVC_GROUP_ID",
        default_value = "stats_lvc_1"
    )]
    group_id: String,
    #[arg(
        short,
        long,
        env = "GAPIT_KAFKALVC_TOPICS",
        default_value = "instruments"
    )]
    topics: String,
    #[arg(long, env = "GAPIT_KAFKALVC_LOG_CONF")]
    log_conf: Option<String>,
    #[arg(long, env = "GAPIT_KAFKALVC_TLS_ENABLE", default_value_t = false)]
    tls_enabled: bool,
    #[arg(long, env = "GAPIT_KAFKALVC_TLS_KEYFILE")]
    tls_key_file: Option<String>,
    #[arg(long, env = "GAPIT_KAFKALVC_TLS_CERTFILE")]
    tls_cert_file: Option<String>,
}

#[tokio::main]
async fn main() {
    println!(
        "{} version {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );
    let args = CliArgs::parse();

    if args.tls_enabled && args.tls_key_file.is_none() || args.tls_cert_file.is_none() {
        panic!("TLS enabled but certificate file or certificate key file not specified.");
    }
    setup_logger(true, args.log_conf.as_ref());
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = args.topics.split(',').map(|t| t.to_owned()).collect();
    let brokers = args.brokers;
    let group_id = args.group_id;

    let lvc = Arc::new(RwLock::new(ValueCache::new(
        args.output_reported_measurement,
        args.inactivity_threshold,
    )));
    let state = AppState {
        data_cache: Arc::clone(&lvc),
        config_output_reported_host: args.output_reported_host,
    };

    // Start Kafka reader
    let lvcclone = Arc::clone(&lvc);
    tokio::spawn(async {
        consume_and_print(brokers, group_id, topics, lvcclone).await;
    });

    // Set up web service
    let app = Router::new()
        .route("/", get(root))
        .route("/uidcount", get(uid_count_last_minute))
        .route("/fieldcount", get(field_count_last_minute))
        .route("/everything", get(everything))
        .route("/uidinfo/{uid}", get(uidinfo))
        .route("/stats", get(stats))
        .route("/inactive", get(list_inactive))
        .route("/inactive/remove", get(remove_inactive))
        .with_state(state);

    if args.tls_enabled && args.tls_cert_file.is_some() && args.tls_key_file.is_some() {
        println!(
            "Starting HTTPS server listening on {}:{}",
            args.bind_ip, args.port
        );
        tokio::spawn(async move {
            // let bind_ip = args.bind_ip.clone();
            let port = args.port;
            https_server(
                app,
                &args.bind_ip.to_string(),
                port,
                args.tls_cert_file.unwrap().into(),
                args.tls_key_file.unwrap().into(),
            )
            .await
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
        Err(e) => panic!("Could not configure TLS: {}", e),
    };
    let address = match Ipv4Addr::from_str(address) {
        Ok(address) => IpAddr::V4(address),
        Err(e) => panic!("Couldn't parse IP address from {}: {}", address, e),
    };
    let socketaddr = SocketAddr::new(address, port);

    axum_server::bind_rustls(socketaddr, config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn root(State(state): State<AppState>) -> (StatusCode, String) {
    let lvcguard = state.data_cache.read().await;
    (StatusCode::OK, lvcguard.all_uids_and_fields())
}
async fn uid_count_last_minute(State(state): State<AppState>) -> (StatusCode, String) {
    let lvcguard = state.data_cache.read().await;
    (
        StatusCode::OK,
        format!("{}", lvcguard.uid_count_last_seconds(60)),
    )
}
async fn field_count_last_minute(State(state): State<AppState>) -> (StatusCode, String) {
    let lvcguard = state.data_cache.read().await;
    (
        StatusCode::OK,
        format!("{}", lvcguard.field_count_last_seconds(60)),
    )
}
async fn everything(State(state): State<AppState>) -> (StatusCode, String) {
    let lvcguard = state.data_cache.read().await;
    (StatusCode::OK, lvcguard.everything())
}
async fn uidinfo(Path(uid): Path<String>, State(state): State<AppState>) -> (StatusCode, String) {
    let lvcguard = state.data_cache.read().await;
    (StatusCode::OK, lvcguard.uidinfo(&uid))
}

// Openmetrics:
// # HELP <metricname> <some help text or description>
// # TYPE <metricname> [counter|gauge|etc]
// # UNIT <metricname> [seconds|fields|etc] # If UNIT is used, it must also be used as the
// suffix.
// metricname[_unit]_suffix{[label_a=value_a]} [int64|float64|bool]
// # HELP lvc_service_uid_total Current count of unique uids in cache
// # TYPE lv_service_uid_total gauge
// lvc_service_uid_total <val>
// # HELP lvc_service_fields_seen_total Fields processed by service
// # TYPE lvc_service_fields_seen_total counter
// # UNIT lvc_service_fields_seen_total fields
// lvc_service_fields_seen_total <val>
async fn stats(State(state): State<AppState>) -> (StatusCode, String) {
    let lvcguard = state.data_cache.read().await;
    let ages = lvcguard.uid_ages();
    let host_tag = if let Some(host) = state.config_output_reported_host {
        format!(",host={}", host)
    } else {
        "".to_string()
    };
    let ret = format!(
        "{}{} timeperiod_length_seconds={},uids_total={},uids_inactive={},uid_fields_total={},uid_fields_inactive={},uid_age_min={},uid_age_max={},uid_age_mean={},ilp_lines_total={},fields_seen_total={}\n",
        lvcguard.output_measurement,
        host_tag,
        lvcguard.last_value_ts - lvcguard.first_value_ts,
        lvcguard.total_uid_count(),
        lvcguard.inactive_uid_count(None),
        lvcguard.total_field_count(),
        lvcguard.inactive_field_count(None),
        ages.min,
        ages.max,
        ages.average,
        lvcguard.ilp_line_count,
        lvcguard.fields_seen_count,
    );
    (StatusCode::OK, ret)
}
async fn list_inactive(State(state): State<AppState>) -> (StatusCode, String) {
    let lvcguard = state.data_cache.read().await;
    let ret = lvcguard.list_inactive(None);
    (StatusCode::OK, ret)
}
async fn remove_inactive(State(state): State<AppState>) -> (StatusCode, String) {
    let mut lvcguard = state.data_cache.write().await;
    let ret = lvcguard.remove_inactive(None);
    (StatusCode::OK, ret)
}

#[derive(Debug, Clone)]
struct AppState {
    data_cache: Arc<RwLock<ValueCache>>,
    config_output_reported_host: Option<String>,
}

#[cfg(test)]
mod test {
    use super::Last5Timestamps;

    #[test]
    fn test_last_5() {
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
    fn test_last_5_stddev() {
        let mut last = Last5Timestamps::new();
        last.push(10);
        last.push(20);
        last.push(40);
        assert_eq!(7.0710678118654755, last.differences_stddev());
    }
}
