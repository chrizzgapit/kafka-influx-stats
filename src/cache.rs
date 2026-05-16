#![warn(clippy::pedantic)]
use crate::Last5Timestamps;
use crate::Precision;
use crate::stats_utils::curtime;

use influxdb_line_protocol::{EscapedStr, FieldValue};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Write;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct ValueCache {
    pub(crate) output_measurement: String,
    pub(crate) precision: Precision,
    pub(crate) created_timestamp: i64,
    pub(crate) first_value_ts: i64,
    pub(crate) last_value_ts: i64,
    pub(crate) minimum_inactivity_seconds: u64,
    pub(crate) kafka_message_count: usize,
    pub(crate) ilp_line_count: usize,
    pub(crate) fields_seen_count: usize,
    pub(crate) fields_changed_count: usize,
    pub(crate) fields_sent_changed_count: usize,
    pub(crate) fields_sent_timeout_count: usize,
    pub(crate) fields_sent_initial_count: usize,
    pub(crate) fields_sent_suppressed_count: usize,
    pub(crate) uids: HashMap<String, UidInfo>,
    // field_names: HashSet<String>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct UidInfo {
    pub(crate) first_seen_ts: i64,
    pub(crate) last_seen_ts: i64,
    pub(crate) seen_count: usize,
    pub(crate) equipment_tag: Option<String>,
    pub(crate) fields: HashMap<(String, String), FieldInfo>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct FieldInfo {
    pub(crate) measurement: String,
    pub(crate) value: InfluxValue,
    pub(crate) first_seen_ts: i64,
    pub(crate) last_seen_ts: i64,
    pub(crate) last_changed_ts: i64,
    pub(crate) last_sent_ts: i64,
    pub(crate) last_5_ts: Last5Timestamps,
    pub(crate) seen_count: usize,
    pub(crate) changed_count: usize,
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct Ages {
    pub(crate) min: i64,
    pub(crate) max: i64,
    pub(crate) average: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum InfluxValue {
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

#[allow(dead_code)]
impl UidInfo {
    fn is_inactive(&self, inactivity_added_seconds: u64) -> bool {
        self.fields
            .iter()
            .filter(|f| !f.1.is_inactive(inactivity_added_seconds))
            .count()
            == 0
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

#[allow(dead_code)]
impl ValueCache {
    pub(crate) fn new(
        output_measurement: String,
        inactivity_threshold: u64,
        precision: Precision,
    ) -> Self {
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
            fields_sent_changed_count: 0,
            fields_sent_timeout_count: 0,
            fields_sent_initial_count: 0,
            fields_sent_suppressed_count: 0,
        }
    }

    pub(crate) fn add_or_update_fields(
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
            .and_modify(|uidinfo| {
                uidinfo.last_seen_ts = uidinfo.last_seen_ts.max(timestamp);
                uidinfo.seen_count += 1;
                for (field_name, field_value) in field_set {
                    uidinfo
                        .fields
                        .entry((field_name.to_string(), measurement.to_string()))
                        .and_modify(|fieldinfo| {
                            let new_val: InfluxValue = field_value.to_owned().into();
                            if fieldinfo.value != new_val {
                                self.fields_changed_count += 1;
                                self.fields_sent_changed_count += 1;
                                fieldinfo.last_changed_ts = timestamp;
                                fieldinfo.last_sent_ts = timestamp;
                                fieldinfo.changed_count += 1;
                            } else if timestamp >= fieldinfo.last_sent_ts + 60 {
                                fieldinfo.last_sent_ts = timestamp;
                                self.fields_sent_timeout_count += 1;
                            } else {
                                self.fields_sent_suppressed_count += 1;
                            }
                            fieldinfo.value = new_val;
                            fieldinfo.last_seen_ts = fieldinfo.last_seen_ts.max(timestamp);
                            fieldinfo.seen_count += 1;
                            fieldinfo.last_5_ts.push(timestamp);
                        })
                        .or_insert_with(|| {
                            self.fields_sent_initial_count += 1;
                            //self.field_names.insert(field_name.to_string());
                            FieldInfo {
                                value: field_value.to_owned().into(),
                                first_seen_ts: timestamp,
                                last_seen_ts: timestamp,
                                last_changed_ts: timestamp,
                                last_sent_ts: timestamp,
                                seen_count: 1,
                                measurement: measurement.to_string(),
                                last_5_ts: Last5Timestamps::new_with_val(timestamp),
                                changed_count: 0,
                            }
                        });
                }
            })
            .or_insert_with(|| {
                UidInfo {
                    first_seen_ts: timestamp,
                    last_seen_ts: timestamp,
                    seen_count: 1,
                    equipment_tag,
                    fields: {
                        let mut hm = HashMap::new();
                        self.fields_sent_initial_count += field_set.len();
                        for (field_name, field_value) in field_set {
                            //self.field_names.insert(field_name.to_string());
                            hm.insert(
                                (field_name.to_string(), measurement.to_string()),
                                FieldInfo {
                                    value: field_value.to_owned().into(),
                                    first_seen_ts: timestamp,
                                    last_seen_ts: timestamp,
                                    last_sent_ts: timestamp,
                                    last_changed_ts: timestamp,
                                    seen_count: 1,
                                    measurement: measurement.to_string(),
                                    last_5_ts: Last5Timestamps::new_with_val(timestamp),
                                    changed_count: 0,
                                },
                            );
                        }
                        hm
                    },
                }
            });
    }

    pub(crate) fn uid_count_last_seconds(&self, seconds: i64) -> usize {
        let oldest_allowed_timestamp = curtime() - seconds;
        self.uids
            .iter()
            .filter(|u| u.1.last_seen_ts >= oldest_allowed_timestamp)
            .count()
    }

    pub(crate) fn total_uid_count(&self) -> usize {
        self.uids.len()
    }

    /// This does not take expected frequency in consideration, only seconds since last value.
    pub(crate) fn inactive_uid_count(&self, added_seconds: Option<u64>) -> usize {
        let inactivity_added_seconds = added_seconds.unwrap_or(self.minimum_inactivity_seconds);
        self.uids
            .iter()
            .filter(|u| u.1.is_inactive(inactivity_added_seconds))
            .count()
    }

    pub(crate) fn field_count_last_seconds(&self, seconds: i64) -> usize {
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

    pub(crate) fn total_field_count(&self) -> usize {
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

    pub(crate) fn inactive_field_count(&self, added_seconds: Option<u64>) -> usize {
        let inactivity_added_seconds = added_seconds.unwrap_or(self.minimum_inactivity_seconds);
        self.uids
            .values()
            .map(|uidinfo| {
                if uidinfo.seen_count <= 1 {
                    0
                } else {
                    uidinfo
                        .fields
                        .iter()
                        .filter(|f| f.1.is_inactive(inactivity_added_seconds))
                        .count()
                }
            })
            .sum()
    }

    pub(crate) fn uids_and_fields_to_string(&self) -> String {
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
                    .unwrap_or_else(|| "unknown".to_string()),
                fields
            );
        }
        result
    }

    #[allow(clippy::cast_possible_wrap)]
    pub(crate) fn everything_to_string(&self) -> String {
        let mut result = String::new();
        for (uid_name, uid_cache) in &self.uids {
            let _ = writeln!(
                result,
                "Uid: \"{}\", equipment-tag \"{}\", first seen: \"{}\", last seen: \"{}\"",
                uid_name,
                uid_cache
                    .equipment_tag
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
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
    pub(crate) fn get_field_and_uid_ts_mismatches(&self) {
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
                            .unwrap_or_else(|| "unknown".to_string()),
                        uid_cache.last_seen_ts,
                        field.0.0,
                        field.1.last_seen_ts
                    );
                }
            }
        }
    }

    pub(crate) fn list_inactive(&self, inactivity_added_seconds: Option<u64>) -> String {
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
                        .unwrap_or_else(|| "unknown".to_string())
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
                            .unwrap_or_else(|| "unknown".to_string())
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

    pub(crate) fn unique_field_name_count(&self) -> (usize, usize) {
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

    pub(crate) fn remove_inactive_uids_and_fields(
        &mut self,
        inactivity_added_seconds: Option<u64>,
    ) -> String {
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
    pub(crate) fn remove_uid_if_inactive(
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

    pub(crate) fn get_uid_age_stats(&self) -> Ages {
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
    pub(crate) fn get_uid_info(&self, uid: &str) -> String {
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

    pub(crate) fn get_uid_field_info(
        &self,
        uid: &str,
        measurement: &str,
        fieldname: &str,
    ) -> String {
        let mut result = String::new();
        if let Some(uid_cache) = self.uids.get(uid) {
            if let Some(field) = uid_cache
                .fields
                .get(&(fieldname.to_string(), measurement.to_string()))
            {
                let values: String = field.last_5_ts.to_string();
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
    pub(crate) fn changed_fields_last_seconds(&self, seconds: i64) -> usize {
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
