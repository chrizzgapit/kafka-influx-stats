use std::{collections::VecDeque, io::Write};
use std::thread;

use chrono::prelude::*;
use env_logger::Builder;
use env_logger::fmt::Formatter;
use log::{LevelFilter, Record};

pub fn setup_logger(log_thread: bool, rust_log: Option<&String>) {
    let output_format = move |formatter: &mut Formatter, record: &Record| {
        let thread_name = if log_thread {
            format!("(t: {}) ", thread::current().name().unwrap_or("unknown"))
        } else {
            String::new()
        };

        let local_time: DateTime<Local> = Local::now();
        let time_str = local_time.format("%H:%M:%S%.3f").to_string();
        writeln!(
            formatter,
            "{} {}{} - {} - {}",
            time_str,
            thread_name,
            record.level(),
            record.target(),
            record.args()
        )
    };

    let mut builder = Builder::new();
    builder
        .format(output_format)
        .filter(None, LevelFilter::Info);

    rust_log.map(|conf| builder.parse_filters(conf));

    builder.init();
}

#[derive(Debug, Clone)]
pub(crate) struct Last5Timestamps {
    timestamps: VecDeque<i64>,
}

#[allow(dead_code)]
impl Last5Timestamps {
    pub(crate) fn new() -> Self {
        Self {
            timestamps: VecDeque::with_capacity(5),
        }
    }
    pub(crate) fn new_with_val(timestamp: i64) -> Self {
        let mut ret = Self {
            timestamps: VecDeque::with_capacity(5),
        };
        ret.timestamps.push_back(timestamp);
        ret
    }
    pub(crate) fn push(&mut self, val: i64) {
        let mut cur_count = self.timestamps.len();
        while cur_count >= 5 {
            let _ = self.timestamps.pop_front();
            cur_count -= 1;
        }
        self.timestamps.push_back(val);
    }
    pub(crate) fn pop(&mut self) -> Option<i64> {
        self.timestamps.pop_front()
    }
    pub(crate) fn drop_front(&mut self) {
        if self.timestamps.is_empty() {
            return;
        }
        let _ = self.timestamps.pop_front();
    }
    pub(crate) fn len(&self) -> usize {
        self.timestamps.len()
    }
    pub(crate) fn differences(&self) -> Vec<i64> {
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
    pub(crate) fn differences_mean(&self) -> i64 {
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
    pub(crate) fn differences_stddev(&self) -> f64 {
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
    pub(crate) fn to_string(&self) -> String {
        self.timestamps
            .iter()
            .map(|&v| format!("{v}"))
            .collect::<Vec<String>>()
            .join(", ")

    }
}