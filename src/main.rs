#![warn(clippy::pedantic)]
use crate::cache::ValueCache;
use crate::consumer::consume_and_process;
use crate::stats_utils::curtime;
use clap::Parser;
use std::sync::{Arc, Mutex};

use log::{info, warn};
use tokio::signal;

use rdkafka::util::get_rdkafka_version;

use crate::stats_utils::{Last5Timestamps, setup_logger};
mod api;
mod config;
use config::Precision;
mod cache;
mod consumer;
mod stats_utils;

#[tokio::main]
async fn main() {
    println!(
        "{} version {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );
    let args = config::CliArgs::parse();
    assert!(
        !(args.tls_enabled && (args.tls_key_file.is_none() || args.tls_cert_file.is_none())),
        "TLS enabled but certificate file or certificate key file not specified."
    );

    setup_logger(true, args.log_conf.as_ref());
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{version_n:08x}, {version_s}");

    let kafkastats = Arc::new(Mutex::new(ValueCache::new(
        args.output_reported_measurement.clone(),
        args.inactivity_threshold,
        args.precision,
    )));

    let state = api::AppState {
        // data_cache: Arc::clone(stats),
        data_cache: Arc::clone(&kafkastats),
        config_output_reported_host: args.output_reported_host.clone(),
    };

    // Start Kafka consumer
    let topics = args
        .topics
        .split(',')
        .map(std::borrow::ToOwned::to_owned)
        .collect();
    let brokers = args.brokers.clone();
    let group_id = args.group_id.clone();
    let statsclone = Arc::clone(&kafkastats);
    tokio::spawn(async {
        consume_and_process(brokers, group_id, topics, statsclone).await;
    });

    // Set up web service
    api::start(args, state).await;
    let _ = signal::ctrl_c().await;
}
