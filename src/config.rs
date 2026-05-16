#![warn(clippy::pedantic)]
use clap::Parser;
use std::fmt::Display;
use std::net::IpAddr;

#[derive(Parser)]
#[command(version)]
pub(crate) struct CliArgs {
    #[arg(short, long, env = "GAPIT_KAFKASTATS_PORT", default_value_t = 3005)]
    pub(crate) port: u16,

    #[arg(
        short,
        long,
        env = "GAPIT_KAFKASTATS_BIND_IP",
        default_value = "0.0.0.0"
    )]
    pub(crate) bind_ip: IpAddr,

    #[arg(
        long,
        env = "GAPIT_KAFKASTATS_OUTPUT_MEASUREMENT",
        default_value = "kafka_influx_statistics"
    )]
    pub(crate) output_reported_measurement: String,

    #[arg(long, env = "GAPIT_KAFKASTATS_OUTPUT_REPORTED_HOST")]
    pub(crate) output_reported_host: Option<String>,

    #[arg(
        long,
        env = "GAPIT_KAFKASTATS_BROKERS",
        default_value = "localhost:9092"
    )]
    pub(crate) brokers: String,

    #[arg(
        long,
        short,
        env = "GAPIT_KAFKASTATS_INACTIVITY_THRESHOLD",
        default_value_t = 60
    )]
    pub(crate) inactivity_threshold: u64,

    #[arg(
        short,
        long,
        env = "GAPIT_KAFKASTATS_GROUP_ID",
        default_value = "kafkastats_1"
    )]
    pub(crate) group_id: String,

    #[arg(
        short,
        long,
        env = "GAPIT_KAFKASTATS_TOPICS",
        default_value = "instruments"
    )]
    pub(crate) topics: String,

    #[arg(long, env = "GAPIT_KAFKASTATS_PRECISION", default_value_t = Precision::Second)]
    pub(crate) precision: Precision,

    #[arg(long, env = "GAPIT_KAFKASTATS_LOG_CONF")]
    pub(crate) log_conf: Option<String>,

    #[arg(long, env = "GAPIT_KAFKASTATS_TLS_ENABLED", default_value_t = false)]
    pub(crate) tls_enabled: bool,

    #[arg(long, env = "GAPIT_KAFKASTATS_TLS_KEYFILE")]
    pub(crate) tls_key_file: Option<String>,

    #[arg(long, env = "GAPIT_KAFKASTATS_TLS_CERTFILE")]
    pub(crate) tls_cert_file: Option<String>,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) enum Precision {
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
