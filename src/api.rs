#![warn(clippy::pedantic)]
use crate::ValueCache;
use crate::config::CliArgs;
use axum::{Router, extract::Path, extract::State, http::StatusCode, routing::get};
use axum_server::tls_rustls::RustlsConfig;
use serde::Deserialize;
use std::fmt::Debug;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

pub(crate) async fn start(args: CliArgs, state: AppState) {
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
        .route("/least_changed_fields/{count}", get(least_changed_fields))
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
                "{}{} timeperiod_length_seconds={},uid_all_count={},uid_inactive_count={},uid_plus_field_combination_count={},uid_field_inactive_count={},uid_age_min={},uid_age_max={},uid_age_mean={},kafka_message_count={},ilp_line_count={},field_count={},changed_fields_count={},unique_field_name_count={},fields_sent_initial_count={},fields_sent_suppressed_count={},fields_sent_changed_count={},fields_sent_timeout={}\n",
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
                valuecache.unique_field_name_count().0,
                valuecache.fields_sent_initial_count,
                valuecache.fields_sent_suppressed_count,
                valuecache.fields_sent_changed_count,
                valuecache.fields_sent_timeout_count,
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

async fn least_changed_fields(
    Path(count): Path<String>,
    State(state): State<AppState>,
) -> (StatusCode, String) {
    match count.parse::<usize>() {
        Ok(count) => match state.data_cache.lock() {
            Ok(data_cache) => {
                let mut least_changed = data_cache
                    .uids
                    .iter()
                    .flat_map(|u| {
                        u.1.fields
                            .iter()
                            .map(|((fieldname, _), fieldinfo)| {
                                (fieldname.clone(), fieldinfo.changed_count)
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                least_changed.sort_by_key(|v| v.1);
                let ret = least_changed
                    .iter()
                    .take(count)
                    .map(|v| format!("{}: {}\n", v.0, v.1))
                    .collect::<Vec<_>>();
                let ret = ret.join("");
                (StatusCode::OK, ret)
            }
            Err(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Couldn't lock mutex".to_string(),
            ),
        },
        Err(_) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("Couldn't parse {count} into usize"),
        ),
    }
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) data_cache: Arc<Mutex<ValueCache>>,
    pub(crate) config_output_reported_host: Option<String>,
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
