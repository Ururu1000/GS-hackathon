//! `beacon --role ingest,sources,matcher,aggregator,relay,router,delivery,gateway [--migrate]`
//!
//! Roles are comma-separated and may repeat `--role`. Each team adds its roles to `Role`.
//! Configuration comes from the environment:
//!
//! | Variable             | Default                                   | Used by           |
//! |----------------------|-------------------------------------------|-------------------|
//! | `DATABASE_URL`       | (required)                                | all               |
//! | `BEACON_CONFIG_DIR`  | `config`                                  | router            |
//! | `SMTP_HOST`          | `localhost`                               | delivery          |
//! | `SMTP_PORT`          | `1025`                                    | delivery          |
//! | `MAIL_FROM`          | `Beacon <beacon@example.test>`            | delivery          |
//! | `DELIVERY_WORKERS`   | `1` (worker loops per channel per process)| delivery          |
//! | `DELIVERY_CHANNELS`  | `email,sms,phone`                         | delivery          |
//! | `SMS_GATEWAY_URL`    | `http://localhost:8070`                   | delivery          |
//! | `PUBLIC_URL`         | `http://localhost:5173` (SMS short links) | delivery          |
//! | `GATEWAY_ADDR`       | `0.0.0.0:8070`                            | gateway           |
//! | `BEACON_KAFKA`       | `localhost:19092`                         | part (a) roles    |
//! | `INGEST_ADDR`        | `0.0.0.0:8090`                            | ingest            |
//! | `INGEST_MAX_INFLIGHT`| `20000` (events in flight before 429)     | ingest            |
//! | `MATCHER_WORKERS`    | `2` (consumers in the matcher group)      | matcher           |
//! | `AGGREGATOR_WORKERS` | `2` (consumers in the aggregator group)   | aggregator        |
//! | `RELAY_WORKERS`      | `1`                                       | relay             |

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use beacon_delivery::channel::Channel;
use beacon_delivery::channel::email::{EmailChannel, SmtpConfig};
use beacon_delivery::channel::gateway_client::{GatewayChannel, GatewayClientConfig, GatewayKind};
use beacon_delivery::config::Directory;
use beacon_delivery::{RouterConfig, WorkerConfig};
use sqlx::postgres::PgPoolOptions;
use tracing::info;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    /// HTTP webhook -> events.raw (part a)
    Ingest,
    /// external Kafka topics from config/sources -> events.raw (part a)
    Sources,
    /// events.raw -> normalize + match -> alerts.matches (part a)
    Matcher,
    /// alerts.matches -> dedup -> alerts + outbox (part a)
    Aggregator,
    /// outbox 'alert_update' rows -> alerts.updates (part a)
    Relay,
    /// outbox 'notify' rows -> delivery_jobs (part b)
    Router,
    /// channel workers: delivery_jobs -> email / sms / phone (part b)
    Delivery,
    /// fake SMS/phone provider + device-simulator API (part b)
    Gateway,
}

const ROLES: &str = "ingest, sources, matcher, aggregator, relay, router, delivery, gateway";

impl Role {
    fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "ingest" => Self::Ingest,
            "sources" => Self::Sources,
            "matcher" => Self::Matcher,
            "aggregator" => Self::Aggregator,
            "relay" => Self::Relay,
            "router" => Self::Router,
            "delivery" => Self::Delivery,
            "gateway" => Self::Gateway,
            other => bail!("unknown role {other:?} (known: {ROLES})"),
        })
    }

    fn uses_kafka(self) -> bool {
        matches!(
            self,
            Self::Ingest | Self::Sources | Self::Matcher | Self::Aggregator | Self::Relay
        )
    }
}

/// Run a role loop, restarting it with backoff when it fails (e.g. a broker
/// outage), so one failing role doesn't take the process down.
async fn supervise<F, Fut>(
    name: &'static str,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    mut f: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut backoff = std::time::Duration::from_millis(500);
    loop {
        match f().await {
            Ok(()) => return,
            Err(e) => {
                tracing::error!(
                    role = name,
                    error = format!("{e:#}"),
                    "role failed; restarting"
                );
                tokio::select! {
                    _ = shutdown.changed() => return,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(std::time::Duration::from_secs(10));
            }
        }
    }
}

struct Args {
    roles: Vec<Role>,
    migrate: bool,
}

fn parse_args() -> Result<Args> {
    let mut roles = Vec::new();
    let mut migrate = false;
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let list = match arg.as_str() {
            "--role" => it.next().context("--role needs a value")?,
            "--migrate" => {
                migrate = true;
                continue;
            }
            "-h" | "--help" => {
                println!("usage: beacon [--migrate] --role <role>[,<role>...]\nroles: {ROLES}");
                std::process::exit(0);
            }
            s if s.starts_with("--role=") => s["--role=".len()..].to_string(),
            other => bail!("unexpected argument {other:?}"),
        };
        for r in list.split(',').map(str::trim).filter(|r| !r.is_empty()) {
            let role = Role::parse(r)?;
            if !roles.contains(&role) {
                roles.push(role);
            }
        }
    }
    if roles.is_empty() && !migrate {
        bail!("no roles given; use --role <role>[,<role>...] (roles: {ROLES})");
    }
    Ok(Args { roles, migrate })
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();

    let args = parse_args()?;
    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&db_url)
        .await
        .context("connecting to Postgres")?;

    if args.migrate {
        beacon_delivery::MIGRATOR
            .run(&pool)
            .await
            .context("running migrations")?;
        info!("migrations applied");
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();

    let kafka = beacon_ingest::kafka::bootstrap_from_env();
    if args.roles.iter().any(|r| r.uses_kafka()) {
        beacon_ingest::kafka::ensure_topics(&kafka)
            .await
            .with_context(|| format!("creating Kafka topics on {kafka}"))?;
    }
    let config_dir = PathBuf::from(env_or("BEACON_CONFIG_DIR", "config"));
    let workers = |key: &str, default: &str| -> Result<usize> {
        Ok(env_or(key, default)
            .parse::<usize>()
            .with_context(|| key.to_string())?
            .max(1))
    };

    for role in &args.roles {
        match role {
            Role::Ingest => {
                let addr = env_or("INGEST_ADDR", "0.0.0.0:8090");
                let max_inflight = workers("INGEST_MAX_INFLIGHT", "20000")?;
                let publisher =
                    beacon_ingest::EventPublisher::new(beacon_ingest::kafka::producer(&kafka)?);
                let state = beacon_ingest::http::IngestState::new(publisher, max_inflight);
                let app = beacon_ingest::http::router(state);
                let listener = tokio::net::TcpListener::bind(&addr)
                    .await
                    .with_context(|| format!("binding {addr}"))?;
                info!(%addr, "ingest listening");
                let mut rx = shutdown_rx.clone();
                tasks.spawn(async move {
                    let served = axum::serve(listener, app)
                        .with_graceful_shutdown(async move {
                            let _ = rx.changed().await;
                        })
                        .await;
                    if let Err(e) = served {
                        tracing::error!(error = %e, "ingest server failed");
                    }
                });
            }
            Role::Sources => {
                let specs = beacon_ingest::kafka_source::load_sources(&config_dir.join("sources"))?;
                info!(sources = specs.len(), "kafka sources starting");
                let publisher =
                    beacon_ingest::EventPublisher::new(beacon_ingest::kafka::producer(&kafka)?);
                for spec in specs {
                    let (kafka, publisher, rx) =
                        (kafka.clone(), publisher.clone(), shutdown_rx.clone());
                    tasks.spawn(supervise("sources", rx.clone(), move || {
                        beacon_ingest::kafka_source::run_kafka_source(
                            spec.clone(),
                            kafka.clone(),
                            publisher.clone(),
                            rx.clone(),
                        )
                    }));
                }
            }
            Role::Matcher => {
                let snapshot =
                    std::sync::Arc::new(beacon_core::load_config_dir(&config_dir).with_context(
                        || format!("loading rules and schemas from {}", config_dir.display()),
                    )?);
                for _ in 0..workers("MATCHER_WORKERS", "2")? {
                    let (kafka, snapshot, rx) =
                        (kafka.clone(), snapshot.clone(), shutdown_rx.clone());
                    tasks.spawn(supervise("matcher", rx.clone(), move || {
                        beacon_engine::matcher::run_matcher(
                            kafka.clone(),
                            snapshot.clone(),
                            rx.clone(),
                        )
                    }));
                }
            }
            Role::Aggregator => {
                for _ in 0..workers("AGGREGATOR_WORKERS", "2")? {
                    let (kafka, pool, rx) = (kafka.clone(), pool.clone(), shutdown_rx.clone());
                    tasks.spawn(supervise("aggregator", rx.clone(), move || {
                        beacon_engine::aggregator::run_aggregator(
                            kafka.clone(),
                            pool.clone(),
                            rx.clone(),
                        )
                    }));
                }
            }
            Role::Relay => {
                for _ in 0..workers("RELAY_WORKERS", "1")? {
                    let (kafka, pool, rx) = (kafka.clone(), pool.clone(), shutdown_rx.clone());
                    tasks.spawn(supervise("relay", rx.clone(), move || {
                        beacon_engine::relay::run_relay(kafka.clone(), pool.clone(), rx.clone())
                    }));
                }
            }
            Role::Router => {
                let dir_path = PathBuf::from(env_or("BEACON_CONFIG_DIR", "config"));
                let dir = Directory::load(&dir_path)
                    .with_context(|| format!("loading people/teams from {}", dir_path.display()))?;
                info!(
                    people = dir.people.len(),
                    teams = dir.teams.len(),
                    "router starting"
                );
                tasks.spawn(beacon_delivery::router::run_router(
                    pool.clone(),
                    Arc::new(dir),
                    RouterConfig::default(),
                    shutdown_rx.clone(),
                ));
            }
            Role::Delivery => {
                let workers: usize = env_or("DELIVERY_WORKERS", "1")
                    .parse()
                    .context("DELIVERY_WORKERS")?;
                let gateway_cfg = GatewayClientConfig {
                    base_url: env_or("SMS_GATEWAY_URL", "http://localhost:8070"),
                    public_url: env_or("PUBLIC_URL", "http://localhost:5173"),
                    ..GatewayClientConfig::default()
                };
                for name in env_or("DELIVERY_CHANNELS", "email,sms,phone")
                    .split(',')
                    .map(str::trim)
                    .filter(|c| !c.is_empty())
                {
                    let channel: Arc<dyn Channel> = match name {
                        "email" => {
                            let smtp = SmtpConfig {
                                host: env_or("SMTP_HOST", "localhost"),
                                port: env_or("SMTP_PORT", "1025").parse().context("SMTP_PORT")?,
                                from: env_or("MAIL_FROM", "Beacon <beacon@example.test>"),
                                ..SmtpConfig::default()
                            };
                            info!(host = %smtp.host, port = smtp.port, "email worker starting");
                            Arc::new(EmailChannel::new(smtp).map_err(anyhow::Error::msg)?)
                        }
                        "sms" | "phone" => {
                            let kind = if name == "sms" {
                                GatewayKind::Sms
                            } else {
                                GatewayKind::Phone
                            };
                            info!(channel = name, gateway = %gateway_cfg.base_url, "gateway worker starting");
                            Arc::new(
                                GatewayChannel::new(kind, gateway_cfg.clone())
                                    .map_err(anyhow::Error::msg)?,
                            )
                        }
                        other => bail!(
                            "DELIVERY_CHANNELS: unknown channel {other:?} (email, sms, phone)"
                        ),
                    };
                    for _ in 0..workers.max(1) {
                        tasks.spawn(beacon_delivery::worker::run_worker(
                            pool.clone(),
                            channel.clone(),
                            WorkerConfig::default(),
                            shutdown_rx.clone(),
                        ));
                    }
                }
            }
            Role::Gateway => {
                let addr: std::net::SocketAddr = env_or("GATEWAY_ADDR", "0.0.0.0:8070")
                    .parse()
                    .context("GATEWAY_ADDR")?;
                let state = beacon_delivery::gateway::GatewayState::new(pool.clone());
                let rx = shutdown_rx.clone();
                tasks.spawn(async move {
                    if let Err(e) = beacon_delivery::gateway::serve(state, addr, rx, |bound| {
                        info!(%bound, "fake SMS/phone gateway listening");
                    })
                    .await
                    {
                        tracing::error!(error = %e, "gateway stopped");
                    }
                });
            }
        }
    }

    if tasks.is_empty() {
        return Ok(());
    }

    let terminate = async {
        #[cfg(unix)]
        {
            let mut sig = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("installing SIGTERM handler");
            sig.recv().await;
        }
        #[cfg(not(unix))]
        std::future::pending::<()>().await;
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("shutting down"),
        // docker stop / kill send SIGTERM: leave consumer groups cleanly.
        _ = terminate => info!("SIGTERM: shutting down"),
        Some(res) = tasks.join_next() => {
            if let Err(e) = res { tracing::error!(error = %e, "role task panicked"); }
        }
    }
    let _ = shutdown_tx.send(true);
    while tasks.join_next().await.is_some() {}
    Ok(())
}
