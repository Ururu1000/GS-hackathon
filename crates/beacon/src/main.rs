//! `beacon --role ingest,sources,matcher,aggregator,relay,router,delivery,gateway,scheduler [--migrate]`
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
//! | `GATEWAY_ADDR`       | `127.0.0.1:8070` (set `0.0.0.0:8070` in a container) | gateway |
//! | `GATEWAY_TOKEN`      | unset = unauthenticated dev mode (warns)  | gateway, delivery, api (simulator mount) |
//! | `ESCALATE_AFTER_OVERRIDE_MS` | unset (demo: replaces rule `escalate_after`) | router |
//! | `REPLY_SECRET`       | unset = SMS replies rejected              | gateway, api, delivery-api |
//! | `DELIVERY_API_ADDR`  | `127.0.0.1:8071`                          | delivery-api      |
//! | `DELIVERY_API_URL`   | this process's `API_ADDR` with the api role (and no delivery-api), else `http://127.0.0.1:8071` (reply forward) | gateway |
//! | `DELIVERY_API_DEMO_ACTOR` | `user:bob` (dev only)                | delivery-api      |
//! | `DELIVERY_API_DEMO_HEADER` | `0`; `1` honours `X-Beacon-Demo-Actor` (dev only) | delivery-api |
//! | `SMTP_TLS`           | `none` \| `starttls` \| `tls`            | delivery          |
//! | `SMTP_USERNAME` / `SMTP_PASSWORD` | unset                        | delivery          |
//! | `SMTP_ALLOW_PLAINTEXT` | `0`; `1` allows plaintext to a non-local host | delivery    |
//! | `GATEWAY_MAX_STREAMS` | `64` concurrent simulator SSE streams    | gateway           |
//! | `DB_MAX_CONNECTIONS` | sized from the roles (see `pool_size`)    | all               |
//! | `SHUTDOWN_GRACE_SECS`| `20` (drain time after SIGTERM/SIGINT)    | all               |
//! | `BEACON_KAFKA`       | `localhost:19092`                         | part (a) roles    |
//! | `INGEST_ADDR`        | `0.0.0.0:8090`                            | ingest            |
//! | `INGEST_MAX_INFLIGHT`| `20000` (events in flight before 429)     | ingest            |
//! | `MATCHER_WORKERS`    | `8` (consumers in the matcher group)      | matcher           |
//! | `AGGREGATOR_WORKERS` | `2` (consumers in the aggregator group)   | aggregator        |
//! | `RELAY_WORKERS`      | `1`                                       | relay             |
//! | `API_ADDR`           | `127.0.0.1:8080` (no auth yet: local only) | api              |
//! | `BEACON_DEMO_USER`   | `anna` (fixed demo session user)          | api               |
//! | `BEACON_LLM`         | `off`; `mock` or `claude` turns on mapping suggestions | api |
//! | `BEACON_LLM_MODEL`   | `claude-opus-5-5` (with `BEACON_LLM=claude`, needs `ANTHROPIC_API_KEY`) | api |
//! | `BEACON_UI_DIR`      | `ui/dist` if it exists (static UI)        | api               |

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
    /// REST + SSE for the UI, serves the built UI (beacon-api)
    Api,
    /// outbox 'notify' rows -> delivery_jobs (part b)
    Router,
    /// channel workers: delivery_jobs -> email / sms / phone (part b)
    Delivery,
    /// fake SMS/phone provider + device-simulator API (part b)
    Gateway,
    /// escalation timers -> next on-call / lead (part b)
    Scheduler,
    /// runbook action executions (part b, M5b)
    Actions,
    /// delivery API standalone dev server until beacon-api mounts it (part b, M5b)
    DeliveryApi,
}

const ROLES: &str = "ingest, sources, matcher, aggregator, relay, api, router, delivery, gateway, scheduler, actions, delivery-api";

impl Role {
    fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "ingest" => Self::Ingest,
            "sources" => Self::Sources,
            "matcher" => Self::Matcher,
            "aggregator" => Self::Aggregator,
            "relay" => Self::Relay,
            "api" => Self::Api,
            "router" => Self::Router,
            "delivery" => Self::Delivery,
            "gateway" => Self::Gateway,
            "scheduler" => Self::Scheduler,
            "actions" => Self::Actions,
            "delivery-api" => Self::DeliveryApi,
            other => bail!("unknown role {other:?} (known: {ROLES})"),
        })
    }

    fn uses_kafka(self) -> bool {
        matches!(
            self,
            Self::Ingest
                | Self::Sources
                | Self::Matcher
                | Self::Aggregator
                | Self::Relay
                | Self::Api
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
    let grace: u64 = env_or("SHUTDOWN_GRACE_SECS", "20")
        .parse()
        .context("SHUTDOWN_GRACE_SECS")?;
    let max_connections = match std::env::var("DB_MAX_CONNECTIONS") {
        Ok(v) => v.parse::<u32>().context("DB_MAX_CONNECTIONS")?.max(1),
        Err(_) => pool_size(&args.roles)?,
    };
    info!(max_connections, "connecting to Postgres");
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        // A saturated pool surfaces as an error (logged, retried by the loops) instead of
        // hanging a role forever (QA H4).
        .acquire_timeout(std::time::Duration::from_secs(10))
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

    // One GatewayState per process: the gateway role and the simulator routes beacon-api
    // mounts must share it (outages and the SSE broadcast are in memory).
    let has = |r: Role| args.roles.contains(&r);
    let gateway_token = std::env::var("GATEWAY_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    if let Some(t) = &gateway_token {
        beacon_api::bearer_header(t)
            .context("GATEWAY_TOKEN must be a valid HTTP header value (no control characters)")?;
    }
    let gateway_state = if has(Role::Gateway) {
        // Replies go to whichever process serves /api/replies: the API role mounts delivery's
        // routes, so in an all-in-one process that is our own API port.
        let default_reply_url = if has(Role::Api) && !has(Role::DeliveryApi) {
            local_url(&env_or("API_ADDR", "127.0.0.1:8080"))?
        } else {
            "http://127.0.0.1:8071".to_string()
        };
        Some(
            beacon_delivery::gateway::GatewayState::new(pool.clone())
                .with_token(gateway_token.clone())
                .with_reply_forward(
                    Some(env_or("DELIVERY_API_URL", &default_reply_url)),
                    reply_secret(),
                )
                .with_mocks()
                .with_max_streams(
                    env_or("GATEWAY_MAX_STREAMS", "64")
                        .parse()
                        .context("GATEWAY_MAX_STREAMS")?,
                ),
        )
    } else {
        None
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
                // Demo convenience: make sure the source topics exist on our broker.
                let local: Vec<String> = specs
                    .iter()
                    .filter(|s| s.bootstrap.is_none())
                    .flat_map(|s| s.topics.clone())
                    .collect();
                beacon_ingest::kafka::ensure_named_topics(&kafka, &local, 6).await?;
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
                // Rules come from Postgres (seeded from config/rules on first
                // start) and reload live on every API edit.
                beacon_engine::config_store::seed_from_yaml(&pool, &config_dir).await?;
                let snapshot =
                    beacon_engine::config_store::watch_config(pool.clone(), config_dir.clone())
                        .await
                        .with_context(|| {
                            format!("loading rules and schemas ({})", config_dir.display())
                        })?;
                let samples = beacon_engine::matcher::SampleSink::start(pool.clone());
                for _ in 0..workers("MATCHER_WORKERS", "8")? {
                    let (kafka, snapshot, samples, rx) = (
                        kafka.clone(),
                        snapshot.clone(),
                        samples.clone(),
                        shutdown_rx.clone(),
                    );
                    tasks.spawn(supervise("matcher", rx.clone(), move || {
                        beacon_engine::matcher::run_matcher(
                            kafka.clone(),
                            snapshot.clone(),
                            Some(samples.clone()),
                            rx.clone(),
                        )
                    }));
                }
            }
            Role::Aggregator => {
                tasks.spawn({
                    let (pool, rx) = (pool.clone(), shutdown_rx.clone());
                    async move {
                        if let Err(e) = beacon_engine::aggregator::run_quiet_sweeper(pool, rx).await
                        {
                            tracing::error!(error = %e, "quiet sweeper stopped");
                        }
                    }
                });
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
            Role::Api => {
                // LLM mapping proposals: BEACON_LLM=off (default) | mock | claude.
                let llm = beacon_api::mapping_suggest::provider_from_env()?;
                if llm != beacon_api::mapping_suggest::Provider::Off {
                    info!(
                        provider = if matches!(llm, beacon_api::mapping_suggest::Provider::Mock) {
                            "mock"
                        } else {
                            "claude"
                        },
                        "mapping suggestions on (synthetic data only)"
                    );
                }
                beacon_api::mapping_suggest::configure(llm);
                // No real auth yet (demo session), so local-only unless asked.
                let addr = env_or("API_ADDR", "127.0.0.1:8080");
                // The demo user is an admin unless BEACON_DEMO_ADMIN=false.
                let admin = env_or("BEACON_DEMO_ADMIN", "true") != "false";
                let session = std::sync::Arc::new(
                    beacon_api::Session::demo_with(
                        &config_dir,
                        &env_or("BEACON_DEMO_USER", "anna"),
                        admin,
                    )
                    .context("building the demo session from config/people and config/teams")?,
                );
                let (updates, _) =
                    tokio::sync::broadcast::channel(beacon_api::feed::CHANNEL_CAPACITY);
                let (ready_tx, ready) = tokio::sync::watch::channel(false);
                let ui_dir = std::env::var("BEACON_UI_DIR")
                    .map(PathBuf::from)
                    .ok()
                    .or_else(|| {
                        Some(PathBuf::from("ui/dist")).filter(|p| p.join("index.html").exists())
                    });
                beacon_engine::config_store::seed_from_yaml(&pool, &config_dir).await?;
                let sources = std::sync::Arc::new(beacon_ingest::kafka_source::load_sources(
                    &config_dir.join("sources"),
                )?);
                let streams =
                    std::sync::Arc::new(tokio::sync::Semaphore::new(beacon_api::MAX_STREAMS));
                let state = beacon_api::ApiState {
                    pool: pool.clone(),
                    updates: updates.clone(),
                    ready,
                    session,
                    sources,
                    streams,
                };
                // Same-origin ingest for the UI's sample replay (CSRF-gated).
                let same_origin_ingest = {
                    let publisher =
                        beacon_ingest::EventPublisher::new(beacon_ingest::kafka::producer(&kafka)?);
                    let max_inflight = workers("INGEST_MAX_INFLIGHT", "20000")?;
                    beacon_ingest::http::ingest_routes(beacon_ingest::http::IngestState::new(
                        publisher,
                        max_inflight,
                    ))
                };
                // Delivery's routes on the same origin, behind the session (Actor/Admin/CSRF).
                // The simulator is mounted only when this process also runs the gateway, since
                // it must share the gateway's in-memory state.
                if gateway_state.is_none() {
                    info!("no gateway role in this process: /api/simulator/* is not mounted");
                }
                let delivery = beacon_api::DeliveryMount {
                    api: delivery_api_state(&pool, &config_dir)?,
                    simulator: gateway_state
                        .clone()
                        .map(|state| beacon_api::SimulatorMount {
                            state,
                            token: gateway_token.clone(),
                        }),
                };
                let app = beacon_api::router_full(
                    state,
                    ui_dir.clone(),
                    Some(same_origin_ingest),
                    Some(delivery),
                );
                let listener = tokio::net::TcpListener::bind(&addr)
                    .await
                    .with_context(|| format!("binding {addr}"))?;
                info!(%addr, ui = ?ui_dir, "api listening");
                {
                    let (kafka, rx) = (kafka.clone(), shutdown_rx.clone());
                    let ready_tx = std::sync::Arc::new(ready_tx);
                    tasks.spawn(supervise("api-feed", rx.clone(), move || {
                        let ready_tx = (*ready_tx).clone();
                        beacon_api::feed::run_feed(
                            kafka.clone(),
                            updates.clone(),
                            ready_tx,
                            rx.clone(),
                        )
                    }));
                }
                let mut rx = shutdown_rx.clone();
                tasks.spawn(async move {
                    let served = axum::serve(listener, app)
                        .with_graceful_shutdown(async move {
                            let _ = rx.changed().await;
                        })
                        .await;
                    if let Err(e) = served {
                        tracing::error!(error = %e, "api server failed");
                    }
                });
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
                    rules = dir.rules.by_rule.len(),
                    "router starting"
                );
                let escalate_after_override = match std::env::var("ESCALATE_AFTER_OVERRIDE_MS") {
                    Ok(v) => Some(std::time::Duration::from_millis(
                        v.parse().context("ESCALATE_AFTER_OVERRIDE_MS")?,
                    )),
                    Err(_) => None,
                };
                tasks.spawn(beacon_delivery::router::run_router(
                    pool.clone(),
                    Arc::new(dir),
                    RouterConfig {
                        escalate_after_override,
                        ..RouterConfig::default()
                    },
                    shutdown_rx.clone(),
                ));
            }
            Role::Actions => {
                let dir_path = PathBuf::from(env_or("BEACON_CONFIG_DIR", "config"));
                let dir = Directory::load(&dir_path)
                    .with_context(|| format!("loading config from {}", dir_path.display()))?;
                let registry = beacon_delivery::actions::ActionRegistry::builtin(
                    beacon_delivery::actions::ActionsConfig {
                        gateway_url: env_or("SMS_GATEWAY_URL", "http://localhost:8070"),
                        http_targets: beacon_delivery::actions::load_http_targets(&dir_path)?,
                        timeout: std::time::Duration::from_secs(5),
                    },
                );
                info!("runbook actions worker starting");
                tasks.spawn(beacon_delivery::actions::run_actions(
                    pool.clone(),
                    Arc::new(dir),
                    registry,
                    alert_ops(),
                    beacon_delivery::actions::ActionsWorkerConfig::default(),
                    shutdown_rx.clone(),
                ));
            }
            Role::DeliveryApi => {
                let state = delivery_api_state(&pool, &config_dir)?;
                let addr: std::net::SocketAddr = env_or("DELIVERY_API_ADDR", "127.0.0.1:8071")
                    .parse()
                    .context("DELIVERY_API_ADDR")?;
                let actor =
                    beacon_delivery::api::Actor(env_or("DELIVERY_API_DEMO_ACTOR", "user:bob"));
                let rx = shutdown_rx.clone();
                tasks.spawn(async move {
                    if let Err(e) = beacon_delivery::api::serve_standalone(
                        state,
                        addr,
                        actor,
                        rx,
                        env_or("DELIVERY_API_DEMO_HEADER", "0") == "1",
                    )
                    .await
                    {
                        tracing::error!(error = %e, "delivery-api stopped");
                    }
                });
            }
            Role::Scheduler => {
                let dir_path = PathBuf::from(env_or("BEACON_CONFIG_DIR", "config"));
                let dir = Directory::load(&dir_path)
                    .with_context(|| format!("loading people/teams from {}", dir_path.display()))?;
                info!("escalation scheduler starting");
                tasks.spawn(beacon_delivery::escalation::run_scheduler(
                    pool.clone(),
                    Arc::new(dir),
                    beacon_delivery::escalation::SchedulerConfig::default(),
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
                    token: std::env::var("GATEWAY_TOKEN")
                        .ok()
                        .filter(|t| !t.is_empty()),
                    ..GatewayClientConfig::default()
                };
                for name in env_or("DELIVERY_CHANNELS", "email,sms,phone")
                    .split(',')
                    .map(str::trim)
                    .filter(|c| !c.is_empty())
                {
                    let channel: Arc<dyn Channel> = match name {
                        "email" => {
                            let tls = match env_or("SMTP_TLS", "none").as_str() {
                                "none" => beacon_delivery::channel::email::SmtpTls::None,
                                "starttls" => beacon_delivery::channel::email::SmtpTls::StartTls,
                                "tls" => beacon_delivery::channel::email::SmtpTls::Tls,
                                other => {
                                    bail!("SMTP_TLS: unknown value {other:?} (none, starttls, tls)")
                                }
                            };
                            let smtp = SmtpConfig {
                                host: env_or("SMTP_HOST", "localhost"),
                                port: env_or("SMTP_PORT", "1025").parse().context("SMTP_PORT")?,
                                from: env_or("MAIL_FROM", "Beacon <beacon@example.test>"),
                                tls,
                                username: std::env::var("SMTP_USERNAME")
                                    .ok()
                                    .filter(|v| !v.is_empty()),
                                password: std::env::var("SMTP_PASSWORD")
                                    .ok()
                                    .filter(|v| !v.is_empty()),
                                allow_plaintext: env_or("SMTP_ALLOW_PLAINTEXT", "0") == "1",
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
                let addr: std::net::SocketAddr = env_or("GATEWAY_ADDR", "127.0.0.1:8070")
                    .parse()
                    .context("GATEWAY_ADDR")?;
                let state = gateway_state
                    .clone()
                    .expect("built above for the gateway role");
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

    tokio::select! {
        sig = shutdown_signal() => info!(signal = sig, "shutting down"),
        Some(res) = tasks.join_next() => match res {
            Err(e) => tracing::error!(error = %e, "role task panicked; shutting down"),
            Ok(()) => tracing::warn!("a role task exited; shutting down"),
        },
    }
    let _ = shutdown_tx.send(true);
    // Drain: roles stop claiming and finish in-flight work; anything still running after the
    // grace period is aborted (its leases expire and another replica reclaims the work).
    let drained = tokio::time::timeout(std::time::Duration::from_secs(grace), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tracing::warn!(
            grace_secs = grace,
            "shutdown grace period elapsed; aborting remaining tasks"
        );
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    Ok(())
}

/// Delivery's API state (runbooks, approvals, people/teams) for the API mount and the
/// standalone `delivery-api` role.
fn delivery_api_state(
    pool: &sqlx::PgPool,
    config_dir: &std::path::Path,
) -> Result<beacon_delivery::api::ApiState> {
    let dir = Directory::load(config_dir)
        .with_context(|| format!("loading config from {}", config_dir.display()))?;
    let registry = beacon_delivery::actions::ActionRegistry::builtin(
        beacon_delivery::actions::ActionsConfig {
            gateway_url: env_or("SMS_GATEWAY_URL", "http://localhost:8070"),
            http_targets: beacon_delivery::actions::load_http_targets(config_dir)?,
            timeout: std::time::Duration::from_secs(5),
        },
    );
    Ok(beacon_delivery::api::ApiState {
        pool: pool.clone(),
        dir: Arc::new(dir),
        registry,
        alert_ops: alert_ops(),
        reply_secret: reply_secret().map(Arc::new),
    })
}

/// `http://<host>:<port>` reaching a listener bound to `addr` from this host
/// (a wildcard bind is reached over loopback).
fn local_url(addr: &str) -> Result<String> {
    let addr: std::net::SocketAddr = addr.parse().with_context(|| format!("parsing {addr}"))?;
    let ip = if addr.ip().is_unspecified() {
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
    } else {
        addr.ip()
    };
    Ok(format!(
        "http://{}",
        std::net::SocketAddr::new(ip, addr.port())
    ))
}

/// `REPLY_SECRET` as bytes, if set.
fn reply_secret() -> Option<Vec<u8>> {
    std::env::var("REPLY_SECRET")
        .ok()
        .filter(|s| !s.is_empty())
        .map(String::into_bytes)
}

/// Alert transitions for delivery. Temporary local SQL until beacon_engine::alerts (board #185,
/// PR #12) is in this branch's base; then wrap `beacon_engine::alerts::{transition, touch}`.
fn alert_ops() -> Arc<dyn beacon_delivery::alert_ops::AlertOps> {
    Arc::new(beacon_delivery::alert_ops::SqlAlertOps)
}

/// Resolves on SIGINT (Ctrl-C) or SIGTERM (what `docker stop` / Kubernetes send, QA H6).
async fn shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => tokio::select! {
                _ = tokio::signal::ctrl_c() => "SIGINT",
                _ = term.recv() => "SIGTERM",
            },
            Err(e) => {
                tracing::warn!(error = %e, "cannot install SIGTERM handler; only Ctrl-C stops the process");
                let _ = tokio::signal::ctrl_c().await;
                "SIGINT"
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT"
    }
}

/// Default pool size from the roles in this process (QA H4). Delivery workers hold a
/// connection only around claims, alert re-reads and finalizes, but up to `concurrency` jobs per
/// worker can do that at once, so they get the largest share. Override with
/// `DB_MAX_CONNECTIONS`.
fn pool_size(roles: &[Role]) -> Result<u32> {
    let mut n: u32 = 4; // migrations, health, slack
    for role in roles {
        n += match role {
            Role::Delivery => {
                let workers: u32 = env_or("DELIVERY_WORKERS", "1")
                    .parse()
                    .context("DELIVERY_WORKERS")?;
                let channels = env_or("DELIVERY_CHANNELS", "email,sms,phone")
                    .split(',')
                    .filter(|c| !c.trim().is_empty())
                    .count() as u32;
                let per_worker = (WorkerConfig::default().concurrency as u32).div_ceil(2) + 2;
                channels * workers.max(1) * per_worker
            }
            Role::Gateway => 4,
            _ => 4,
        };
    }
    Ok(n.min(200))
}
