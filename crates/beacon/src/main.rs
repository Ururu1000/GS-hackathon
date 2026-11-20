//! `beacon --role router,delivery [--migrate]`
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
//! | `DELIVERY_WORKERS`   | `1` (worker loops per process)            | delivery          |

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use beacon_delivery::channel::Channel;
use beacon_delivery::channel::email::{EmailChannel, SmtpConfig};
use beacon_delivery::config::Directory;
use beacon_delivery::{RouterConfig, WorkerConfig};
use sqlx::postgres::PgPoolOptions;
use tracing::info;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    /// outbox 'notify' rows -> delivery_jobs (part b)
    Router,
    /// channel workers: delivery_jobs -> email (part b)
    Delivery,
}

impl Role {
    fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "router" => Self::Router,
            "delivery" => Self::Delivery,
            other => bail!("unknown role {other:?} (known: router, delivery)"),
        })
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
                println!(
                    "usage: beacon [--migrate] --role <role>[,<role>...]\nroles: router, delivery"
                );
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
        bail!("no roles given; use --role router,delivery");
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

    for role in &args.roles {
        match role {
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
                let smtp = SmtpConfig {
                    host: env_or("SMTP_HOST", "localhost"),
                    port: env_or("SMTP_PORT", "1025").parse().context("SMTP_PORT")?,
                    from: env_or("MAIL_FROM", "Beacon <beacon@example.test>"),
                    ..SmtpConfig::default()
                };
                info!(host = %smtp.host, port = smtp.port, "email worker starting");
                let email: Arc<dyn Channel> =
                    Arc::new(EmailChannel::new(smtp).map_err(anyhow::Error::msg)?);
                let workers: usize = env_or("DELIVERY_WORKERS", "1")
                    .parse()
                    .context("DELIVERY_WORKERS")?;
                for _ in 0..workers.max(1) {
                    tasks.spawn(beacon_delivery::worker::run_worker(
                        pool.clone(),
                        email.clone(),
                        WorkerConfig::default(),
                        shutdown_rx.clone(),
                    ));
                }
            }
        }
    }

    if tasks.is_empty() {
        return Ok(());
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("shutting down"),
        Some(res) = tasks.join_next() => {
            if let Err(e) = res { tracing::error!(error = %e, "role task panicked"); }
        }
    }
    let _ = shutdown_tx.send(true);
    while tasks.join_next().await.is_some() {}
    Ok(())
}
