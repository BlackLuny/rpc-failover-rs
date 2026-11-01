use std::{path::PathBuf, time::Duration};
use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use rpc_failover_rs::{auth::token_digest, config::Config, gateway::Gateway};

#[derive(Parser)]
#[command(version, about = "Authenticated read-only blockchain RPC failover gateway")]
struct Args { #[command(subcommand)] command: Command }
#[derive(Subcommand)]
enum Command {
    Serve { #[arg(short, long)] config: PathBuf },
    CheckConfig { #[arg(short, long)] config: PathBuf },
    Probe { #[arg(short, long)] config: PathBuf, #[arg(long)] deep: bool, #[arg(long)] report: Option<PathBuf> },
    /// Generate a credential. --out creates a mode-0600 file and prints only its digest.
    Keygen { #[arg(long)] out: Option<PathBuf> },
}
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}
async fn run() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "rpc_failover_rs=info".into())).json().with_writer(std::io::stderr).init();
    match Args::parse().command {
        Command::CheckConfig { config } => {
            let config = Config::load(&config)?;
            println!("configuration valid: {} chains, {} authentication keys", config.chains.len(), config.auth.keys.len());
        },
        Command::Keygen { out } => {
            use std::io::Write;
            let mut bytes = [0u8; 32];
            getrandom::fill(&mut bytes).map_err(|_| anyhow::anyhow!("OS random generator unavailable"))?;
            let token = hex::encode(bytes);
            if let Some(path) = out {
                let mut options = std::fs::OpenOptions::new();
                options.write(true).create_new(true);
                #[cfg(unix)] {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                let mut file = options.open(path).context("cannot create credential file")?;
                writeln!(file, "{token}")?;
                file.sync_all()?;
            } else { println!("token={token}"); }
            println!("sha256={}", token_digest(token.as_bytes()));
        },
        Command::Probe { config, deep, report } => {
            let gateway = Gateway::new(Config::load(&config)?)?;
            gateway.probe_all().await;
            if deep { rpc_failover_rs::verify::deep_probe(&gateway).await; }
            let status = gateway.status();
            let output = serde_json::to_string_pretty(&status)?;
            if let Some(path) = report { std::fs::write(path, format!("{output}\n"))?; }
            println!("{output}");
            if status.chains.iter().any(|chain| chain.healthy_upstreams < 2) { bail!("verification failed: every chain needs at least two healthy upstreams"); }
        },
        Command::Serve { config } => {
            let gateway = Gateway::new(Config::load(&config)?)?;
            gateway.probe_all().await;
            let _probes = gateway.spawn_probes();
            let listener = gateway.config.listen;
            let app = gateway.router();
            tracing::info!(listen = %listener, chains = gateway.chains.len(), "RPC gateway listening");
            if let Some(tls) = &gateway.config.tls {
                let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_file, &tls.key_file).await.context("cannot load TLS certificate/key")?;
                let handle = axum_server::Handle::new();
                let shutdown = handle.clone();
                tokio::spawn(async move { shutdown_signal().await; shutdown.graceful_shutdown(Some(Duration::from_secs(5))); });
                axum_server::bind_rustls(listener, tls).handle(handle).serve(app.into_make_service()).await?;
            } else {
                let socket = tokio::net::TcpListener::bind(listener).await?;
                axum::serve(socket, app).with_graceful_shutdown(shutdown_signal()).await?;
            }
        },
    }
    Ok(())
}
async fn shutdown_signal() {
    #[cfg(unix)] {
        if let Ok(mut signal) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            tokio::select! { _ = signal.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
