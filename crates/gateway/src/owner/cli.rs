mod telegram;

use super::{discover, serve_control, transport};
use crate::{Gateway, GatewayConfig};
use anyhow::{Context, Result, bail, ensure};
use hermes_engine::{Config, Session};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    net::IpAddr,
    path::{Path, PathBuf},
};

/// Handle shared command-line roles before starting the desktop or gateway.
pub fn dispatch_role() -> Option<i32> {
    #[cfg(feature = "reliquary")]
    if std::env::args_os().nth(1).is_some_and(|arg| arg == "--licenses") {
        println!("{}", include_str!("../../../../RELIQUARY-NOTICE"));
        return Some(hermes_engine::herdr::dispatch_role().unwrap_or(0));
    }
    hermes_engine::herdr::dispatch_role()
}

pub async fn serve(
    args: impl IntoIterator<Item = String>,
    stock_adapters: Vec<crate::messaging::StockAdapter>,
) -> Result<()> {
    let mut port = 8765;
    let mut host: IpAddr = "127.0.0.1".parse()?;
    let mut root = None;
    let mut ephemeral = false;
    let mut token_file = None;
    let mut args = args.into_iter().peekable();
    if args.peek().is_some_and(|argument| argument == "telegram") {
        args.next();
        return telegram::run(args).await;
    }
    if args.peek().is_some_and(|argument| argument == "run") { args.next(); }
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--ephemeral-token" => ephemeral = true,
            "--port" => {
                port = args
                    .next()
                    .context("--port requires a port number")?
                    .parse()
                    .context("Invalid port")?
            }
            "--host" => {
                host = args
                    .next()
                    .context("--host requires a loopback IP address")?
                    .parse()
                    .context("Invalid IP address")?
            }
            "--data-dir" => {
                root = Some(PathBuf::from(
                    args.next().context("--data-dir requires a path")?,
                ))
            }
            "--token-file" => {
                token_file = Some(PathBuf::from(
                    args.next().context("--token-file requires a path")?,
                ))
            }
            "--help" | "-h" => {
                println!(
                    "Hermes Rust loopback gateway\n\nUsage: hermes-gateway [--host 127.0.0.1] [--port 8765] [--data-dir PATH] [--token-file PATH]\n\nProvider: HERMES_RUST_BASE_URL, HERMES_RUST_MODEL, HERMES_RUST_API_KEY\nAuthentication: HERMES_RUST_GATEWAY_TOKEN or a private generated token file\nSupports a documented subset of Hermes' legacy and hermes-gateway-v1 WebSocket protocols.\nUse rpc.discover for the supported method list.\nUse hermes-gateway gateway telegram --help for headless Telegram. Ctrl-C or SIGTERM shuts down and saves active turns."
                );
                return Ok(());
            }
            _ => bail!("Unknown option: {arg}. Use --help"),
        }
    }
    ensure!(
        host.is_loopback(),
        "Only loopback gateway listeners are supported"
    );
    let root = match root {
        Some(root) => root,
        None => Session::default_dir()?,
    };
    fs::create_dir_all(&root).context("Could not create the data directory")?;
    let root = fs::canonicalize(&root)?;
    let Some(lock) = super::try_profile_lock(&root)? else {
        return Ok(());
    };
    if discover(&root).await?.is_some() {
        return Ok(());
    }
    let mut config = GatewayConfig::new(&root, Config::from_env());
    config.stock_adapters = stock_adapters;
    config.bind = (host, port).into();
    let token_path = if ephemeral {
        None
    } else if let Ok(token) = std::env::var("HERMES_RUST_GATEWAY_TOKEN") {
        ensure!(
            !token.is_empty(),
            "HERMES_RUST_GATEWAY_TOKEN must not be empty"
        );
        config.token = token;
        None
    } else {
        let file = token_file.unwrap_or_else(|| root.join("gateway.token"));
        config.token = read_or_create_token(&file)?;
        Some(file)
    };
    let control = transport::Listener::bind(&root)?;
    let profile = super::profile_id(&root)?;
    let name = hermes_library::profiles::name_for_home(&root);
    let gateway = Gateway::start_profile(config, profile, name.into(), lock).await?;
    println!(
        "{}",
        serde_json::json!({"implementation":"hermes-rust","http_url":gateway.http_url(),"websocket_url":gateway.websocket_url(),"token_file":token_path,"protocol":"hermes-gateway-v1","scope":"documented subset"})
    );

    let ctrl = tokio_util::sync::CancellationToken::new();
    let shared = gateway.shared.clone();
    let service = serve_control(control, &root, shared, ctrl.clone());
    let cause = tokio::select! {
        result = service => result,
        _ = ctrl.cancelled() => Ok(()),
        result = shutdown_signal() => result,
    };
    gateway.shutdown().await?;
    cause
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { signal = tokio::signal::ctrl_c() => signal?, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

fn read_or_create_token(path: &Path) -> Result<String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "Token file must be a regular file"
            );
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                ensure!(
                    metadata.permissions().mode() & 0o077 == 0,
                    "Token file permissions must be private (chmod 600 {})",
                    path.display()
                );
            }
            let token = fs::read_to_string(path).context("Could not read gateway token")?;
            ensure!(
                !token.trim().is_empty(),
                "Gateway token file must not be empty"
            );
            Ok(token.trim().into())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                fs::create_dir_all(parent)?;
            }
            let token = format!(
                "{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            );
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options
                .open(path)
                .context("Could not create private gateway token file")?;
            writeln!(file, "{token}")?;
            file.sync_all()?;
            Ok(token)
        }
        Err(error) => Err(error).context("Could not inspect token file"),
    }
}
