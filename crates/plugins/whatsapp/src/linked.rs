mod ipc;
mod protocol;

use crate::{
    common::{self, jid, lock},
    intake, outbound,
};
use anyhow::{Context, Result, ensure};
use hermes_gateway_core::messaging::{
    AccessPolicy, Adapter, AdapterFactory, AdapterFuture, CONTRACT_VERSION, Capabilities,
    ConnectionFailure, DeliveryIntent, DeliveryPart, DeliveryResult, Descriptor,
    EnrollmentCredential, EnrollmentLease, EnrollmentPrompt, GatewayError, Ingress,
    PreparationFailure, SendAttempt, StockAdapter, StockConfiguration, VerifiedAdapter,
    VerifiedInstallation,
    runtime::{blocking, db, now},
    storage,
};
use ipc::{Bridge, Setup};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_whatsapp_linked_handoffs (
    connection TEXT NOT NULL, envelope TEXT NOT NULL, digest TEXT NOT NULL,
    body TEXT, complete INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(connection,envelope)
);
CREATE TABLE IF NOT EXISTS messaging_whatsapp_linked_effects (
    connection TEXT NOT NULL, room TEXT NOT NULL, native TEXT NOT NULL, operation TEXT NOT NULL,
    delivery TEXT NOT NULL, text TEXT NOT NULL, issued INTEGER NOT NULL, state TEXT,
    PRIMARY KEY(connection,room,native)
);
CREATE TABLE IF NOT EXISTS messaging_whatsapp_handover (
    connection TEXT NOT NULL, room TEXT NOT NULL, until_ms INTEGER NOT NULL,
    PRIMARY KEY(connection,room)
);
CREATE TABLE IF NOT EXISTS messaging_whatsapp_linked_resume (
    connection TEXT PRIMARY KEY, clean INTEGER NOT NULL
);
";

pub const LINKED_ADAPTER: StockAdapter = StockAdapter {
    platform: "whatsapp",
    schema: &[common::TABLES, intake::TABLES, outbound::TABLES, TABLES],
    configure,
    replace_credential,
};

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Mode {
    #[default]
    Bot,
    Personal,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    account: String,
    #[serde(default)]
    mode: Mode,
    #[serde(default)]
    access: AccessPolicy,
    #[serde(default)]
    mention_patterns: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    progress: bool,
    #[serde(default)]
    read_signals: bool,
    #[serde(default)]
    typing: bool,
    #[serde(default)]
    handover_ms: u32,
    #[serde(default)]
    burst_ms: u16,
    #[serde(default)]
    home: Option<String>,
    #[cfg(test)]
    #[serde(default)]
    fixture_script: Option<PathBuf>,
}

impl Settings {
    fn validate(&self) -> Result<()> {
        ensure!(
            jid(&self.account) && !self.account.ends_with("@g.us"),
            "A verified WhatsApp account JID is required"
        );
        ensure!(
            self.burst_ms <= 1200 && self.handover_ms <= 86_400_000,
            "Invalid WhatsApp timing configuration"
        );
        ensure!(
            self.home.as_deref().is_none_or(jid),
            "Invalid WhatsApp home destination"
        );
        ensure!(
            self.mention_patterns
                .iter()
                .all(|(room, patterns)| jid(room)
                    && room.ends_with("@g.us")
                    && patterns.len() <= 12
                    && patterns.iter().all(
                        |pattern| (1..=80).contains(&pattern.len()) && !pattern.contains('\0')
                    )),
            "Invalid WhatsApp mention pattern"
        );
        Ok(())
    }

    fn policy(&self) -> AccessPolicy {
        let mut policy = common::strict_policy(self.access.clone());
        if self.mode == Mode::Personal {
            policy.users.insert(self.account.clone());
        }
        let groups: Vec<_> = policy
            .rooms
            .keys()
            .chain(policy.scope.rooms.keys())
            .filter(|room| room.ends_with("@g.us"))
            .cloned()
            .collect();
        for group in groups {
            policy
                .scope
                .rooms
                .entry(group)
                .or_default()
                .mention_required
                .get_or_insert(true);
        }
        policy
    }

    fn bridge_source(&self) -> Result<PathBuf, ConnectionFailure> {
        #[cfg(test)]
        if let Some(path) = &self.fixture_script {
            return Ok(path.clone());
        }
        let executable = std::env::current_exe().map_err(|_| ConnectionFailure::Configuration)?;
        let dir = executable
            .parent()
            .ok_or(ConnectionFailure::Configuration)?;
        let candidates = [
            dir.join("whatsapp-bridge"),
            dir.join("../share/hermes/whatsapp-bridge"),
            dir.join("../Resources/whatsapp-bridge"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("bridge"),
        ];
        for package in candidates {
            let file = package.join("bridge.mjs");
            if std::fs::read(&file).as_deref().ok() != Some(include_bytes!("../bridge/bridge.mjs"))
                || std::fs::read(package.join("package-lock.json"))
                    .as_deref()
                    .ok()
                    != Some(include_bytes!("../bridge/package-lock.json"))
            {
                continue;
            }
            let manifest = package.join("node_modules/@whiskeysockets/baileys/package.json");
            let version = std::fs::read(manifest)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
            if version.is_some_and(|manifest| manifest["version"] == "7.0.0-rc13") {
                return Ok(file);
            }
        }
        Err(ConnectionFailure::Configuration)
    }
}

fn configure(root: &Path, settings: Value) -> Result<StockConfiguration> {
    let settings: Settings = serde_json::from_value(settings)?;
    settings.validate()?;
    Ok(StockConfiguration {
        settings: serde_json::to_value(&settings)?,
        policy: settings.policy(),
        factory: Arc::new(Factory {
            root: root.into(),
            settings,
            status: Arc::new(Mutex::new(json!({"bridge":"setup_required"}))),
            pending: tokio::sync::Mutex::new(None),
        }),
    })
}

fn replace_credential(old: &Value, new: &Value) -> bool {
    old["account"] != new["account"]
}

struct Pending {
    id: String,
    bridge: Arc<Bridge>,
}

struct Factory {
    root: PathBuf,
    settings: Settings,
    status: Arc<Mutex<Value>>,
    pending: tokio::sync::Mutex<Option<Pending>>,
}

struct Linked {
    settings: Settings,
    status: Arc<Mutex<Value>>,
    bridge: Option<Arc<Bridge>>,
    auth_file: PathBuf,
    device: String,
}

impl Factory {
    fn launch(&self) -> Result<Arc<Bridge>, ConnectionFailure> {
        let script = self.settings.bridge_source().map_err(|error| {
            lock(&self.status)["bridge"] = json!("bridge_assets_unavailable");
            error
        })?;
        Bridge::launch(&script).map(Arc::new).map_err(|error| {
            lock(&self.status)["bridge"] = json!("node_runtime_unavailable");
            error
        })
    }

    fn auth_file(&self, lease: &str) -> Result<PathBuf, ConnectionFailure> {
        if !common::text_id(lease) || lease.len() < 16 {
            return Err(ConnectionFailure::Credentials);
        }
        let pinned = storage::key("linked-device", &(&self.settings.account, lease))
            .map_err(|_| ConnectionFailure::Retryable)?;
        Ok(self
            .root
            .join("messaging")
            .join("whatsapp")
            .join(format!("{pinned}.sealed")))
    }

    fn verified(&self, state: &Setup) -> Result<String, ConnectionFailure> {
        if state.account.as_deref() != Some(&self.settings.account) {
            lock(&self.status)["bridge"] = json!("account_mismatch");
            return Err(ConnectionFailure::Credentials);
        }
        state
            .device
            .as_deref()
            .filter(|id| common::text_id(id))
            .map(str::to_owned)
            .ok_or(ConnectionFailure::Configuration)
    }

    fn adapter(
        &self,
        device: String,
        auth_file: PathBuf,
        bridge: Option<Arc<Bridge>>,
    ) -> VerifiedAdapter {
        VerifiedAdapter {
            account: self.settings.account.clone(),
            adapter: Arc::new(Linked {
                settings: self.settings.clone(),
                status: self.status.clone(),
                bridge,
                auth_file,
                device,
            }),
        }
    }
}

impl AdapterFactory for Factory {
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            platform: "whatsapp".into(),
            version: CONTRACT_VERSION,
            capabilities: Capabilities {
                attachments: true,
                interactive_prompts: true,
                progress: self.settings.progress,
                ..Capabilities::TEXT
            },
        }
    }

    fn verification_status(&self) -> Option<Value> {
        Some(lock(&self.status).clone())
    }

    fn connect(
        &self,
        _: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        Box::pin(async { Err(ConnectionFailure::Credentials) })
    }

    fn enrollment_prompt<'a>(
        &'a self,
        lease: &'a EnrollmentLease,
        cancel: CancellationToken,
    ) -> AdapterFuture<'a, Result<Option<EnrollmentPrompt>, ConnectionFailure>> {
        Box::pin(async move {
            let bridge = self.launch()?;
            let old = self.pending.lock().await.replace(Pending {
                id: lease.id.clone(),
                bridge: bridge.clone(),
            });
            if let Some(old) = old {
                old.bridge.stop().await;
            }
            bridge.start(None, &self.settings.account, self.settings.burst_ms)?;
            lock(&self.status)["bridge"] = json!("pairing");
            let deadline = lease.expires_at.saturating_sub(now()).max(0) as u64;
            let expires = bridge.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(deadline)).await;
                expires.stop().await;
            });
            let state = bridge
                .wait(
                    |state| state.pairing.is_some() || state.browser.is_some(),
                    &cancel,
                )
                .await?;
            if let Some(url) = state.browser {
                lock(&self.status)["bridge"] = json!("passkey_required");
                let provider = reqwest::Url::parse(&url).is_ok_and(|url| {
                    url.scheme() == "https"
                        && matches!(
                            url.host_str(),
                            Some("web.whatsapp.com" | "www.whatsapp.com")
                        )
                        && url.username().is_empty()
                        && url.password().is_none()
                });
                if !provider {
                    return Err(ConnectionFailure::Configuration);
                }
                return Ok(Some(EnrollmentPrompt::Url { url }));
            }
            Ok(state
                .pairing
                .map(|payload| EnrollmentPrompt::Qr { payload }))
        })
    }

    fn connect_enrolled(
        &self,
        credential: EnrollmentCredential,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        Box::pin(async move {
            let auth_file = self.auth_file(credential.secret())?;
            let pending = self
                .pending
                .lock()
                .await
                .as_ref()
                .filter(|pending| pending.id == credential.secret())
                .map(|pending| pending.bridge.clone());
            if let Some(bridge) = pending {
                let state = bridge
                    .wait(
                        |state| state.account.is_some() && state.auth.is_some(),
                        &cancel,
                    )
                    .await?;
                let device = self.verified(&state)?;
                let mut pending = self.pending.lock().await;
                if !pending.as_ref().is_some_and(|pending| {
                    pending.id == credential.secret() && Arc::ptr_eq(&pending.bridge, &bridge)
                }) {
                    return Err(ConnectionFailure::Credentials);
                }
                let auth = state.auth.ok_or(ConnectionFailure::Credentials)?;
                save_auth(&self.root, &auth_file, &auth)
                    .map_err(|_| ConnectionFailure::Retryable)?;
                bridge.stop().await;
                *pending = None;
                return Ok(self.adapter(device, auth_file, None));
            }
            let revoked = revoked_file(&auth_file);
            if revoked.exists() {
                return Err(ConnectionFailure::Credentials);
            }
            let encrypted =
                std::fs::read_to_string(&auth_file).map_err(|_| ConnectionFailure::Credentials)?;
            let authentication = hermes_engine::secret::unseal(&encrypted)
                .map_err(|_| ConnectionFailure::Credentials)?;
            let auth: Value = serde_json::from_str(&authentication)
                .map_err(|_| ConnectionFailure::Credentials)?;
            let bridge = self.launch()?;
            bridge.start(Some(auth), &self.settings.account, self.settings.burst_ms)?;
            lock(&self.status)["bridge"] = json!("reconnecting");
            let clock = tokio::time::timeout(
                Duration::from_secs(25),
                bridge.wait(|state| state.account.is_some(), &cancel),
            )
            .await
            .map_err(|_| ConnectionFailure::Retryable)?;
            let state = match clock {
                Ok(state) => state,
                Err(ConnectionFailure::Credentials) => {
                    record_revocation(&auth_file).map_err(|_| ConnectionFailure::Retryable)?;
                    lock(&self.status)["bridge"] = json!("revoked");
                    return Err(ConnectionFailure::Credentials);
                }
                Err(error) => return Err(error),
            };
            let device = self.verified(&state)?;
            Ok(self.adapter(device, auth_file, Some(bridge)))
        })
    }
}

fn save_auth(root: &Path, path: &Path, auth: &Value) -> Result<()> {
    let us = serde_json::to_string(auth)?;
    ensure!(
        us.len() <= 6 * 1024 * 1024,
        "WhatsApp credential state exceeds the bridge limit"
    );
    let secret = hermes_engine::secret::seal(root, &us)?;
    let directory = path.parent().context("WhatsApp credential directory")?;
    std::fs::create_dir_all(directory)?;
    let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
    temporary.write_all(secret.as_bytes())?;
    temporary.as_file().sync_all()?;
    temporary.persist(path)?;
    Ok(())
}

fn revoked_file(path: &Path) -> PathBuf {
    path.with_extension("revoked")
}

fn record_revocation(path: &Path) -> Result<()> {
    std::fs::write(revoked_file(path), b"Revoked")?;
    Ok(())
}

impl Adapter for Linked {
    fn initial_transport_ready(&self) -> bool {
        false
    }
    fn verified_installation(&self) -> Option<VerifiedInstallation> {
        Some(VerifiedInstallation {
            installation: self.settings.account.clone(),
            tenant: None,
            application: None,
            receiver: None,
            origin: None,
            device: Some(self.device.clone()),
            local_client: None,
        })
    }

    fn listen(
        &self,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, ConnectionFailure> {
        Box::pin(async move {
            let Some(bridge) = &self.bridge else {
                return ConnectionFailure::Configuration;
            };
            if let Some(auth) = bridge.state().auth
                && persist_active(&ingress, &self.auth_file, auth)
                    .await
                    .is_err()
            {
                return ConnectionFailure::Retryable;
            }
            match ingress.checked(true, |_| {
                db(ingress.storage().write(|tx| {
                    let old = tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM messaging_whatsapp_linked_resume
                    WHERE connection=?1 AND clean=0)",
                        [ingress.connection_name()],
                        |row| row.get::<_, bool>(0),
                    )?;
                    tx.execute(
                        "INSERT INTO messaging_whatsapp_linked_resume VALUES(?1,0)
                            ON CONFLICT(connection) DO UPDATE SET clean=0",
                        [ingress.connection_name()],
                    )?;
                    Ok(old)
                }))
            }) {
                Ok(true) => {
                    let _ = ingress.report_gap();
                }
                Ok(false) => {}
                Err(_) => return ConnectionFailure::Retryable,
            }
            if ingress.report_transport_ready(true).is_err() {
                return ConnectionFailure::Stopped;
            }
            if bridge.notify("activate", json!({})).is_err() {
                return ConnectionFailure::Retryable;
            }
            *lock(&self.status) =
                json!({"bridge":"ready","account":self.settings.account,"mode":self.settings.mode});
            let media = protocol::Reader {
                bridge,
                settings: &self.settings,
            };
            let result = tokio::select! {
                _ = cancel.cancelled() => ConnectionFailure::Stopped,
                _ = intake::resume(&ingress,&media,&cancel) => ConnectionFailure::Stopped,
                result = protocol::events(self,bridge,&ingress,&cancel) => result,
            };
            if result == ConnectionFailure::Stopped {
                let _ = ingress.checked(false, |_| db(ingress.storage().write(|tx| {
                    tx.execute("UPDATE messaging_whatsapp_linked_resume SET clean=1 WHERE connection=?1",[ingress.connection_name()])?;
                    Ok(())
                })));
            } else {
                let _ = ingress.report_gap();
            }
            bridge.stop().await;
            result
        })
    }

    fn prepare(&self, intent: &DeliveryIntent) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        outbound::plan(intent, None, false, false)
    }

    fn prepare_with_ingress(
        &self,
        intent: &DeliveryIntent,
        ingress: &Ingress,
    ) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        outbound::plan(intent, Some(ingress), false, true)
    }

    fn send(
        &self,
        attempt: SendAttempt,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, DeliveryResult> {
        Box::pin(async move {
            let Some(bridge) = &self.bridge else {
                return DeliveryResult::Rejected;
            };
            protocol::send(&self.settings, bridge, &ingress, &attempt, &cancel).await
        })
    }
}

async fn persist_active(ingress: &Ingress, path: &Path, auth: Value) -> Result<(), GatewayError> {
    let (scope, path) = (ingress.clone(), path.to_owned());
    blocking(move || scope.checked(true, |_| db(save_auth(scope.root(), &path, &auth)))).await
}
