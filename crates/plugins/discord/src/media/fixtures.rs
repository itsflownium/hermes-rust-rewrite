use crate::{GatewayConfig, RunningGateway, messaging::*};
use hermes_engine::{CancellationToken, Config};
use std::{path::Path, sync::Arc, time::Duration};
use tempfile::TempDir;

#[derive(Clone)]
struct Fixture(async_channel::Sender<Ingress>);

pub(super) struct Speech(pub(super) async_channel::Sender<i16>);

impl AudioService for Speech {
    fn transcribe<'a>(
        &'a self,
        _: &'a Path,
        wav: &'a [u8],
    ) -> AdapterFuture<'a, Result<String, GatewayError>> {
        Box::pin(async move {
            let sample = i16::from_le_bytes(wav[44..46].try_into().unwrap());
            self.0.send(sample).await.unwrap();
            Ok(format!("spoken {sample}"))
        })
    }

    fn synthesize<'a>(
        &'a self,
        _: &'a Path,
        _: &'a str,
    ) -> AdapterFuture<'a, Result<Vec<u8>, GatewayError>> {
        Box::pin(async { Ok(super::pcm_wave(&[1; 320])) })
    }
}

impl AdapterFactory for Fixture {
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            platform: "discord-fixture".into(),
            version: CONTRACT_VERSION,
            capabilities: Capabilities {
                attachments: true,
                threads: true,
                ..Capabilities::TEXT
            },
        }
    }

    fn connect(
        &self,
        _: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        Box::pin(async {
            Ok(VerifiedAdapter {
                account: "900".into(),
                adapter: Arc::new(self.clone()),
            })
        })
    }
}

impl Adapter for Fixture {
    fn listen(
        &self,
        ingress: Ingress,
        stop: CancellationToken,
    ) -> AdapterFuture<'_, ConnectionFailure> {
        Box::pin(async move {
            self.0.send(ingress).await.unwrap();
            stop.cancelled().await;
            ConnectionFailure::Stopped
        })
    }

    fn prepare(&self, _: &DeliveryIntent) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        Err(PreparationFailure::Unsupported)
    }

    fn send(
        &self,
        _: SendAttempt,
        _: Ingress,
        _: CancellationToken,
    ) -> AdapterFuture<'_, DeliveryResult> {
        Box::pin(async { DeliveryResult::Rejected })
    }
}

type ProfileFixture = (
    TempDir,
    RunningGateway,
    Ingress,
    async_channel::Receiver<i16>,
);

pub(crate) async fn profile() -> ProfileFixture {
    profile_with_model("http://127.0.0.1:9".into(), false).await
}

pub(super) async fn profile_with_model(base_url: String, tools: bool) -> ProfileFixture {
    let profile = TempDir::new().unwrap();
    let (ready, connected) = async_channel::bounded(4);
    let (audio, spoken) = async_channel::bounded(8);
    let mut config = GatewayConfig::new(
        profile.path(),
        Config {
            base_url,
            model: "fixture".into(),
            api_key: String::new(),
        },
    );
    config.messaging_audio = Some(Arc::new(Speech(audio)));
    config.adapters.push(AdapterConfig {
        name: "discord-fixture".into(),
        factory: Arc::new(Fixture(ready)),
        policy: AccessPolicy {
            users: ["10".into(), "20".into()].into_iter().collect(),
            rooms: [(
                "42".into(),
                RoomPolicy {
                    tools,
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            scope: ScopePolicy {
                rooms: [(
                    "42".into(),
                    RoomRules {
                        turn: TurnPolicy {
                            approval: Some(ApprovalPolicy::Off),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            },
            ..Default::default()
        },
    });
    let gateway = crate::start_test_gateway(config).await.unwrap();
    let ingress = tokio::time::timeout(Duration::from_secs(5), connected.recv())
        .await
        .unwrap()
        .unwrap();
    (profile, gateway, ingress, spoken)
}

pub(crate) fn room() -> Room {
    Room {
        kind: RoomKind::Group,
        chat: "42".into(),
        thread: None,
    }
}

pub(crate) fn actor(id: &str) -> Sender {
    Sender {
        id: id.into(),
        roles: Default::default(),
    }
}
