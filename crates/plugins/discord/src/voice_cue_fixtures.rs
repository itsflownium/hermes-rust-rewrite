use super::*;
use crate::{
    DiscordAdapter,
    client::DiscordClient,
    settings::{SessionStartLimit, VerifiedBot},
    voice::FixtureCall,
};
use crate::messaging::{
    Adapter, AdapterFactory, AdapterFuture, AudioService, ConnectionFailure, DeliveryIntent,
    DeliveryPart, DeliveryResult, Descriptor, FirstToolFeedback, GatewayError, InboundEvent,
    Ingress, PreparationFailure, SendAttempt, VerifiedAdapter, VerifiedInstallation,
};
use tokio_util::sync::CancellationToken;

struct VoiceFixtureAdapter {
    discord: DiscordAdapter,
    connected: async_channel::Sender<Ingress>,
}

struct Factory {
    stock: DiscordFactory,
    voice: Arc<VoiceFixtureAdapter>,
}

impl AdapterFactory for Factory {
    fn descriptor(&self) -> Descriptor {
        self.stock.descriptor()
    }

    fn connect(
        &self,
        _: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        Box::pin(async {
            Ok(VerifiedAdapter {
                account: "9".into(),
                adapter: self.voice.clone(),
            })
        })
    }
}

impl Adapter for VoiceFixtureAdapter {
    fn initial_transport_ready(&self) -> bool {
        self.discord.initial_transport_ready()
    }

    fn verified_installation(&self) -> Option<VerifiedInstallation> {
        self.discord.verified_installation()
    }

    fn listen(
        &self,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, ConnectionFailure> {
        Box::pin(async move {
            let _ = self.connected.try_send(ingress.clone());
            self.discord.listen(ingress, cancel).await
        })
    }

    fn first_tool_feedback(
        &self,
        ingress: Ingress,
        event: &InboundEvent,
    ) -> Option<FirstToolFeedback> {
        self.discord.first_tool_feedback(ingress, event)
    }

    fn prepare(&self, intent: &DeliveryIntent) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        self.discord.prepare(intent)
    }

    fn prepare_with_ingress(
        &self,
        intent: &DeliveryIntent,
        ingress: &Ingress,
    ) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        self.discord.prepare_with_ingress(intent, ingress)
    }

    fn send(
        &self,
        attempt: SendAttempt,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, DeliveryResult> {
        self.discord.send(attempt, ingress, cancel)
    }
}

struct Speech {
    called: mpsc::Sender<String>,
    heard: AtomicU64,
    fail: bool,
}

impl AudioService for Speech {
    fn transcribe<'a>(
        &'a self,
        _: &'a Path,
        _: &'a [u8],
    ) -> AdapterFuture<'a, Result<String, GatewayError>> {
        Box::pin(async move {
            self.heard.fetch_add(1, Ordering::Relaxed);
            Ok("approval".into())
        })
    }

    fn synthesize<'a>(
        &'a self,
        _: &'a Path,
        text: &'a str,
    ) -> AdapterFuture<'a, Result<Vec<u8>, GatewayError>> {
        Box::pin(async move {
            self.called.send(text.to_owned()).await.unwrap();
            if self.fail {
                return Err(GatewayError::Unsupported);
            }
            let samples = vec![1000; 48_000];
            Ok(crate::media::pcm_wave(&samples))
        })
    }
}

async fn voice_call(
    fixture: &mut Fixtures,
    root: &Path,
    model: &Model,
    enabled: bool,
    audio: Arc<Speech>,
) -> (RunningGateway, Wire, Ingress, FixtureCall) {
    let settings: DiscordSettings = serde_json::from_value(json!({
        "grants":{"users":["7"]},"operators":["7"],
        "voice_enabled":true,
        "voice_fx":{"enabled":enabled,"ambient_enabled":false,"ack_phrases":["Checking now."]},
        "commands":{"sync":false},
        "guilds":{"11":{"rules":{"turn":{"tools":true,"approval":"manual"}},
            "channels":{"21":{"rules":{"mention_required":false},"threads":{"22":{}}}}}}
    }))
    .unwrap();
    let verified = VerifiedBot {
        bot_id: "9".into(),
        application_id: "90".into(),
        gateway: Url::parse(&fixture.transport.gateway).unwrap(),
        session_start_limit: SessionStartLimit {
            total: 100,
            remaining: 100,
            reset_after: 1000,
            max_concurrency: 1,
        },
        shards: 1,
    };
    let client = DiscordClient::for_fixture(
        EnrollmentCredential::new("fixture-bot-secret".into()).unwrap(),
        fixture.api.clone(),
    )
    .unwrap();
    let (connected, ready) = async_channel::bounded(1);
    let voice = Arc::new(VoiceFixtureAdapter {
        discord: DiscordAdapter::new(settings.clone(), client, verified),
        connected,
    });
    let gateway = model
        .gateway_with_audio(
            root,
            vec![AdapterConfig {
                name: "fixture-cues".into(),
                policy: settings.host_policy(),
                factory: Arc::new(Factory {
                    stock: DiscordFactory::for_fixture(
                        root.to_owned(),
                        settings,
                        fixture.api.clone(),
                    )
                    .unwrap(),
                    voice: voice.clone(),
                }),
            }],
            Some(audio),
        )
        .await;
    let ingress = timeout(LIMIT, ready.recv()).await.unwrap().unwrap();
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "voice-cues").await;
    wire.dispatch(
        2,
        "GUILD_CREATE",
        json!({
            "id":"11","channels":[{"id":"21","type":0},{"id":"24","type":2}],
            "threads":[{"id":"22","type":11,"parent_id":"21"}],
            "voice_states":[{"user_id":"7","channel_id":"24"}],
        }),
    )
    .await;
    wire.dispatch(3, "INTERACTION_CREATE", command(3, "voice", Some("join")))
        .await;
    assert_eq!(wire.authentication(4).await["d"]["channel_id"], "24");
    wire.dispatch(
        4,
        "VOICE_STATE_UPDATE",
        json!({
            "guild_id":"11","user_id":"9","channel_id":"24","session_id":"fixture-voice-session",
        }),
    )
    .await;
    let call = timeout(LIMIT, async {
        loop {
            if let Some(call) = FixtureCall::attach(&voice.discord.voice, &ingress, 11) {
                break call;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("local voice driver fixture");
    (gateway, wire, ingress, call)
}

fn command(sequence: u64, name: &str, action: Option<&str>) -> Value {
    let id = (((now() - 1_420_070_400_000) as u64) << 22) + sequence;
    json!({
        "id":id.to_string(),"application_id":"90","token":format!("fixture-cue-{sequence}"),"type":2,
        "channel_id":"21","guild_id":"11","member":{"roles":[],"user":{"id":"7"}},
        "data":{"name":name,"options":action.map(|action| vec![json!({"name":"action","value":action})]).unwrap_or_default()},
    })
}

async fn tool_prompt(
    fixture: &mut Fixtures,
    gateway: &RunningGateway,
    model: &Model,
    target: &str,
) {
    if timeout(Duration::from_secs(8), async {
        while let Some(write) = fixture.writes.recv().await {
            if matches!(write, Write::Message { channel, payload, .. }
                if channel == target && payload["components"].is_array())
            {
                return;
            }
        }
        panic!("Discord fixture closed");
    })
    .await
    .is_err()
    {
        panic!(
            "Missing native tool approval in {target}. Model requests: {}. Messaging: {}",
            model.requests().len(),
            gateway.messaging_status().await.unwrap()
        );
    }
}

#[tokio::test]
async fn first_native_tool_for_live_voice_acks_once_and_stops_with_the_turn() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let (calls, mut spoken) = mpsc::channel(8);
    let audio = Arc::new(Speech {
        called: calls,
        heard: AtomicU64::new(0),
        fail: false,
    });
    let (gateway, wire, ingress, call) =
        voice_call(&mut fixture, root.path(), &model, true, audio.clone()).await;

    call.utterance(7, 1).await;
    tool_prompt(&mut fixture, &gateway, &model, "21").await;
    assert_eq!(
        timeout(LIMIT, spoken.recv()).await.unwrap().unwrap(),
        "Checking now."
    );
    timeout(LIMIT, async {
        while !call.speaking() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cue submitted to Songbird");
    assert_eq!(model.requests().len(), 1);

    let prior: InboundEvent = ingress.storage().read(|db| Ok(db.query_row(
        "SELECT input FROM messaging_ingress WHERE event LIKE 'discord-voice-%' AND input IS NOT NULL",
        [],
        |row| crate::messaging::storage::decode(row, 0),
    )?)).unwrap();
    assert!(ingress.submit(vec![prior], None).await.unwrap().receipts[0].repeated);
    call.utterance(7, 1).await;
    assert!(
        timeout(Duration::from_millis(850), spoken.recv())
            .await
            .is_err()
    );
    assert_eq!(audio.heard.load(Ordering::Relaxed), 1);
    assert!(call.speaking());

    wire.dispatch(5, "INTERACTION_CREATE", command(5, "stop", None))
        .await;
    timeout(LIMIT, async {
        while call.speaking() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("native interruption stopped the spoken cue");

    wire.dispatch(
        6,
        "MESSAGE_CREATE",
        message("287", "22", "<@9> approval", true),
    )
    .await;
    tool_prompt(&mut fixture, &gateway, &model, "22").await;
    assert_eq!(model.requests().len(), 2);
    assert!(
        timeout(Duration::from_millis(250), spoken.recv())
            .await
            .is_err()
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn disabled_and_unavailable_voice_cues_leave_the_native_tool_unchanged() {
    for enabled in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
        let mut fixture = Fixtures::start().await;
        let (calls, mut spoken) = mpsc::channel(4);
        let audio = Arc::new(Speech {
            called: calls,
            heard: AtomicU64::new(0),
            fail: enabled,
        });
        let (gateway, _wire, _ingress, call) =
            voice_call(&mut fixture, root.path(), &model, enabled, audio).await;
        call.utterance(7, 1).await;
        tool_prompt(&mut fixture, &gateway, &model, "21").await;
        if enabled {
            assert_eq!(
                timeout(LIMIT, spoken.recv()).await.unwrap().unwrap(),
                "Checking now."
            );
        } else {
            assert!(
                timeout(Duration::from_millis(250), spoken.recv())
                    .await
                    .is_err()
            );
        }
        assert!(!call.speaking());
        assert_eq!(model.requests().len(), 1);
        gateway.shutdown().await.unwrap();
    }
}
