use crate::messaging::{
    Advance, ConnectionFailure, GatewayError,
    runtime::{blocking, db, now},
};
use super::{
    DiscordAdapter, Ingress,
    client::DiscordClient,
};
use futures_util::{SinkExt, StreamExt};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::{
    sync::mpsc,
    time::{Instant, interval_at, sleep_until, timeout, timeout_at},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message as WsMessage};
use tokio_util::sync::CancellationToken;

type WebSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

pub(super) const STREAM: &str = "discord.gateway.v10";
const FRESH_SESSION: &str = "fresh";
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_DISPATCH_QUEUE: usize = 128;
pub(super) const TRUSTED_BURST_WINDOW: Duration = Duration::from_millis(300);
pub(super) const MAX_BURST_MESSAGES: usize = 6;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const IDENTIFY_INTERVAL_MS: i64 = 5_000;

pub(crate) const SOCKET_TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_discord_identify (
    connection TEXT PRIMARY KEY, next_ms INTEGER NOT NULL
);
";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Cursor {
    session: String,
    resume: String,
    sequence: u64,
}

#[derive(Deserialize)]
struct Payload {
    op: u8,
    #[serde(default)]
    d: Value,
    s: Option<u64>,
    t: Option<String>,
}

pub(super) struct Dispatch {
    pub kind: String,
    pub data: Value,
    pub received: Instant,
    sequence: u64,
}

struct Disconnect {
    failure: ConnectionFailure,
    drain: bool,
    reset: bool,
}

impl Disconnect {
    fn retry() -> Self {
        Self {
            failure: ConnectionFailure::Retryable,
            drain: false,
            reset: false,
        }
    }

    fn restart(reset: bool) -> Self {
        Self {
            drain: true,
            reset,
            ..Self::retry()
        }
    }

    fn stopped() -> Self {
        Self {
            failure: ConnectionFailure::Stopped,
            ..Self::retry()
        }
    }

    fn for_close(code: Option<u16>) -> Self {
        match code {
            Some(4004) => Self {
                failure: ConnectionFailure::Credentials,
                ..Self::retry()
            },
            Some(4010..=4014) => Self {
                failure: ConnectionFailure::Configuration,
                ..Self::retry()
            },
            Some(4007 | 4009) => Self::restart(true),
            _ => Self::retry(),
        }
    }
}

pub(super) fn intake_failure(error: GatewayError) -> ConnectionFailure {
    match error {
        GatewayError::StaleConnection => ConnectionFailure::Stopped,
        _ => ConnectionFailure::Retryable,
    }
}

pub(super) async fn listen(
    adapter: &DiscordAdapter,
    ingress: Ingress,
    cancel: CancellationToken,
) -> ConnectionFailure {
    match run(adapter, ingress, cancel).await {
        Ok(failure) | Err(failure) => failure,
    }
}

async fn run(
    adapter: &DiscordAdapter,
    ingress: Ingress,
    cancel: CancellationToken,
) -> Result<ConnectionFailure, ConnectionFailure> {
    let previous = ingress
        .checkpoint(STREAM.into())
        .await
        .map_err(intake_failure)?;
    let resume = previous
        .as_deref()
        .filter(|raw| *raw != FRESH_SESSION)
        .map(|raw| {
            serde_json::from_str::<Cursor>(raw).map_err(|_| ConnectionFailure::Configuration)
        })
        .transpose()?;
    let resuming = resume.is_some();
    let url = match &resume {
        Some(cursor) => adapter
            .client
            .gateway_url(&cursor.resume)
            .map_err(|_| ConnectionFailure::Configuration)?,
        None => adapter.bot.gateway.clone(),
    };
    if !resuming {
        let limits = &adapter.bot.session_start_limit;
        if adapter.bot.shards > 1 {
            return Err(ConnectionFailure::Configuration);
        }
        if limits.remaining == 0 {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(ConnectionFailure::Stopped),
                _ = tokio::time::sleep(Duration::from_millis(limits.reset_after.max(1))) => {},
            }
            return Err(ConnectionFailure::Retryable);
        }
    }
    if !resuming {
        reserve_identify(&ingress, &cancel).await?;
    }
    let socket = adapter
        .client
        .websocket(&url)
        .await
        .map_err(|_| ConnectionFailure::Retryable)?;
    let (events, queue) = mpsc::channel(MAX_DISPATCH_QUEUE);
    let (outgoing, outbound) = mpsc::channel(16);
    let auth = authentication(
        &adapter.client,
        adapter.settings.gateway_intents(),
        resume.as_ref(),
    );
    let sequence = resume.as_ref().map(|cursor| cursor.sequence);
    let worker = dispatches(
        adapter, &ingress, queue, outgoing, previous, resume, &cancel,
    );
    tokio::pin!(worker);
    let end = {
        let wire = socket_loop(
            socket,
            auth,
            sequence,
            events,
            outbound,
            Duration::from_secs(adapter.settings.dispatch_silence_secs),
            &cancel,
        );
        tokio::pin!(wire);
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Disconnect::stopped(),
            error = &mut worker => return Err(error.err().unwrap_or(ConnectionFailure::Retryable)),
            end = &mut wire => end,
        }
    };
    let _ = ingress.report_transport_ready(false);
    if end.drain {
        match timeout(DRAIN_TIMEOUT, &mut worker).await {
            Ok(Ok(())) => {}
            Ok(Err(failure)) => return Err(failure),
            Err(_) => return Err(ConnectionFailure::Retryable),
        }
    }
    if end.reset {
        ingress.report_gap().map_err(intake_failure)?;
        ingress
            .invalidate_actor_evidence(None)
            .await
            .map_err(intake_failure)?;
        adapter
            .remember_recovery(&ingress)
            .map_err(intake_failure)?;
        let previous = ingress
            .checkpoint(STREAM.into())
            .await
            .map_err(intake_failure)?;
        ingress
            .submit(
                Vec::new(),
                Some(Advance {
                    stream: STREAM.into(),
                    previous,
                    checkpoint: FRESH_SESSION.into(),
                }),
            )
            .await
            .map_err(intake_failure)?;
    }
    Ok(end.failure)
}

async fn reserve_identify(
    ingress: &Ingress,
    cancel: &CancellationToken,
) -> Result<(), ConnectionFailure> {
    use rusqlite::{OptionalExtension, params};
    loop {
        let scope = ingress.clone();
        let wait =
            blocking(move || {
                scope.checked(true, |_| {
                    db(scope.storage().write(|tx| {
                        let clock = now();
                        let next: Option<i64> = tx.query_row(
                "SELECT next_ms FROM messaging_discord_identify WHERE connection=?1",
                [scope.connection_name()], |row| row.get(0),
            ).optional()?;
                        if let Some(next) = next
                            && next > clock
                        {
                            return Ok(next.saturating_sub(clock).min(IDENTIFY_INTERVAL_MS));
                        }
                        tx.execute(
                "INSERT INTO messaging_discord_identify(connection,next_ms) VALUES (?1,?2)
                 ON CONFLICT(connection) DO UPDATE SET next_ms=excluded.next_ms",
                params![scope.connection_name(), clock.saturating_add(IDENTIFY_INTERVAL_MS)],
            )?;
                        Ok(0)
                    }))
                })
            })
            .await
            .map_err(intake_failure)?;
        if wait == 0 {
            return Ok(());
        }
        tokio::select! {
            _ = cancel.cancelled() => return Err(ConnectionFailure::Stopped),
            _ = tokio::time::sleep(Duration::from_millis(wait as u64)) => {},
        }
    }
}

fn authentication(client: &DiscordClient, intents: u64, resume: Option<&Cursor>) -> Value {
    if let Some(cursor) = resume {
        return json!({"op":6,"d":{"token":client.token(),"session_id":cursor.session,"seq":cursor.sequence}});
    }
    json!({"op":2,"d":{"token":client.token(),"intents":intents,"properties":{
        "os":std::env::consts::OS,"browser":"Mercury","device":"Mercury"
    }}})
}

async fn socket_loop(
    mut socket: WebSocket,
    auth: Value,
    mut sequence: Option<u64>,
    events: mpsc::Sender<Dispatch>,
    mut outgoing: mpsc::Receiver<Value>,
    dispatch_silence: Duration,
    cancel: &CancellationToken,
) -> Disconnect {
    let hello = match timeout(HELLO_TIMEOUT, socket.next()).await {
        Ok(Some(Ok(WsMessage::Text(text)))) => serde_json::from_str::<Payload>(&text).ok(),
        Ok(Some(Ok(WsMessage::Close(close)))) => {
            return Disconnect::for_close(close.map(|frame| frame.code.into()));
        }
        _ => None,
    };
    let millis = hello
        .filter(|hello| hello.op == 10)
        .and_then(|hello| hello.d.get("heartbeat_interval")?.as_u64())
        .filter(|millis| (10..=300_000).contains(millis));
    let Some(millis) = millis else {
        return Disconnect::retry();
    };
    if send_json(&mut socket, auth).await.is_err() {
        return Disconnect::retry();
    }
    let mut heartbeat = interval_at(
        Instant::now() + Duration::from_millis(jitter(millis)),
        Duration::from_millis(millis),
    );
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut awaiting_ack = false;
    let silence = sleep_until(Instant::now() + dispatch_silence);
    tokio::pin!(silence);
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Disconnect::stopped(),
            _ = &mut silence => return Disconnect::restart(false),
            _ = heartbeat.tick() => {
                if awaiting_ack || send_heartbeat(&mut socket, sequence).await.is_err() {
                    return Disconnect::retry();
                }
                awaiting_ack = true;
            }
            message = socket.next() => match message {
                Some(Ok(WsMessage::Text(text))) => {
                    let Ok(frame) = serde_json::from_str::<Payload>(&text) else {
                        return Disconnect::retry();
                    };
                    match frame.op {
                        0 => {
                            let (Some(seq), Some(kind)) = (frame.s, frame.t) else {
                                return Disconnect::retry();
                            };
                            sequence = Some(sequence.map_or(seq, |previous| previous.max(seq)));
                            silence.as_mut().reset(Instant::now() + dispatch_silence);
                            let event = Dispatch { kind, data: frame.d, received: Instant::now(), sequence: seq };
                            if events.try_send(event).is_err() {
                                return Disconnect::restart(false);
                            }
                        }
                        1 => {
                            if send_heartbeat(&mut socket, sequence).await.is_err() {
                                return Disconnect::retry();
                            }
                            awaiting_ack = true;
                        }
                        7 => return Disconnect::restart(false),
                        9 => return Disconnect::restart(frame.d.as_bool() != Some(true)),
                        11 => awaiting_ack = false,
                        _ => {}
                    }
                }
                Some(Ok(WsMessage::Ping(_))) => {
                    if socket.flush().await.is_err() { return Disconnect::retry(); }
                }
                Some(Ok(WsMessage::Close(close))) => return Disconnect::for_close(close.map(|frame| frame.code.into())),
                Some(Ok(WsMessage::Pong(_))) => {},
                _ => return Disconnect::retry(),
            },
            Some(operation) = outgoing.recv() => {
                if send_json(&mut socket, operation).await.is_err() {
                    return Disconnect::retry();
                }
            }
        }
    }
}

async fn send_heartbeat(socket: &mut WebSocket, sequence: Option<u64>) -> Result<(), ()> {
    send_json(socket, json!({"op":1,"d":sequence})).await
}

async fn send_json(socket: &mut WebSocket, json: Value) -> Result<(), ()> {
    timeout(
        HELLO_TIMEOUT,
        socket.send(WsMessage::Text(json.to_string().into())),
    )
    .await
    .map_err(|_| ())?
    .map_err(|_| ())
}

fn jitter(millis: u64) -> u64 {
    let mut sample = [0; 8];
    if SystemRandom::new().fill(&mut sample).is_err() {
        return millis / 2;
    }
    u64::from_ne_bytes(sample) % millis
}

async fn dispatches(
    adapter: &DiscordAdapter,
    ingress: &Ingress,
    mut queue: mpsc::Receiver<Dispatch>,
    outgoing: mpsc::Sender<Value>,
    mut previous: Option<String>,
    mut cursor: Option<Cursor>,
    cancel: &CancellationToken,
) -> Result<(), ConnectionFailure> {
    let mut first_ready = true;
    let mut maintenance = tokio::time::interval(Duration::from_secs(2));
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut pending = None;
    loop {
        let frame = if pending.is_some() {
            pending.take()
        } else {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                frame = queue.recv() => frame,
                _ = maintenance.tick(), if !first_ready => {
                    adapter.resume_preparations(ingress, cancel).await?;
                    adapter.maintain_threads(ingress, cancel);
                    for operation in adapter.voice_housekeeping(ingress) {
                        let _ = outgoing.try_send(operation);
                    }
                    continue;
                }
            }
        };
        let Some(frame) = frame else { break };
        if let Some(cursor) = &cursor
            && frame.sequence <= cursor.sequence
        {
            continue;
        }
        let ready = matches!(frame.kind.as_str(), "READY" | "RESUMED");
        let mut last_sequence = frame.sequence;
        let mut continuations = Vec::new();
        if cursor.is_some()
            && frame.kind == "MESSAGE_CREATE"
            && let Some(mut burst) = adapter.trusted_burst(&frame.data)
        {
            let deadline = frame.received + TRUSTED_BURST_WINDOW;
            for _ in 1..MAX_BURST_MESSAGES {
                let next = tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    received = timeout_at(deadline, queue.recv()) => received.ok().flatten(),
                };
                let Some(next) = next else { break };
                if next.kind != "MESSAGE_CREATE"
                    || next.received > deadline
                    || next.sequence <= last_sequence
                    || !burst.take(&next.data)
                {
                    pending = Some(next);
                    break;
                }
                last_sequence = next.sequence;
                continuations.push(next.data);
            }
        }
        let next = if frame.kind == "READY" {
            let bot = frame.data.pointer("/user/id").and_then(Value::as_str);
            let app = frame
                .data
                .pointer("/application/id")
                .and_then(Value::as_str);
            if bot != Some(adapter.bot.bot_id.as_str())
                || app != Some(adapter.bot.application_id.as_str())
            {
                return Err(ConnectionFailure::Credentials);
            }
            let session = frame
                .data
                .get("session_id")
                .and_then(Value::as_str)
                .filter(|value| crate::messaging::runtime::valid_id(value))
                .ok_or(ConnectionFailure::Retryable)?;
            let resume = frame
                .data
                .get("resume_gateway_url")
                .and_then(Value::as_str)
                .ok_or(ConnectionFailure::Retryable)?;
            let resume = adapter
                .client
                .gateway_url(resume)
                .map_err(|_| ConnectionFailure::Configuration)?;
            Cursor {
                session: session.into(),
                resume: resume.to_string(),
                sequence: frame.sequence,
            }
        } else {
            let mut cursor = cursor.clone().ok_or(ConnectionFailure::Retryable)?;
            cursor.sequence = last_sequence;
            cursor
        };
        let checkpoint =
            serde_json::to_string(&next).map_err(|_| ConnectionFailure::Configuration)?;
        let advance = Advance {
            stream: STREAM.into(),
            previous: previous.clone(),
            checkpoint: checkpoint.clone(),
        };
        let fresh = previous.as_deref() == Some(FRESH_SESSION) && frame.kind == "READY";
        adapter
            .dispatch(ingress, frame, continuations, advance, &outgoing, cancel)
            .await?;
        previous = Some(checkpoint);
        cursor = Some(next);
        if ready && first_ready {
            adapter.transport_ready(ingress, fresh, cancel).await?;
            ingress
                .report_transport_ready(true)
                .map_err(intake_failure)?;
            first_ready = false;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_provider_close_without_discarding_resumable_state() {
        assert!(Disconnect::for_close(Some(4007)).reset);
        assert!(Disconnect::for_close(Some(4009)).reset);
        assert!(!Disconnect::for_close(Some(4000)).reset);
        assert_eq!(
            Disconnect::for_close(Some(4004)).failure,
            ConnectionFailure::Credentials
        );
        assert_eq!(
            Disconnect::for_close(Some(4014)).failure,
            ConnectionFailure::Configuration
        );
    }
}
