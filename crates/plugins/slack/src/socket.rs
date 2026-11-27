use crate::{Connection, api::Failure, hub::Hub, intake, lock};
use futures_util::{SinkExt, StreamExt};
use hermes_gateway_core::messaging::{ConnectionFailure, GatewayError, Ingress, ReceiverLease};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::{
    net::TcpStream,
    time::{Instant, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use tokio_util::sync::CancellationToken;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
const FRAME_LIMIT: usize = 1024 * 1024;
const ACK_LIMIT: Duration = Duration::from_millis(2_600);

struct Health<'a>(&'a Hub);

impl Drop for Health<'_> {
    fn drop(&mut self) {
        self.0.set_ready(false);
    }
}

pub(crate) async fn run(
    connection: &Connection,
    ingress: &Ingress,
    hub: &Hub,
    cancel: &CancellationToken,
) -> ConnectionFailure {
    let lease = match ingress.claim_receiver() {
        Ok(lease) => lease,
        Err(GatewayError::Conflict) => {
            lock(&connection.verification)["socket"] = json!("receiver_in_use");
            return ConnectionFailure::Retryable;
        }
        Err(GatewayError::StaleConnection) => return ConnectionFailure::Stopped,
        Err(_) => return ConnectionFailure::Configuration,
    };
    let _health = Health(hub);
    let mut delay = Duration::from_millis(250);
    loop {
        let connected = open(connection, cancel).await;
        let failure = match connected {
            Ok(mut socket) => {
                if lease.check().is_err() {
                    return ConnectionFailure::Stopped;
                }
                hub.set_ready(true);
                delay = Duration::from_millis(250);
                let outcome = drive(&mut socket, connection, ingress, &lease, hub, cancel).await;
                hub.set_ready(false);
                outcome
            }
            Err(error) => error.connection(),
        };
        if failure != ConnectionFailure::Retryable {
            return failure;
        }
        tokio::select! {
            _ = cancel.cancelled() => return ConnectionFailure::Stopped,
            _ = tokio::time::sleep(delay) => {}
        }
        delay = (delay * 2).min(Duration::from_secs(5));
    }
}

async fn open(connection: &Connection, cancel: &CancellationToken) -> Result<Socket, Failure> {
    let endpoint = connection.api.socket_url(cancel).await?;
    let mut config = WebSocketConfig::default();
    config.max_frame_size = Some(FRAME_LIMIT);
    config.max_message_size = Some(FRAME_LIMIT);
    tokio::select! {
        _ = cancel.cancelled() => Err(Failure::Cancelled),
        result = timeout(Duration::from_secs(8), async {
            let (mut socket, _) = connect_async_with_config(endpoint.as_str(), Some(config), true)
                .await.map_err(|_| Failure::Unknown)?;
            loop {
                match socket.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let hello: Value = serde_json::from_str(&text).map_err(|_| Failure::Unknown)?;
                        if hello["type"] != "hello" || hello.pointer("/connection_info/app_id").and_then(Value::as_str) != Some(&connection.id.app) {
                            lock(&connection.verification)["socket"] = json!("wrong_app");
                            return Err(Failure::Credentials);
                        }
                        return Ok(socket);
                    }
                    Some(Ok(Message::Ping(_))) => socket.flush().await.map_err(|_| Failure::Unknown)?,
                    Some(Ok(Message::Pong(_))) => {}
                    _ => return Err(Failure::Unknown),
                }
            }
        }) => result.unwrap_or(Err(Failure::Unknown)),
    }
}

async fn drive(
    socket: &mut Socket,
    connection: &Connection,
    ingress: &Ingress,
    lease: &ReceiverLease,
    hub: &Hub,
    cancel: &CancellationToken,
) -> ConnectionFailure {
    let mut ping = tokio::time::interval(Duration::from_secs(15));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_frame = Instant::now();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return ConnectionFailure::Stopped,
            event = socket.next() => {
                let Some(Ok(event)) = event else { return ConnectionFailure::Retryable };
                last_frame = Instant::now();
                match event {
                    Message::Text(text) => {
                        if lease.check().is_err() { return ConnectionFailure::Stopped }
                        let Ok(frame) = serde_json::from_str::<Value>(&text) else { continue };
                        if frame["type"] == "disconnect" {
                            return ConnectionFailure::Retryable;
                        }
                        let Some(envelope) = frame["envelope_id"].as_str()
                            .filter(|id| !id.is_empty() && id.len() <= 256 && !id.chars().any(char::is_control))
                        else { continue };
                        let started = Instant::now();
                        let Ok(Ok(response)) = timeout(ACK_LIMIT, intake::receive(connection, ingress, hub, &frame)).await else { continue };
                        if lease.check().is_err() { return ConnectionFailure::Stopped }
                        let mut ack = json!({"envelope_id":envelope});
                        if frame["accepts_response_payload"] == true && let Some(response) = response {
                            ack["payload"] = response;
                        }
                        let budget = Duration::from_secs(3).saturating_sub(started.elapsed());
                        let sent = timeout(budget, socket.send(Message::Text(ack.to_string().into()))).await;
                        if !matches!(sent, Ok(Ok(()))) { return ConnectionFailure::Retryable }
                    }
                    Message::Ping(_) => if socket.flush().await.is_err() { return ConnectionFailure::Retryable },
                    Message::Close(_) => return ConnectionFailure::Retryable,
                    _ => {}
                }
            }
            _ = ping.tick() => {
                if last_frame.elapsed() > Duration::from_secs(45) { return ConnectionFailure::Retryable }
                if socket.send(Message::Ping(Vec::new().into())).await.is_err() {
                    return ConnectionFailure::Retryable;
                }
            }
        }
    }
}
