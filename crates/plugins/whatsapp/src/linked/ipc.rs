use crate::common::lock;
use hermes_gateway_core::messaging::ConnectionFailure;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::Path,
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    process::Command,
    sync::{mpsc, oneshot, watch},
};
use tokio_util::sync::CancellationToken;

const VERSION: u8 = 1;
const MAX_FRAME: usize = 8 * 1024 * 1024;
pub(super) const MAX_MEDIA: usize = 4 * 1024 * 1024;
const OUTSTANDING: usize = 32;

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Frame {
    version: u8,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<u64>,
    #[serde(default)]
    pub data: Value,
}

impl Frame {
    fn new(kind: &str, id: Option<u64>, data: Value) -> Self {
        Self {
            version: VERSION,
            kind: kind.into(),
            id,
            data,
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct Setup {
    pub pairing: Option<String>,
    pub browser: Option<String>,
    pub account: Option<String>,
    pub device: Option<String>,
    pub auth: Option<Value>,
    pub revoked: bool,
    pub stopped: bool,
}

struct Reply {
    id: u64,
    outstanding: Arc<Mutex<BTreeMap<u64, oneshot::Sender<Value>>>>,
}

impl Drop for Reply {
    fn drop(&mut self) {
        lock(&self.outstanding).remove(&self.id);
    }
}

pub(super) struct Bridge {
    outgoing: mpsc::Sender<Frame>,
    incoming: tokio::sync::Mutex<mpsc::Receiver<Frame>>,
    setup: watch::Receiver<Setup>,
    outstanding: Arc<Mutex<BTreeMap<u64, oneshot::Sender<Value>>>>,
    sequence: AtomicU64,
    stop: CancellationToken,
    ended: watch::Receiver<bool>,
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum CallFailure {
    Unavailable,
    Unknown,
    Rejected,
}

impl Bridge {
    pub fn launch(script: &Path) -> Result<Self, ConnectionFailure> {
        let mut command = Command::new("node");
        command
            .arg(script)
            .env_remove("NODE_OPTIONS")
            .env_remove("NODE_PATH")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        #[cfg(windows)]
        command.creation_flags(0x08000000);
        let mut child = command
            .spawn()
            .map_err(|_| ConnectionFailure::Configuration)?;
        let stdin = child.stdin.take().ok_or(ConnectionFailure::Configuration)?;
        let stdout = child
            .stdout
            .take()
            .ok_or(ConnectionFailure::Configuration)?;
        let (outgoing, commands) = mpsc::channel(OUTSTANDING);
        let (updates, incoming) = mpsc::channel(128);
        let (state, setup) = watch::channel(Setup::default());
        let (finished, ended) = watch::channel(false);
        let outstanding = Arc::new(Mutex::new(BTreeMap::new()));
        let pending = outstanding.clone();
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => {},
                _ = child.wait() => {},
                _ = reader(stdout,updates,state.clone(),pending.clone()) => {},
                _ = writer(stdin,commands) => {},
            }
            cancel.cancel();
            let _ = child.kill().await;
            lock(&pending).clear();
            state.send_modify(|value| value.stopped = true);
            finished.send_replace(true);
        });
        Ok(Self {
            outgoing,
            incoming: tokio::sync::Mutex::new(incoming),
            setup,
            outstanding,
            sequence: AtomicU64::new(1),
            stop,
            ended,
        })
    }

    pub fn start(
        &self,
        auth: Option<Value>,
        expected: &str,
        burst_ms: u16,
    ) -> Result<(), ConnectionFailure> {
        self.notify(
            "start",
            json!({"auth":auth,"expected":expected,"burst_ms":burst_ms}),
        )
        .map_err(|_| ConnectionFailure::Retryable)
    }

    pub fn notify(&self, kind: &str, data: Value) -> Result<(), CallFailure> {
        self.outgoing
            .try_send(Frame::new(kind, None, data))
            .map_err(|_| CallFailure::Unavailable)
    }

    pub async fn request(
        &self,
        kind: &str,
        data: Value,
        cancel: &CancellationToken,
    ) -> Result<Value, CallFailure> {
        if self.stop.is_cancelled() || cancel.is_cancelled() {
            return Err(CallFailure::Unavailable);
        }
        let id = self.sequence.fetch_add(1, Ordering::Relaxed);
        let (response, received) = oneshot::channel();
        {
            let mut waiting = lock(&self.outstanding);
            if waiting.len() >= OUTSTANDING {
                return Err(CallFailure::Unavailable);
            }
            waiting.insert(id, response);
        }
        let _request = Reply {
            id,
            outstanding: self.outstanding.clone(),
        };
        self.outgoing
            .try_send(Frame::new(kind, Some(id), data))
            .map_err(|_| CallFailure::Unavailable)?;
        let answer = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(CallFailure::Unknown),
            result = tokio::time::timeout(Duration::from_secs(20),received) => {
                result.map_err(|_| CallFailure::Unknown)?.map_err(|_| CallFailure::Unknown)?
            }
        };
        match answer["outcome"].as_str() {
            Some("ok") => Ok(answer),
            Some("rejected") => Err(CallFailure::Rejected),
            Some("unavailable") => Err(CallFailure::Unavailable),
            _ => Err(CallFailure::Unknown),
        }
    }

    pub fn state(&self) -> Setup {
        self.setup.borrow().clone()
    }

    pub async fn wait(
        &self,
        predicate: impl Fn(&Setup) -> bool,
        cancel: &CancellationToken,
    ) -> Result<Setup, ConnectionFailure> {
        let mut watch = self.setup.clone();
        loop {
            let snapshot = watch.borrow_and_update().clone();
            if snapshot.revoked {
                return Err(ConnectionFailure::Credentials);
            }
            if snapshot.stopped {
                return Err(ConnectionFailure::Retryable);
            }
            if predicate(&snapshot) {
                return Ok(snapshot);
            }
            tokio::select! {
                _ = cancel.cancelled() => return Err(ConnectionFailure::Stopped),
                change = watch.changed() => if change.is_err() { return Err(ConnectionFailure::Retryable) },
            }
        }
    }

    pub async fn next(&self) -> Option<Frame> {
        self.incoming.lock().await.recv().await
    }

    pub async fn stop(&self) {
        self.stop.cancel();
        let mut ended = self.ended.clone();
        if !*ended.borrow() {
            let _ = ended.changed().await;
        }
    }
}

async fn writer(
    mut stream: impl AsyncWrite + Unpin,
    mut commands: mpsc::Receiver<Frame>,
) -> std::io::Result<()> {
    while let Some(frame) = commands.recv().await {
        let body = serde_json::to_vec(&frame).map_err(std::io::Error::other)?;
        if body.len() > MAX_FRAME {
            return Err(std::io::Error::other("Bridge frame too large"));
        }
        stream.write_u32(body.len() as u32).await?;
        stream.write_all(&body).await?;
        stream.flush().await?;
    }
    Ok(())
}

async fn reader(
    mut stream: impl AsyncRead + Unpin,
    updates: mpsc::Sender<Frame>,
    setup: watch::Sender<Setup>,
    outstanding: Arc<Mutex<BTreeMap<u64, oneshot::Sender<Value>>>>,
) -> std::io::Result<()> {
    loop {
        let size = stream.read_u32().await? as usize;
        if size == 0 || size > MAX_FRAME {
            return Err(std::io::Error::other("Invalid bridge frame size"));
        }
        let mut body = vec![0; size];
        stream.read_exact(&mut body).await?;
        let frame: Frame = serde_json::from_slice(&body).map_err(std::io::Error::other)?;
        if frame.version != VERSION {
            return Err(std::io::Error::other("Unsupported bridge version"));
        }
        match frame.kind.as_str() {
            "response" => {
                if let Some(request) = frame.id.and_then(|id| lock(&outstanding).remove(&id)) {
                    let _ = request.send(frame.data);
                }
            }
            "qr" => setup.send_modify(|state| {
                state.pairing = frame.data["payload"]
                    .as_str()
                    .filter(|value| value.len() <= 8192)
                    .map(str::to_owned);
            }),
            "passkey" => setup.send_modify(|state| {
                state.browser = frame.data["url"]
                    .as_str()
                    .filter(|value| value.len() <= 8192)
                    .map(str::to_owned);
            }),
            "ready" => {
                setup.send_modify(|state| {
                    state.account = frame.data["account"].as_str().map(str::to_owned);
                    state.device = frame.data["device"].as_str().map(str::to_owned);
                    state.pairing = None;
                    state.browser = None;
                    if let Some(auth) = frame.data.get("auth") {
                        state.auth = Some(auth.clone());
                    }
                });
                push(&updates, frame)?;
            }
            "credentials" => {
                setup.send_modify(|state| state.auth = frame.data.get("auth").cloned());
                push(&updates, frame)?;
            }
            "closed" if frame.data["revoked"] == true => {
                setup.send_modify(|state| state.revoked = true);
                push(&updates, frame)?;
                return Ok(());
            }
            _ => push(&updates, frame)?,
        }
    }
}

fn push(updates: &mpsc::Sender<Frame>, frame: Frame) -> std::io::Result<()> {
    updates
        .try_send(frame)
        .map_err(|_| std::io::Error::other("Bridge handoff backlog"))
}
