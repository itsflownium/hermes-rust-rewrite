//! Loopback model and owner fixtures for gateway and stock adapter tests.

use super::{AdapterConfig, AudioService, StockAdapter};
use crate::{Gateway, GatewayConfig, Peer, RunningGateway, lock};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::State,
    http::header,
    response::{IntoResponse, Response as HttpResponse},
    routing::post,
};
use futures_util::{StreamExt, stream};
use hermes_engine::{CancellationToken, Config};
use serde_json::{Value, json};
use std::{
    convert::Infallible,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, mpsc},
    task::JoinHandle,
};

#[derive(Clone)]
struct ModelState {
    requests: Arc<Mutex<Vec<Value>>>,
    steer_release: Arc<Semaphore>,
}

pub struct Model {
    state: ModelState,
    stock_adapters: Vec<StockAdapter>,
    url: String,
    task: JoinHandle<()>,
}

impl Drop for Model {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Model {
    pub async fn new() -> Self {
        let state = ModelState {
            requests: Arc::default(),
            steer_release: Arc::new(Semaphore::new(0)),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1", listener.local_addr().unwrap());
        let router = Router::new()
            .route("/v1/chat/completions", post(model))
            .route(
                "/v1/models",
                axum::routing::get(|| async {
                    Json(json!({"data":[{"id":"fixture"},{"id":"second-model"}]}))
                }),
            )
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            state,
            stock_adapters: Vec::new(),
            url,
            task,
        }
    }

    pub fn with_stock_adapter(mut self, adapter: StockAdapter) -> Self {
        self.stock_adapters.push(adapter);
        self
    }

    pub async fn gateway(&self, root: &Path, adapters: Vec<AdapterConfig>) -> RunningGateway {
        self.gateway_with_audio(root, adapters, None).await
    }

    pub async fn gateway_with_audio(
        &self,
        root: &Path,
        adapters: Vec<AdapterConfig>,
        audio: Option<Arc<dyn AudioService>>,
    ) -> RunningGateway {
        let mut config = GatewayConfig::new(
            root,
            Config {
                base_url: self.url.clone(),
                model: "fixture".into(),
                api_key: String::new(),
            },
        );
        config.adapters = adapters;
        config.stock_adapters.clone_from(&self.stock_adapters);
        config.messaging_audio = audio;
        Gateway::start(config).await.unwrap()
    }

    pub fn requests(&self) -> Vec<Value> {
        self.state.requests.lock().unwrap().clone()
    }

    pub fn release_waiting_turn(&self) {
        self.state.steer_release.add_permits(1);
    }
}

fn delta(content: Value, finish: Option<&str>) -> String {
    format!(
        "data: {}\n\n",
        json!({"choices":[{"index":0,"delta":content,"finish_reason":finish}]})
    )
}

async fn model(State(state): State<ModelState>, Json(request): Json<Value>) -> HttpResponse {
    state.requests.lock().unwrap().push(request.clone());
    let last = request["messages"].as_array().unwrap().last().unwrap();
    let text = last["content"].as_str().unwrap_or("attachment");
    let pending = match text {
        "hold" => Some(false),
        "steer wait" => Some(true),
        _ => None,
    };
    let body = if let Some(steer) = pending {
        let initial = stream::once(async {
            Ok::<_, Infallible>(Bytes::from(delta(json!({"content":"Working"}), None)))
        });
        let rest = stream::once(async move {
            if steer {
                let permit = state.steer_release.acquire().await.unwrap();
                permit.forget();
                Ok::<_, Infallible>(Bytes::from(delta(json!({}), Some("stop"))))
            } else {
                std::future::pending::<Result<Bytes, Infallible>>().await
            }
        });
        Body::from_stream(initial.chain(rest))
    } else if matches!(text, "approval" | "clarify" | "write artifact") {
        let (name, args) = match text {
            "approval" => ("get_current_time", json!({})),
            "write artifact" => (
                "write_file",
                json!({"path":"result.txt","content":"scoped artifact"}),
            ),
            _ => (
                "clarify",
                json!({"question":"Choose a release branch","choices":["stable","preview"]}),
            ),
        };
        Body::from(delta(
            json!({"tool_calls":[{"index":0,"id":format!("{name}-id"),"type":"function","function":{
                "name":name,"arguments":args.to_string(),
            }}]}),
            Some("tool_calls"),
        ))
    } else {
        let output = if text == "split" {
            "first|second".into()
        } else if last["tool_call_id"] == "write_file-id" {
            "saved\nMEDIA:result.txt".into()
        } else {
            format!("answer {text}")
        };
        Body::from(delta(json!({"content":output}), Some("stop")))
    };
    ([(header::CONTENT_TYPE, "text/event-stream")], body).into_response()
}

/// Exercise authenticated dispatch in-process with a simulated peer.
pub async fn dispatch(
    gateway: &RunningGateway,
    method: &str,
    params: &Value,
    local_native: bool,
) -> Result<Value, i64> {
    let peer = Peer {
        id: "stock-adapter-fixture".into(),
        tx: mpsc::channel(4).0,
        stop: CancellationToken::new(),
        canonical: true,
        local_native,
    };
    gateway
        .shared
        .dispatch(method, params, &peer)
        .await
        .map_err(|error| error.code)
}

pub async fn transcript(gateway: &RunningGateway, session_id: &str) -> Value {
    let session = gateway.shared.session(session_id).await.unwrap();
    serde_json::to_value(&lock(&session.view).history).unwrap()
}
