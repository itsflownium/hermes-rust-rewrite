//! An authenticated push endpoint owned by an active messaging connection.
use super::{AdapterFuture, GatewayError, Ingress, storage};
use crate::Shared;
use axum::{
    body::to_bytes,
    extract::{Path, Request, State},
    http::{HeaderName, StatusCode},
};
use std::{sync::Arc, time::Duration};
use subtle::ConstantTimeEq;

const MAX_WEBHOOK_BODY: usize = 2 * 1024 * 1024;

pub type WebhookHandler =
    dyn Fn(Ingress, Vec<u8>) -> AdapterFuture<'static, Result<(), GatewayError>> + Send + Sync;

#[derive(Clone)]
pub(super) struct Webhook {
    pub(super) id: String,
    header: HeaderName,
    secret: String,
    receive: Arc<WebhookHandler>,
}

impl Ingress {
    /// Register a push receiver for this verified generation on the gateway listener.
    pub fn register_webhook(
        &self,
        header: HeaderName,
        secret: String,
        receive: Arc<WebhookHandler>,
    ) -> Result<String, GatewayError> {
        if secret.is_empty()
            || secret.len() > 256
            || !secret
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(GatewayError::Invalid);
        }
        let id = super::runtime::db(storage::key(
            "messaging-webhook",
            &(self.root(), self.name(), self.connection_name()),
        ))?;
        self.attach_webhook(Webhook {
            id: id.clone(),
            header,
            secret,
            receive,
        })?;
        Ok(format!("/api/messaging/webhooks/{id}"))
    }
}

pub(crate) async fn receive(
    State(owner): State<Arc<Shared>>,
    Path(id): Path<String>,
    request: Request,
) -> StatusCode {
    let Some((ingress, webhook)) = owner.messaging.find_webhook(&id).await else {
        return StatusCode::NOT_FOUND;
    };
    let mut supplied = request.headers().get_all(&webhook.header).iter();
    let valid = supplied
        .next()
        .is_some_and(|value| bool::from(value.as_bytes().ct_eq(webhook.secret.as_bytes())))
        && supplied.next().is_none();
    if !valid {
        return StatusCode::FORBIDDEN;
    }

    let body = match tokio::time::timeout(
        Duration::from_secs(10),
        to_bytes(request.into_body(), MAX_WEBHOOK_BODY),
    )
    .await
    {
        Ok(Ok(body)) => body.to_vec(),
        Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE,
        Err(_) => return StatusCode::REQUEST_TIMEOUT,
    };
    match (webhook.receive)(ingress, body).await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(GatewayError::Invalid | GatewayError::Unsupported) => StatusCode::BAD_REQUEST,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}
