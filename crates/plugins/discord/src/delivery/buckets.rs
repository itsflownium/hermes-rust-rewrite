use super::{retry, snowflake};
use crate::messaging::{
    DeliveryResult, GatewayError, Ingress, RetryScope,
    runtime::{blocking, db, now},
};
use reqwest::{Method, Request, StatusCode, header::HeaderMap};
use rusqlite::{OptionalExtension, params};
use serde_json::Value;
use std::time::Duration;

pub(crate) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_discord_rate_routes (
    connection TEXT NOT NULL, major TEXT NOT NULL, operation TEXT NOT NULL,
    bucket TEXT NOT NULL, seen_ms INTEGER NOT NULL,
    PRIMARY KEY(connection,major,operation)
);
CREATE TABLE IF NOT EXISTS messaging_discord_rate_buckets (
    connection TEXT NOT NULL, major TEXT NOT NULL, bucket TEXT NOT NULL, until_ms INTEGER NOT NULL,
    PRIMARY KEY(connection,major,bucket)
);
";

const MAX_DELAY: Duration = Duration::from_secs(86_400);
const MAX_LIMITED_ATTEMPTS: u32 = 8;
const FALLBACK_DELAY: Duration = Duration::from_secs(1);
const MIN_DELAY: Duration = Duration::from_millis(250);
const ROUTE_RETENTION_MS: i64 = 7 * 86_400 * 1000;

pub(super) struct Route {
    major: String,
    operation: &'static str,
}

impl Route {
    pub(super) fn from_request(request: &Request) -> Option<Self> {
        let path = request.url().path_segments()?.collect::<Vec<_>>();
        let channels = path.iter().position(|segment| *segment == "channels")?;
        let rest = &path[channels..];
        let major = *rest.get(1)?;
        if !snowflake(major) {
            return None;
        }
        let operation = match (request.method(), rest) {
            (&Method::GET, ["channels", _]) => "get-channel",
            (&Method::GET, ["channels", _, "messages", _]) => "get-message",
            (&Method::POST, ["channels", _, "messages"]) => "create-message",
            (&Method::POST, ["channels", _, "typing"]) => "typing",
            (&Method::PUT, ["channels", _, "messages", _, "reactions", _, "@me"]) => "create-reaction",
            (&Method::PATCH, ["channels", _]) => "edit-channel",
            (&Method::PATCH, ["channels", _, "messages", _]) => "edit-message",
            (&Method::PUT, ["channels", _, "messages", "pins", _]) => "pin-message",
            (&Method::DELETE, ["channels", _, "messages", _]) => "delete-message",
            (&Method::POST, ["channels", _, "messages", _, "threads"]) => "create-message-thread",
            (&Method::POST, ["channels", _, "threads"]) => "create-forum-post",
            _ => return None,
        };
        Some(Self {
            major: major.to_owned(),
            operation,
        })
    }
}

pub(super) async fn wait(
    ingress: &Ingress,
    route: Option<&Route>,
) -> Result<Option<(Duration, RetryScope)>, GatewayError> {
    let scope = ingress.clone();
    let route = route.map(|route| (route.major.clone(), route.operation));
    blocking(move || scope.checked(true, |_| {
        let current = now();
        db(scope.storage().read(|db| {
            let global: Option<i64> = db.query_row(
                "SELECT until_ms FROM messaging_cooldowns WHERE connection=?1 AND scope='' AND until_ms>?2",
                params![scope.connection_name(), current], |row| row.get(0),
            ).optional()?;
            if let Some(deadline) = global {
                return Ok(Some((remaining(deadline, current), RetryScope::Account)));
            }
            let Some((major, operation)) = route else {
                return Ok(None);
            };
            let deadline: Option<i64> = db.query_row(
                "SELECT b.until_ms FROM messaging_discord_rate_routes r
                 JOIN messaging_discord_rate_buckets b ON b.connection=r.connection AND b.major=r.major AND b.bucket=r.bucket
                 WHERE r.connection=?1 AND r.major=?2 AND r.operation=?3 AND b.until_ms>?4",
                params![scope.connection_name(), major, operation, current], |row| row.get(0),
            ).optional()?;
            Ok(deadline.map(|deadline| (remaining(deadline, current), RetryScope::Part)))
        }))
    })).await
}

pub(super) async fn observe(
    ingress: &Ingress,
    route: Option<&Route>,
    status: StatusCode,
    headers: &HeaderMap,
    body: &Value,
    attempt: u32,
) -> Option<DeliveryResult> {
    let rejected = status == StatusCode::TOO_MANY_REQUESTS;
    let bucket = read_header(headers, "x-ratelimit-bucket").filter(|bucket| valid_bucket(bucket));
    if !rejected && (!status.is_success() || bucket.is_none()) {
        return None;
    }
    let rates = Rejection::from_response(headers, body);
    if rejected && rates.global {
        // Also persist the account fence directly. Other workers can still be
        // between their outbox claim and HTTP dispatch when the 429 arrives.
        let _ = record_global(ingress, rates.after).await;
        return Some(rates.result(RetryScope::Account, attempt));
    }
    let mut scope = RetryScope::Destination;
    if let Some((route, bucket)) = route.zip(bucket) {
        let delay = if rejected {
            Some(rates.after)
        } else if read_header(headers, "x-ratelimit-remaining") == Some("0") {
            seconds(read_header(headers, "x-ratelimit-reset-after")).map(|value| bounded(value).0)
        } else {
            None
        };
        if remember(ingress, route, bucket, delay).await.is_ok() {
            scope = RetryScope::Part;
        }
    }
    rejected.then(|| rates.result(scope, attempt))
}

async fn record_global(ingress: &Ingress, delay: Duration) -> Result<(), GatewayError> {
    let scope = ingress.clone();
    blocking(move || {
        scope.checked(false, |_| {
            db(scope.storage().write(|tx| {
                tx.execute(
                    "INSERT INTO messaging_cooldowns(connection,scope,until_ms) VALUES (?1,'',?2)
             ON CONFLICT(connection,scope) DO UPDATE SET until_ms=MAX(until_ms,excluded.until_ms)",
                    params![scope.connection_name(), deadline(now(), delay)],
                )?;
                Ok(())
            }))
        })
    })
    .await
}

async fn remember(
    ingress: &Ingress,
    route: &Route,
    bucket: &str,
    delay: Option<Duration>,
) -> Result<(), GatewayError> {
    let (scope, major, operation, bucket) = (
        ingress.clone(),
        route.major.to_owned(),
        route.operation,
        bucket.to_owned(),
    );
    blocking(move || scope.checked(false, |_| db(scope.storage().write(|tx| {
        let current = now();
        tx.execute(
            "DELETE FROM messaging_discord_rate_buckets WHERE connection=?1 AND until_ms<=?2",
            params![scope.connection_name(), current],
        )?;
        tx.execute(
            "DELETE FROM messaging_discord_rate_routes WHERE connection=?1 AND seen_ms<?2",
            params![scope.connection_name(), current.saturating_sub(ROUTE_RETENTION_MS)],
        )?;
        tx.execute(
            "INSERT INTO messaging_discord_rate_routes(connection,major,operation,bucket,seen_ms) VALUES (?1,?2,?3,?4,?5)
             ON CONFLICT(connection,major,operation) DO UPDATE SET bucket=excluded.bucket,seen_ms=excluded.seen_ms",
            params![scope.connection_name(), major, operation, bucket, current],
        )?;
        if let Some(delay) = delay {
            tx.execute(
                "INSERT INTO messaging_discord_rate_buckets(connection,major,bucket,until_ms) VALUES (?1,?2,?3,?4)
                 ON CONFLICT(connection,major,bucket) DO UPDATE SET until_ms=MAX(until_ms,excluded.until_ms)",
                params![scope.connection_name(), major, bucket, deadline(current, delay)],
            )?;
        }
        Ok(())
    })))).await
}

fn remaining(deadline: i64, current: i64) -> Duration {
    Duration::from_millis(deadline.saturating_sub(current).max(1) as u64)
}

fn deadline(current: i64, delay: Duration) -> i64 {
    // Round up so fractional provider milliseconds cannot shorten the fence.
    current.saturating_add(delay.as_nanos().div_ceil(1_000_000) as i64)
}

fn valid_bucket(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn read_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

fn seconds(value: Option<&str>) -> Option<f64> {
    value?.parse().ok()
}

fn bounded(seconds: f64) -> (Duration, bool) {
    if seconds > MAX_DELAY.as_secs_f64() {
        (MAX_DELAY, true)
    } else {
        (
            Duration::try_from_secs_f64(seconds)
                .unwrap_or(FALLBACK_DELAY)
                .max(MIN_DELAY),
            false,
        )
    }
}

pub(super) struct Rejection {
    after: Duration,
    excessive: bool,
    global: bool,
}

impl Rejection {
    pub(super) fn from_response(headers: &HeaderMap, body: &Value) -> Self {
        let highest = [
            body["retry_after"].as_f64(),
            seconds(read_header(headers, "retry-after")),
            seconds(read_header(headers, "x-ratelimit-reset-after")),
        ]
        .into_iter()
        .flatten()
        .filter(|seconds| !seconds.is_nan() && *seconds >= 0.0)
        .reduce(f64::max);
        let (after, excessive) = highest.map(bounded).unwrap_or((FALLBACK_DELAY, false));
        let global = body["global"] == true
            || read_header(headers, "x-ratelimit-global") == Some("true")
            || read_header(headers, "x-ratelimit-scope") == Some("global");
        Self {
            after,
            excessive,
            global,
        }
    }

    pub(super) fn result(&self, scope: RetryScope, attempt: u32) -> DeliveryResult {
        if self.excessive || attempt >= MAX_LIMITED_ATTEMPTS {
            DeliveryResult::Rejected
        } else {
            retry(self.after, scope)
        }
    }

    #[cfg(test)]
    pub(super) fn fallback_scope(&self) -> RetryScope {
        if self.global {
            RetryScope::Account
        } else {
            RetryScope::Destination
        }
    }
}
