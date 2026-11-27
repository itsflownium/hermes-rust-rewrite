mod worker;

use super::{Shared, lock};
use hermes_telegram::{Client, DeliveryResolution, Identity, Store, Token, Transport};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Arc, Mutex as StdMutex, Weak},
    time::Duration,
};
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use worker::Connector;
pub(super) use worker::submission_key;

struct Active {
    bot: Identity,
    token: Token,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

pub(crate) struct Host {
    store: Store,
    transport: Option<Arc<dyn Transport>>,
    active: Mutex<Option<Active>>,
    last_error: Arc<StdMutex<Option<String>>>,
    shutdown: CancellationToken,
}

impl Host {
    pub(crate) fn new(root: &Path, shutdown: CancellationToken) -> Arc<Self> {
        Self::build(root, shutdown, None)
    }

    fn build(
        root: &Path,
        shutdown: CancellationToken,
        transport: Option<Arc<dyn Transport>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store: Store::new(root),
            transport,
            active: Mutex::new(None),
            last_error: Arc::default(),
            shutdown,
        })
    }

    #[cfg(test)]
    pub(super) fn with_transport(
        root: &Path,
        shutdown: CancellationToken,
        transport: Arc<dyn Transport>,
    ) -> Arc<Self> {
        Self::build(root, shutdown, Some(transport))
    }

    fn client(&self, token: Token) -> Result<Client, String> {
        match &self.transport {
            Some(transport) => Ok(Client::with_transport(token, transport.clone())),
            None => Client::new(token).map_err(|error| error.to_string()),
        }
    }

    pub(super) fn restore(self: &Arc<Self>, owner: Option<Weak<Shared>>) {
        let host = self.clone();
        tokio::spawn(async move {
            // The previous owner's lease can outlive it by a moment. Keep
            // retrying a connection while the saved setup remains enabled.
            let mut wait = Duration::from_millis(250);
            loop {
                if host.shutdown.is_cancelled() {
                    return;
                }
                match host.connect_inner(owner.clone(), true).await {
                    Ok(None) => return,
                    Ok(Some(_)) => wait = Duration::from_secs(5),
                    Err(error) => {
                        *lock(&host.last_error) = Some(error);
                        wait = (wait * 2).min(Duration::from_secs(5));
                    }
                }
                tokio::select! {
                    _ = host.shutdown.cancelled() => return,
                    _ = tokio::time::sleep(wait) => {}
                }
            }
        });
    }

    async fn load_settings(&self) -> Result<Option<hermes_telegram::Settings>, String> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || store.load_settings())
            .await
            .map_err(|_| "Could not load saved Telegram settings".to_owned())?
    }

    pub(crate) async fn status(&self) -> Result<Value, String> {
        let settings = self.load_settings().await?;
        let (connected, bot) = {
            let active = self.active.lock().await;
            (
                active.as_ref().is_some_and(|a| !a.task.is_finished()),
                active
                    .as_ref()
                    .map(|a| a.bot.clone())
                    .or_else(|| settings.as_ref().and_then(|s| s.bot.clone())),
            )
        };
        let work = if let Some(bot) = bot.clone() {
            let store = self.store.clone();
            tokio::task::spawn_blocking(move || store.work_status(&bot))
                .await
                .map_err(|_| "Could not read Telegram work status".to_owned())??
        } else {
            json!({"counts":{},"conflicts":0,"issues":[]})
        };
        Ok(json!({
            "enabled": settings.as_ref().is_some_and(|s| s.enabled),
            "agent_replies": settings.as_ref().is_some_and(|s| s.agent_replies),
            "connected": connected,
            "bot": bot,
            "work": work,
            "last_error": lock(&self.last_error).clone(),
        }))
    }

    pub(super) async fn connect(&self, owner: Option<Weak<Shared>>) -> Result<Value, String> {
        self.connect_inner(owner, false)
            .await?
            .ok_or_else(|| "Save a Telegram bot token and allowlist before connecting".to_owned())
    }

    async fn connect_inner(
        &self,
        owner: Option<Weak<Shared>>,
        restoring: bool,
    ) -> Result<Option<Value>, String> {
        let mut active = self.active.lock().await;
        if self.shutdown.is_cancelled() {
            return Ok(None);
        }
        let Some(settings) = self.load_settings().await? else {
            return Ok(None);
        };
        if restoring && !settings.enabled {
            return Ok(None);
        }
        if let Some(current) = active.as_ref().filter(|a| !a.task.is_finished())
            && settings.enabled
            && settings.bot.as_ref().map(|saved| saved.id) == Some(current.bot.id)
            && settings.token.as_ref() == Some(&current.token)
        {
            return Ok(Some(
                json!({"connected":true,"enabled":true,"bot":current.bot}),
            ));
        }
        if let Some(previous) = active.take() {
            previous.cancel.cancel();
            let _ = previous.task.await;
        }
        if settings.allowed.is_empty()
            || settings
                .allowed
                .iter()
                .any(|id| !(1..=4_503_599_627_370_495).contains(id))
        {
            return Err(
                "Save a non-empty allowlist of numeric Telegram user IDs before connecting".into(),
            );
        }
        let token = settings
            .token
            .ok_or("Save a Telegram bot token before connecting")?;
        let client = self.client(token.clone())?;
        let bot = client
            .verify(&self.shutdown)
            .await
            .map_err(|e| e.to_string())?;
        if settings
            .bot
            .as_ref()
            .is_some_and(|saved| saved.id != bot.id)
        {
            return Err(
                "The saved Telegram bot identity changed; review the setup before reconnecting"
                    .into(),
            );
        }
        let store = self.store.clone();
        let credential = token.clone();
        let bot_for_store = bot.clone();
        let lease = tokio::task::spawn_blocking(move || {
            let lease = store.claim(bot_for_store.id)?;
            store.recover_sends(&bot_for_store)?;
            store.remember_bot(&bot_for_store, &credential, restoring)?;
            Ok::<_, String>(lease)
        })
        .await
        .map_err(|_| "Could not open the native Telegram inbox".to_owned())??;

        let cancel = self.shutdown.child_token();
        let connector = Arc::new(Connector {
            store: self.store.clone(),
            client,
            bot: bot.clone(),
            cancel: cancel.clone(),
        });
        let last_error = self.last_error.clone();
        *lock(&last_error) = None;
        let task = tokio::spawn(async move {
            let _lease = lease;
            let result = connector
                .run(owner.filter(|_| settings.agent_replies))
                .await;
            if let Err(error) = result {
                *lock(&last_error) = Some(error);
            }
        });
        *active = Some(Active {
            bot: bot.clone(),
            token,
            cancel,
            task,
        });
        Ok(Some(json!({"connected":true,"enabled":true,"bot":bot})))
    }

    pub(crate) async fn disconnect(&self) -> Result<Value, String> {
        let mut active = self.active.lock().await;
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || store.set_disabled())
            .await
            .map_err(|_| "Could not save Telegram disconnect state".to_owned())??;
        if let Some(connection) = active.take() {
            connection.cancel.cancel();
            connection
                .task
                .await
                .map_err(|_| "Telegram connector stopped unexpectedly".to_owned())?;
        }
        Ok(json!({"connected":false,"enabled":false}))
    }

    pub(crate) async fn send(&self, user_id: i64, text: String) -> Result<Value, String> {
        let active = self.active.lock().await;
        let connection = active
            .as_ref()
            .filter(|a| !a.task.is_finished())
            .ok_or_else(|| "Telegram is disconnected; enable it before replying.".to_owned())?;
        let store = self.store.clone();
        let client = self.client(connection.token.clone())?;
        let bot = connection.bot.clone();
        let credential = client.credential().clone();
        let policy_store = store.clone();
        let policy_bot = bot.clone();
        tokio::task::spawn_blocking(move || {
            policy_store.authorize_reply(&policy_bot, &credential, user_id)
        })
        .await
        .map_err(|_| "Could not check the saved Telegram allowlist".to_owned())??;
        let receipt = client
            .send(user_id, &text, &connection.cancel)
            .await
            .map_err(|e| e.to_string())?;
        let message_id = receipt.message_id;
        tokio::task::spawn_blocking(move || store.record_sent(&bot, user_id, text, receipt))
            .await
            .map_err(|_| "Reply delivered, but the native inbox could not be saved".to_owned())??;
        Ok(json!({"sent":true,"message_id":message_id}))
    }

    pub(super) async fn resolve_delivery(
        &self,
        bot_id: i64,
        update_id: i64,
        attempt: i64,
        resolution: DeliveryResolution,
    ) -> Result<bool, String> {
        let bot = self
            .load_settings()
            .await?
            .and_then(|settings| settings.bot)
            .filter(|bot| bot.id == bot_id)
            .ok_or_else(|| "This bot does not match the saved Telegram connection".to_owned())?;
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            store.resolve_delivery(&bot, update_id, attempt, resolution)
        })
        .await
        .map_err(|_| "Could not resolve the Telegram delivery".to_owned())?
    }

    pub(crate) async fn shutdown(&self) {
        let mut active = self.active.lock().await;
        if let Some(connection) = active.take() {
            connection.cancel.cancel();
            let _ = connection.task.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hermes_telegram::{Method, RequestFuture};
    use serde_json::{Value, json};
    use std::{
        collections::{BTreeSet, VecDeque},
        sync::Mutex as StdMutex,
    };

    struct Fake {
        first_updates: StdMutex<VecDeque<Value>>,
        calls: StdMutex<Vec<String>>,
    }

    impl Fake {
        fn new(updates: Vec<Value>) -> Arc<Self> {
            Arc::new(Self {
                first_updates: StdMutex::new(updates.into()),
                calls: StdMutex::new(Vec::new()),
            })
        }

        fn call_count(&self, name: &str) -> usize {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|call| call.as_str() == name)
                .count()
        }
    }

    impl Transport for Fake {
        fn request<'a>(
            &'a self,
            _token: &'a Token,
            method: Method,
            body: Value,
        ) -> RequestFuture<'a> {
            let name = match method {
                Method::GetMe => "getMe",
                Method::GetWebhookInfo => "getWebhookInfo",
                Method::GetUpdates => "getUpdates",
                Method::SendMessage => "sendMessage",
                other => panic!("Unexpected method in the prior Telegram fixture: {other:?}"),
            };
            self.calls.lock().unwrap().push(name.to_owned());
            let response = match method {
                Method::GetMe => Ok(
                    json!({"id":123456,"is_bot":true,"username":"synthetic_bot","first_name":"Synthetic"}),
                ),
                Method::GetWebhookInfo => Ok(json!({"url":""})),
                Method::GetUpdates => {
                    if body["offset"].as_i64().unwrap_or_default() > 0 {
                        Ok(json!([]))
                    } else {
                        Ok(self
                            .first_updates
                            .lock()
                            .unwrap()
                            .pop_front()
                            .unwrap_or_else(|| json!([])))
                    }
                }
                Method::SendMessage => {
                    Ok(json!({"message_id":77,"date":1234,"chat":{"id":body["chat_id"]}}))
                }
                other => panic!("Unexpected method in the prior Telegram fixture: {other:?}"),
            };
            let pause = matches!(method, Method::GetUpdates);
            Box::pin(async move {
                if pause {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                response
            })
        }
    }

    fn private_update(update_id: i64, user_id: i64, chat_id: i64, text: &str) -> Value {
        json!({"update_id":update_id,"message":{
            "message_id":update_id,"date":1234,
            "from":{"id":user_id,"is_bot":false,"username":"tester"},
            "chat":{"id":chat_id,"type":"private"},"text":text
        }})
    }

    fn saved_settings(root: &Path) -> Store {
        let store = Store::new(root);
        store
            .save_settings(&hermes_telegram::Settings {
                version: 1,
                token: Some(Token::parse("123456:abcdefghijklmnopqrstuvwxyz1234567890").unwrap()),
                allowed: BTreeSet::from([42]),
                bot: None,
                enabled: false,
                agent_replies: false,
            })
            .unwrap();
        store
    }

    async fn wait_for_inbox(store: &Store, bot: &Identity, cursor: i64) -> hermes_telegram::Inbox {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let store = store.clone();
                let bot = bot.clone();
                let inbox = tokio::task::spawn_blocking(move || store.load_inbox(&bot))
                    .await
                    .unwrap()
                    .unwrap();
                if inbox.cursor >= cursor {
                    return (*inbox).clone();
                }
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn gateway_owns_polling_and_persists_explicit_disconnect_across_restart() {
        let root = tempfile::tempdir().unwrap();
        let store = saved_settings(root.path());
        let update = private_update(10, 42, 42, "hello from allowed user");
        let unallowed = private_update(11, 99, 99, "should be ignored");
        let group = private_update(12, 42, -10042, "group should be ignored");
        let fake = Fake::new(vec![json!([update, unallowed, group])]);
        let host = Host::with_transport(root.path(), CancellationToken::new(), fake.clone());

        let connected = host.connect(None).await.unwrap();
        assert_eq!(connected["enabled"], true);
        assert_eq!(connected["bot"]["id"], 123456);
        let bot: Identity = serde_json::from_value(connected["bot"].clone()).unwrap();
        let inbox = wait_for_inbox(&store, &bot, 13).await;
        assert_eq!(inbox.cursor, 13);
        assert_eq!(inbox.messages.len(), 1);
        assert_eq!(inbox.messages[0].text, "hello from allowed user");
        assert!(fake.call_count("getUpdates") > 0);

        assert!(host.send(99, "blocked".into()).await.is_err());
        host.send(42, "manual reply".into()).await.unwrap();
        let inbox = wait_for_inbox(&store, &bot, 13).await;
        assert_eq!(inbox.messages.len(), 2);
        assert!(inbox.messages[1].outgoing);
        assert_eq!(inbox.messages[1].text, "manual reply");

        host.shutdown().await;
        assert!(store.load_settings().unwrap().unwrap().enabled);
        let resumed_fake = Fake::new(vec![]);
        let resumed =
            Host::with_transport(root.path(), CancellationToken::new(), resumed_fake.clone());
        resumed.restore(None);
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if resumed_fake.call_count("getUpdates") > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        })
        .await
        .unwrap();
        let state = resumed.status().await.unwrap();
        assert_eq!(state["enabled"], true);
        assert_eq!(state["connected"], true);
        assert_eq!(resumed_fake.call_count("getMe"), 1);

        resumed.disconnect().await.unwrap();
        let settings = store.load_settings().unwrap().unwrap();
        assert!(!settings.enabled);
        let state = resumed.status().await.unwrap();
        assert_eq!(state["connected"], false);

        let disconnected_fake = Fake::new(vec![]);
        let after_disconnect = Host::with_transport(
            root.path(),
            CancellationToken::new(),
            disconnected_fake.clone(),
        );
        after_disconnect.restore(None);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(disconnected_fake.call_count("getMe"), 0);
        assert_eq!(disconnected_fake.call_count("getUpdates"), 0);
    }
}
