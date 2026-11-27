//! Native Telegram settings, connection ownership, and per-bot inbox storage.
mod inbox;

pub use inbox::{DeliveryOutcome, DeliveryResolution, TurnOutcome, Work, WorkStage};
use super::telegram::{Identity, Token};
use fs2::FileExt;
use hermes_engine::secret;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

const MAX_INBOX_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
pub struct Settings {
    pub version: u32,
    pub token: Option<Token>,
    pub allowed: BTreeSet<i64>,
    pub bot: Option<Identity>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub agent_replies: bool,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub update_id: Option<i64>,
    pub message_id: i64,
    pub user_id: i64,
    pub sender: String,
    pub text: String,
    pub timestamp: i64,
    pub outgoing: bool,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Inbox {
    pub version: u32,
    pub bot: Identity,
    pub cursor: i64,
    #[serde(default)]
    pub revision: u64,
    pub messages: Vec<ChatMessage>,
}
#[derive(Clone)]
pub struct Store {
    profile: PathBuf,
    root: PathBuf,
}
impl Store {
    pub fn new(library_root: &Path) -> Self {
        Self {
            profile: library_root.to_owned(),
            root: library_root.join("messaging").join("telegram"),
        }
    }
    fn ensure_directory(&self) -> Result<(), String> {
        for path in [self.root.parent().unwrap(), self.root.as_path()] {
            if let Ok(meta) = fs::symlink_metadata(path)
                && (meta.file_type().is_symlink() || !meta.is_dir())
            {
                return Err("Native Telegram storage must be a regular directory".into());
            }
            fs::create_dir_all(path).map_err(|error| error.to_string())?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                    .map_err(|error| error.to_string())?;
            }
        }
        Ok(())
    }
    fn lock(&self, name: &str) -> Result<File, String> {
        self.ensure_directory()?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.root.join(name))
            .map_err(|error| error.to_string())?;
        file.lock_exclusive().map_err(|error| error.to_string())?;
        Ok(file)
    }
    pub fn load_settings(&self) -> Result<Option<Settings>, String> {
        let path = self.root.join("settings.json");
        if !path.exists() {
            return Ok(None);
        }
        let _lock = self.lock("settings.lock")?;
        let mut saved = self.read_saved()?;
        let settings = Self::decode_settings(saved.clone())?;
        // Older versions could leave the token in plaintext.
        if let Some(token) = saved["token"].as_str().filter(|token| !secret::is_sealed(token))
            && let Ok(sealed) = secret::seal(&self.root, token)
            && secret::is_sealed(&sealed)
        {
            saved["token"] = sealed.into();
            let _ = atomic_json(&path, &saved);
        }
        Ok(Some(settings))
    }
    pub fn save_settings(&self, settings: &Settings) -> Result<(), String> {
        let _lock = self.lock("settings.lock")?;
        let mut saved = serde_json::to_value(settings).map_err(|error| error.to_string())?;
        if let Some(token) = saved["token"].as_str() {
            saved["token"] = secret::seal(&self.root, token)
                .map_err(|error| format!("{error:#}"))?
                .into();
        }
        atomic_json(&self.root.join("settings.json"), &saved)
    }
    fn read_saved(&self) -> Result<Value, String> {
        let saved: Value = private_json(&self.root.join("settings.json"), 1024 * 1024)?;
        if !saved.is_object() {
            return Err("Could not read native Telegram storage".into());
        }
        Ok(saved)
    }
    fn decode_settings(mut saved: Value) -> Result<Settings, String> {
        if let Some(token) = saved["token"].as_str() {
            saved["token"] = secret::unseal(token)
                .map_err(|error| format!("{error:#}"))?
                .into();
        }
        let settings: Settings = serde_json::from_value(saved)
            .map_err(|_| "Could not read native Telegram storage".to_owned())?;
        if settings.version != 1 {
            return Err("Unsupported native Telegram settings version".into());
        }
        Ok(settings)
    }
    pub fn clear_token(&self) -> Result<Option<Settings>, String> {
        let _lock = self.lock("settings.lock")?;
        let path = self.root.join("settings.json");
        if !path.exists() {
            return Ok(None);
        }
        // Removing a token must work even when it cannot be opened.
        let mut saved = self.read_saved()?;
        saved["token"] = Value::Null;
        saved["bot"] = Value::Null;
        saved["enabled"] = Value::Bool(false);
        atomic_json(&path, &saved)?;
        serde_json::from_value(saved).map(Some).map_err(|_| "Could not read native Telegram storage".into())
    }
    pub fn set_disabled(&self) -> Result<(), String> {
        let _lock = self.lock("settings.lock")?;
        let path = self.root.join("settings.json");
        if !path.exists() {
            return Ok(());
        }
        let mut saved = self.read_saved()?;
        saved["enabled"] = Value::Bool(false);
        atomic_json(&path, &saved)
    }
    pub fn remember_bot(&self, identity: &Identity, token: &Token, require_enabled: bool) -> Result<(), String> {
        let _lock = self.lock("settings.lock")?;
        let mut saved = self.read_saved()?;
        let settings = Self::decode_settings(saved.clone())?;
        if settings.token.as_ref() != Some(token) || (require_enabled && !settings.enabled) {
            return Err(
                "Telegram setup changed. Reopen Messaging and enable the saved setup.".into(),
            );
        }
        if settings
            .bot
            .as_ref()
            .is_some_and(|saved| saved.id != identity.id)
        {
            return Err(
                "Telegram bot identity changed; review the saved setup before reconnecting".into(),
            );
        }
        // A connection change must keep the on-disk token as saved.
        saved["bot"] = serde_json::to_value(identity).map_err(|error| error.to_string())?;
        saved["enabled"] = Value::Bool(true);
        atomic_json(&self.root.join("settings.json"), &saved)
    }
    fn connection_settings(&self, bot: &Identity, token: &Token) -> Result<Settings, String> {
        let settings = Self::decode_settings(self.read_saved()?)?;
        if settings.token.as_ref() != Some(token)
            || !settings.enabled
            || settings.bot.as_ref().map(|identity| identity.id) != Some(bot.id)
        {
            return Err(
                "Telegram setup changed. Disable and reopen Messaging before reconnecting.".into(),
            );
        }
        Ok(settings)
    }
    pub fn authorize_reply(
        &self,
        bot: &Identity,
        token: &Token,
        user_id: i64,
    ) -> Result<(), String> {
        let _lock = self.lock("settings.lock")?;
        let settings = self.connection_settings(bot, token)?;
        if settings.agent_replies {
            return Err("This Telegram connector is configured for agent replies".into());
        }
        if !settings.allowed.contains(&user_id) {
            return Err("This Telegram user is no longer on the saved allowlist".into());
        }
        Ok(())
    }
    pub fn claim(&self, bot_id: i64) -> Result<Arc<File>, String> {
        if bot_id <= 0 {
            return Err("Telegram bot identity is invalid".into());
        }
        self.ensure_directory()?;
        let installation = self.profile.parent()
            .filter(|parent| parent.file_name().is_some_and(|name| name == "profiles"))
            .and_then(Path::parent)
            .unwrap_or(&self.profile);
        let leases = Store::new(installation);
        leases.ensure_directory()?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(leases.root.join(format!("bot-{bot_id}.connection.lock")))
            .map_err(|error| error.to_string())?;
        file.try_lock_exclusive().map_err(|_| {
            "This Telegram bot is already connected to another profile in this installation".to_owned()
        })?;
        Ok(Arc::new(file))
    }

    /// Detect records that need a cutover before a different runtime can
    /// acknowledge updates for this bot. Check while holding the account lease.
    pub fn has_native_state(&self, bot_id: i64) -> Result<bool, String> {
        if self.root.join(format!("bot-{bot_id}.inbox.json")).try_exists().map_err(|e| e.to_string())? {
            return Ok(true);
        }
        if self.root.join("settings.json").try_exists().map_err(|e| e.to_string())? {
            let _lock = self.lock("settings.lock")?;
            let settings = self.read_saved()?;
            let saved_bot = settings.pointer("/bot/id").and_then(Value::as_i64);
            if saved_bot == Some(bot_id) || (saved_bot.is_none() && !settings["token"].is_null()) {
                return Ok(true);
            }
        }
        self.has_native_inbox(bot_id)
    }
    /// Check current policy while taking a durable step toward execution or delivery.
    pub fn with_agent_policy<T>(
        &self,
        bot: &Identity,
        token: &Token,
        user_id: i64,
        action: impl FnOnce() -> Result<T, String>,
    ) -> Result<Option<T>, String> {
        let _lock = self.lock("settings.lock")?;
        let settings = self.connection_settings(bot, token)?;
        if settings.agent_replies && settings.allowed.contains(&user_id) {
            return action().map(Some);
        }
        Ok(None)
    }
}

fn private_json<T: serde::de::DeserializeOwned>(path: &Path, max: u64) -> Result<T, String> {
    let meta = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if !meta.is_file() || meta.file_type().is_symlink() || meta.len() > max {
        return Err("Native Telegram storage is not a regular, bounded file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o077 != 0 {
            return Err(
                "Native Telegram credentials and inbox files must be private to your user".into(),
            );
        }
    }
    let file = File::open(path).map_err(|error| error.to_string())?;
    serde_json::from_reader(file.take(max + 1))
        .map_err(|_| "Could not read native Telegram storage".to_owned())
}
fn atomic_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    atomic_bytes(
        path,
        &serde_json::to_vec(value).map_err(|error| error.to_string())?,
    )
}
fn atomic_bytes(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or("Native Telegram storage has no parent")?;
    let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|error| error.to_string())?;
    temp.write_all(bytes)
        .and_then(|_| temp.write_all(b"\n"))
        .and_then(|_| temp.as_file().sync_all())
        .map_err(|error| error.to_string())?;
    temp.persist(path)
        .map_err(|_| "Could not replace native Telegram storage".to_owned())?;
    #[cfg(unix)]
    File::open(parent)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "uses the real system credential store with a temporary profile"]
    fn managed_gateway_preserves_saved_ciphertext() {
        const CHILD: &str = "HERMES_TELEGRAM_CREDENTIAL_TEST_CHILD";
        let data_dir = hermes_engine::Session::default_dir().unwrap();
        let token = Token::parse("123456:abcdefghijklmnopqrstuvwxyz1234567890").unwrap();

        if std::env::var_os(CHILD).is_some() {
            let store = Store::new(&data_dir);
            assert_eq!(store.load_settings().unwrap().unwrap().token.as_ref(), Some(&token));
            let bot = Identity {
                id: 123456,
                username: "synthetic_bot".into(),
                name: "Synthetic".into(),
            };
            let before = store.read_saved().unwrap();
            assert!(before["token"].as_str().is_some_and(secret::is_sealed));
            for _ in 0..2 {
                store.remember_bot(&bot, &token, false).unwrap();
                let saved = store.read_saved().unwrap();
                assert_eq!(saved["token"], before["token"]);
                assert_eq!(saved["allowed"], before["allowed"]);
                assert_eq!(saved["enabled"], true);
                assert_eq!(saved["bot"]["id"], bot.id);
            }
            return;
        }

        fs::create_dir_all(&data_dir).unwrap();
        let profile = tempfile::tempdir_in(data_dir).unwrap();
        let store = Store::new(profile.path());
        let settings = Settings {
            version: 1,
            token: Some(token),
            allowed: BTreeSet::from([42]),
            bot: None,
            enabled: false,
            agent_replies: false,
        };
        let run_gateway = || {
            let child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--ignored", "--exact", "store::tests::managed_gateway_preserves_saved_ciphertext"])
                .env("HERMES_RUST_DATA_DIR", profile.path())
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                child.status.success(),
                "{}{}",
                String::from_utf8_lossy(&child.stdout),
                String::from_utf8_lossy(&child.stderr),
            );
        };

        store.save_settings(&settings).unwrap();
        let before = store.read_saved().unwrap();
        assert!(before["token"].as_str().is_some_and(secret::is_sealed));
        run_gateway();
        assert_eq!(store.read_saved().unwrap()["token"], before["token"]);

        // Simulate a token left in plaintext by an older gateway.
        atomic_json(&store.root.join("settings.json"), &settings).unwrap();
        run_gateway();
        let migrated = store.read_saved().unwrap();
        assert!(migrated["token"].as_str().is_some_and(secret::is_sealed));
        assert_eq!(migrated["enabled"], true);
    }
}
