use super::{AccessPolicy, AdapterFactory, ConnectionSettings, store::Store};
use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;
use std::{path::Path, sync::Arc};

/// Registration for a stock adapter linked into the gateway executable.
#[derive(Clone, Copy)]
pub struct StockAdapter {
    pub platform: &'static str,
    pub schema: &'static [&'static str],
    pub configure: fn(&Path, Value) -> Result<StockConfiguration>,
    pub replace_credential: fn(&Value, &Value) -> bool,
}

pub struct StockConfiguration {
    pub settings: Value,
    pub policy: AccessPolicy,
    pub factory: Arc<dyn AdapterFactory>,
}

#[derive(Serialize)]
pub(crate) struct StockConnection {
    #[serde(flatten)]
    pub connection: ConnectionSettings,
    pub settings: Value,
}

impl StockAdapter {
    pub(super) fn validate(self) -> Result<()> {
        ensure!(
            !self.platform.is_empty()
                && self.platform.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || byte == b'_'
                        || byte == b'-'
                }),
            "Invalid stock messaging platform",
        );
        Ok(())
    }

    fn settings_table(self) -> String {
        format!("\"messaging_{}_settings\"", self.platform)
    }
}

impl Store {
    pub(super) fn install_stock(&self, adapter: StockAdapter) -> Result<()> {
        self.write(|tx| {
            tx.execute_batch(&format!(
                "CREATE TABLE IF NOT EXISTS {} (
                    name TEXT PRIMARY KEY REFERENCES messaging_configuration(name),
                    settings TEXT NOT NULL
                );",
                adapter.settings_table(),
            ))?;
            for schema in adapter.schema {
                tx.execute_batch(schema)?;
            }
            Ok(())
        })
    }

    pub(super) fn stock_settings(
        &self,
        adapter: StockAdapter,
        name: &str,
    ) -> Result<Option<Value>> {
        self.read(|db| {
            Ok(db
                .query_row(
                    &format!(
                        "SELECT settings FROM {} WHERE name = ?1",
                        adapter.settings_table()
                    ),
                    [name],
                    |row| super::store::decode(row, 0),
                )
                .optional()?)
        })
    }

    pub(super) fn stock_connections(&self, adapter: StockAdapter) -> Result<Vec<(String, Value)>> {
        self.read(|db| {
            let mut query = db.prepare(&format!(
                "SELECT name, settings FROM {} ORDER BY name",
                adapter.settings_table(),
            ))?;
            Ok(query
                .query_map([], |row| Ok((row.get(0)?, super::store::decode(row, 1)?)))?
                .collect::<rusqlite::Result<_>>()?)
        })
    }

    pub(super) fn save_stock(
        &self,
        adapter: StockAdapter,
        name: &str,
        revision: Option<u64>,
        enabled: bool,
        configuration: &StockConfiguration,
    ) -> Result<()> {
        let table = adapter.settings_table();
        let policy = serde_json::to_string(&configuration.policy)?;
        let settings = serde_json::to_string(&configuration.settings)?;
        self.write(|tx| {
            if let Some(revision) = revision {
                let revision = i64::try_from(revision)?;
                let (previous, credential): (Value, Option<String>) = tx.query_row(
                    &format!("SELECT s.settings, c.credential_ref FROM {table} s
                        JOIN messaging_configuration c USING(name)
                        WHERE s.name = ?1 AND c.platform = ?2 AND c.revision = ?3"),
                    params![name, adapter.platform, revision],
                    |row| Ok((super::store::decode(row, 0)?, row.get(1)?)),
                ).optional()?.context("Messaging connection settings changed or do not exist")?;
                let replace_credential = (adapter.replace_credential)(&previous, &configuration.settings);
                tx.execute(
                    "UPDATE messaging_configuration SET policy = ?2, revision = revision + 1,
                        credential_ref = CASE WHEN ?3 THEN NULL ELSE credential_ref END,
                        binding = CASE WHEN ?3 THEN NULL ELSE binding END WHERE name = ?1",
                    params![name, policy, replace_credential],
                )?;
                tx.execute(&format!("UPDATE {table} SET settings = ?2 WHERE name = ?1"), params![name, settings])?;
                if replace_credential && let Some(credential) = credential {
                    tx.execute("DELETE FROM messaging_connection_secrets WHERE reference = ?1", [credential])?;
                }
            } else {
                ensure!(
                    tx.execute(
                        "INSERT OR IGNORE INTO messaging_configuration (name, platform, enabled, revision, policy)
                         VALUES (?1, ?2, ?3, 0, ?4)",
                        params![name, adapter.platform, enabled, policy],
                    )? == 1,
                    "A messaging connection already uses that name"
                );
                tx.execute(&format!("INSERT INTO {table} (name, settings) VALUES (?1, ?2)"), params![name, settings])?;
            }
            Ok(())
        })
    }
}
