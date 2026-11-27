//! Transaction operations for bundled Rust adapters with durable protocol state.
//! A plugin uses these operations under its connection lease to commit its
//! protocol state together with the gateway's cursor or delivery receipt.

use super::{Advance, IntakeResult, intake::Staged, store::Store};
use anyhow::{Result, ensure};
use rusqlite::{Connection, Transaction};

pub use super::store::{
    Action, Candidate, Route, check_advance, decode, enqueue, ingress_replay, install_plan,
    intake_receipt, key, optional_decode, read_route, write_advance,
};

pub struct AdapterStorage<'a> {
    store: &'a Store,
    connection: &'a str,
}

impl<'a> AdapterStorage<'a> {
    pub(super) fn new(store: &'a Store, connection: &'a str) -> Self {
        Self { store, connection }
    }

    pub fn read<T>(&self, read: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        self.store.read(read)
    }

    /// Commit within an operation that has checked the current connection lease.
    pub fn write<T>(&self, change: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        self.store.write(change)
    }

    pub fn intake(
        &self,
        revision: i64,
        candidates: Vec<Candidate>,
        advance: Option<&Advance>,
    ) -> Result<Option<IntakeResult>> {
        self.store
            .intake(self.connection, revision, candidates, advance)
    }

    pub fn preparation(&self, event: &str) -> Result<Option<Staged>> {
        self.store.preparation(self.connection, event)
    }

    pub fn route(&self, id: &str) -> Result<Option<Route>> {
        Ok(self
            .store
            .route(id)?
            .filter(|route| route.connection == self.connection))
    }

    pub fn action(&self, route: &Route, id: &str) -> Result<Option<Action>> {
        ensure!(
            route.connection == self.connection,
            "Messaging connection does not own this route"
        );
        self.store.action(route, id)
    }

    #[cfg(feature = "test-support")]
    pub fn put_attachment(&self, name: String, bytes: &[u8]) -> Result<super::AttachmentRef> {
        self.store.put_attachment(self.connection, name, bytes)
    }

    #[cfg(feature = "test-support")]
    pub fn enqueue(&self, route: &Route, intent: &super::DeliveryIntent) -> Result<()> {
        ensure!(
            route.connection == self.connection,
            "Messaging connection does not own this route"
        );
        self.store.enqueue(route, intent)
    }
}
