use super::{
    policy::ResourcePolicy,
    runtime::{Ingress, blocking, db, valid_id},
    store::{self, Candidate, Route, Store, decode, key},
    types::{
        AccessPolicy, Advance, Disposition, GatewayError, InboundBody, InboundEvent, IntakeReceipt,
        Room, RoomKind, Sender,
    },
};
use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[path = "actor_refresh.rs"]
mod actor_refresh;

pub(super) const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS messaging_configuration (
    name TEXT PRIMARY KEY, platform TEXT NOT NULL, enabled INTEGER NOT NULL,
    revision INTEGER NOT NULL, policy TEXT NOT NULL, credential_ref TEXT, binding TEXT
);
CREATE TABLE IF NOT EXISTS messaging_connection_secrets (
    reference TEXT PRIMARY KEY, value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messaging_enrollment (
    name TEXT PRIMARY KEY REFERENCES messaging_configuration(name),
    lease TEXT NOT NULL, revision INTEGER NOT NULL, expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messaging_actor_evidence (
    connection TEXT NOT NULL, actor TEXT NOT NULL, data TEXT NOT NULL, revision INTEGER NOT NULL,
    PRIMARY KEY(connection, actor)
);
CREATE TABLE IF NOT EXISTS messaging_resource_evidence (
    connection TEXT NOT NULL, event TEXT NOT NULL, evidence TEXT NOT NULL,
    PRIMARY KEY(connection, event)
);
CREATE TABLE IF NOT EXISTS messaging_resource_delivery (
    delivery TEXT PRIMARY KEY REFERENCES messaging_deliveries(id), connection TEXT NOT NULL,
    event TEXT NOT NULL,
    FOREIGN KEY(connection,event) REFERENCES messaging_resource_evidence(connection,event)
);
CREATE TABLE IF NOT EXISTS messaging_actor_aliases (
    connection TEXT NOT NULL, alias TEXT NOT NULL, actor TEXT NOT NULL,
    PRIMARY KEY(connection, alias)
);
CREATE TABLE IF NOT EXISTS messaging_pairing (
    connection TEXT NOT NULL, challenge TEXT NOT NULL, actor TEXT NOT NULL,
    room TEXT NOT NULL, expires_at INTEGER NOT NULL, authorized INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(connection, challenge)
);
";
pub(super) const MAX_LEASE_MS: i64 = 15 * 60 * 1000;
const MAX_SECRET: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalClientIdentity {
    pub service: String,
    pub store_incarnation: String,
}

/// Provider-verified installation and transport identity. None of these fields
/// contains credentials. A device renewal changes the transport scope without
/// changing the logical account; replacing a local store changes its identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VerifiedInstallation {
    pub installation: String,
    pub tenant: Option<String>,
    pub application: Option<String>,
    pub receiver: Option<String>,
    pub origin: Option<String>,
    pub device: Option<String>,
    pub local_client: Option<LocalClientIdentity>,
}

impl VerifiedInstallation {
    pub(super) fn validate(&self) -> bool {
        valid_id(&self.installation)
            && [
                &self.tenant,
                &self.application,
                &self.receiver,
                &self.device,
            ]
            .into_iter()
            .flatten()
            .all(|id| valid_id(id))
            && self.origin.as_ref().is_none_or(|origin| {
                reqwest::Url::parse(origin).is_ok_and(|url| {
                    url.origin().ascii_serialization() == *origin && url.scheme() == "https"
                })
            })
            && self
                .local_client
                .as_ref()
                .is_none_or(|local| valid_id(&local.service) && valid_id(&local.store_incarnation))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct AccountBinding {
    pub(super) account: String,
    pub(super) tenant: Option<String>,
    pub(super) application: Option<String>,
    pub(super) installation: Option<String>,
    pub(super) local_client: Option<LocalClientIdentity>,
}

impl AccountBinding {
    pub(super) fn new(account: String, proof: Option<&VerifiedInstallation>) -> Self {
        Self {
            account,
            tenant: proof.and_then(|p| p.tenant.clone()),
            application: proof.and_then(|p| p.application.clone()),
            installation: proof.map(|p| p.installation.clone()),
            local_client: proof.and_then(|p| p.local_client.clone()),
        }
    }

    pub(super) fn key(&self, platform: &str) -> Result<String> {
        if self.installation.is_none() {
            // The published core account namespace is retained for persisted routes.
            key("connection", &(platform, &self.account))
        } else {
            key("connection", &(platform, self))
        }
    }
}

/// An owner-approved credential, available only to the factory that's verifying
/// the specified connection. Omit it from diagnostics and transport metadata.
#[derive(Clone)]
pub struct EnrollmentCredential(String);

impl EnrollmentCredential {
    pub fn new(secret: String) -> Result<Self, GatewayError> {
        if secret.trim().is_empty() || secret.len() > MAX_SECRET || secret.contains('\0') {
            return Err(GatewayError::Invalid);
        }
        Ok(Self(secret))
    }

    pub fn secret(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ConnectionSettings {
    pub name: String,
    pub platform: String,
    pub enabled: bool,
    pub revision: u64,
    pub policy: AccessPolicy,
    pub credential_ref: Option<String>,
    #[serde(skip)]
    pub(super) binding: Option<AccountBinding>,
}

/// A provider challenge returned only to the private owner while its setup
/// lease is current. Its text is intentionally absent from diagnostic output.
#[derive(Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EnrollmentPrompt {
    Url { url: String },
    PairingCode { code: String },
    Qr { payload: String },
}

impl EnrollmentPrompt {
    pub(super) fn validate(&self) -> bool {
        match self {
            Self::Url { url } => {
                url.len() <= 8192
                    && reqwest::Url::parse(url).is_ok_and(|url| {
                        url.scheme() == "https"
                            && url.username().is_empty()
                            && url.password().is_none()
                    })
            }
            Self::PairingCode { code } => valid_id(code),
            Self::Qr { payload } => {
                !payload.trim().is_empty() && payload.len() <= 8192 && !payload.contains('\0')
            }
        }
    }
}

#[derive(Clone, Serialize)]
pub struct EnrollmentLease {
    pub id: String,
    pub expires_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<EnrollmentPrompt>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    Person,
    Shared,
    Connection,
}

/// One provider namespace. The resulting ID can be used as an event sender.
/// Namespace boundaries remain significant when providers reuse displayed IDs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActorAlias {
    pub namespace: String,
    pub id: String,
}

impl ActorAlias {
    pub fn sender_id(&self) -> Result<String, GatewayError> {
        if !valid_id(&self.namespace) || !valid_id(&self.id) {
            return Err(GatewayError::Invalid);
        }
        db(key("actor-alias", self))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemberEvidence {
    pub room: Room,
    pub roles: BTreeSet<String>,
}

/// Revisions are adapter-owned monotonic observations for one canonical actor in
/// one verified installation. Absence from a newer snapshot withdraws membership.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActorEvidence {
    pub actor: String,
    pub kind: ActorKind,
    pub revision: u64,
    pub expires_at: i64,
    pub aliases: Vec<ActorAlias>,
    pub memberships: Vec<MemberEvidence>,
}

#[derive(Eq, PartialEq, Serialize, Deserialize)]
struct StoredActor {
    transport_scope: String,
    evidence: ActorEvidence,
}

pub(super) enum ActorObservation {
    Missing,
    Stale,
    Verified(ActorEvidence),
}

/// Scope of an authenticated provider event outside a chat stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceSource {
    pub operation: String,
    pub resource: String,
}

const RESOURCE_EVENT: &str = "resource-event";

pub(super) fn resource_event_id(value: &str) -> bool {
    value.starts_with("resource-event-")
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
struct ResourceEvidence {
    source: ResourceSource,
    provider_event: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PairingChallenge {
    pub code: String,
    pub expires_at: i64,
}

pub(super) fn time_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn settings(row: &rusqlite::Row<'_>) -> rusqlite::Result<ConnectionSettings> {
    let binding = match row.get::<_, Option<String>>(6)? {
        Some(_) => Some(decode(row, 6)?),
        None => None,
    };
    Ok(ConnectionSettings {
        name: row.get(0)?,
        platform: row.get(1)?,
        enabled: row.get(2)?,
        revision: row.get::<_, i64>(3)? as u64,
        policy: decode(row, 4)?,
        credential_ref: row.get(5)?,
        binding,
    })
}

impl ResourceSource {
    pub(super) fn scope_route(&self, mut route: Route) -> Result<Route> {
        route.id = key("resource-route", &(&route.id, self))?;
        Ok(route)
    }
}

pub(super) fn link_resource_delivery(
    tx: &rusqlite::Transaction<'_>,
    connection: &str,
    event: &str,
    delivery: &str,
) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO messaging_resource_delivery (delivery,connection,event)
         SELECT ?3,connection,event FROM messaging_resource_evidence WHERE connection=?1 AND event=?2",
        params![connection,event,delivery],
    )?;
    Ok(())
}

impl Store {
    pub(super) fn connection_settings(&self) -> Result<Vec<ConnectionSettings>> {
        self.read(|db| {
            let mut select = db.prepare(
                "SELECT name,platform,enabled,revision,policy,credential_ref,binding
                 FROM messaging_configuration ORDER BY name",
            )?;
            let rows = select.query_map([], settings)?;
            Ok(rows.collect::<rusqlite::Result<_>>()?)
        })
    }

    pub(super) fn connection_setting(&self, name: &str) -> Result<Option<ConnectionSettings>> {
        self.read(|db| {
            Ok(db
                .query_row(
                    "SELECT name,platform,enabled,revision,policy,credential_ref,binding
                 FROM messaging_configuration WHERE name=?1",
                    [name],
                    settings,
                )
                .optional()?)
        })
    }

    pub(super) fn register_connection(
        &self,
        name: &str,
        platform: &str,
        policy: &AccessPolicy,
    ) -> Result<ConnectionSettings> {
        let policy = serde_json::to_string(policy)?;
        self.write(|tx| {
            tx.execute(
                "INSERT OR IGNORE INTO messaging_configuration
                 (name,platform,enabled,revision,policy) VALUES (?1,?2,1,0,?3)",
                params![name, platform, policy],
            )?;
            let existing = tx.query_row(
                "SELECT name,platform,enabled,revision,policy,credential_ref,binding
                 FROM messaging_configuration WHERE name=?1",
                [name],
                settings,
            )?;
            ensure!(
                existing.platform == platform,
                "Connection name is bound to another platform"
            );
            Ok(existing)
        })
    }

    pub(super) fn enable_connection_with_policy(
        &self,
        name: &str,
        policy: &AccessPolicy,
    ) -> Result<ConnectionSettings> {
        let policy = serde_json::to_string(policy)?;
        self.write(|tx| {
            Ok(tx.query_row(
                "UPDATE messaging_configuration SET enabled=1,policy=?2,revision=revision+1
                 WHERE name=?1 RETURNING name,platform,enabled,revision,policy,credential_ref,binding",
                params![name, policy], settings,
            )?)
        })
    }

    pub(super) fn set_connection_policy(&self, name: &str, policy: &AccessPolicy) -> Result<i64> {
        let policy = serde_json::to_string(policy)?;
        self.write(|tx| {
            Ok(tx.query_row(
                "UPDATE messaging_configuration SET policy=?2,revision=revision+1 WHERE name=?1
                 RETURNING revision",
                params![name, policy],
                |row| row.get(0),
            )?)
        })
    }

    pub(super) fn set_connection_enabled(&self, name: &str, enabled: bool) -> Result<()> {
        self.write(|tx| {
            ensure!(tx.execute(
                "UPDATE messaging_configuration SET enabled=?2,revision=revision+1 WHERE name=?1",
                params![name, enabled],
            )? == 1, "No such messaging connection");
            Ok(())
        })
    }

    pub(super) fn create_enrollment(&self, name: &str, ttl_ms: i64) -> Result<EnrollmentLease> {
        ensure!(
            (1..=MAX_LEASE_MS).contains(&ttl_ms),
            "Invalid enrollment lifetime"
        );
        let lease = EnrollmentLease {
            id: crate::new_id(),
            expires_at: time_ms() + ttl_ms,
            prompt: None,
        };
        self.write(|tx| {
            ensure!(
                tx.execute(
                    "INSERT INTO messaging_enrollment (name,lease,revision,expires_at)
                 SELECT name,?2,revision,?3 FROM messaging_configuration WHERE name=?1
                 ON CONFLICT(name) DO UPDATE SET lease=excluded.lease,
                    revision=excluded.revision,expires_at=excluded.expires_at",
                    params![name, lease.id, lease.expires_at],
                )? == 1,
                "No such messaging connection"
            );
            Ok(lease)
        })
    }

    pub(super) fn enrollment_deadline(&self, name: &str, lease: &str) -> Result<Option<i64>> {
        self.read(|db| {
            Ok(db
                .query_row(
                    "SELECT expires_at FROM messaging_enrollment e
                 JOIN messaging_configuration c ON c.name=e.name AND c.revision=e.revision
                 WHERE e.name=?1 AND e.lease=?2 AND e.expires_at>?3",
                    params![name, lease, time_ms()],
                    |row| row.get(0),
                )
                .optional()?)
        })
    }

    pub(super) fn complete_enrollment(
        &self,
        name: &str,
        lease: &str,
        binding: &AccountBinding,
        value: &str,
    ) -> Result<()> {
        let binding = serde_json::to_string(binding)?;
        let reference = crate::new_id();
        self.write(|tx| {
            let old: Option<String> = tx
                .query_row(
                    "SELECT c.credential_ref FROM messaging_enrollment e
                 JOIN messaging_configuration c ON c.name=e.name AND c.revision=e.revision
                 WHERE e.name=?1 AND e.lease=?2 AND e.expires_at>?3",
                    params![name, lease, time_ms()],
                    |row| row.get(0),
                )
                .context("Enrollment expired or was replaced")?;
            tx.execute(
                "INSERT INTO messaging_connection_secrets (reference,value) VALUES (?1,?2)",
                params![reference, value],
            )?;
            tx.execute(
                "UPDATE messaging_configuration SET credential_ref=?2,binding=?3,enabled=1,
                 revision=revision+1 WHERE name=?1",
                params![name, reference, binding],
            )?;
            if let Some(old) = old {
                tx.execute(
                    "DELETE FROM messaging_connection_secrets WHERE reference=?1",
                    [old],
                )?;
            }
            tx.execute("DELETE FROM messaging_enrollment WHERE name=?1", [name])?;
            Ok(())
        })
    }

    pub(super) fn renew_credential(
        &self,
        name: &str,
        previous_revision: i64,
        value: &str,
    ) -> Result<Option<i64>> {
        self.write(|tx| {
            let entry: Option<String> = tx
                .query_row(
                    "SELECT credential_ref FROM messaging_configuration
                 WHERE name=?1 AND revision=?2 AND enabled=1 AND binding IS NOT NULL",
                    params![name, previous_revision],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();
            let Some(reference) = entry else {
                return Ok(None);
            };
            ensure!(
                tx.execute(
                    "UPDATE messaging_connection_secrets SET value=?2 WHERE reference=?1",
                    params![reference, value],
                )? == 1,
                "Adapter credential reference is unavailable"
            );
            Ok(Some(tx.query_row(
                "UPDATE messaging_configuration SET revision=revision+1
                 WHERE name=?1 RETURNING revision",
                [name],
                |row| row.get(0),
            )?))
        })
    }

    pub(super) fn credential(&self, reference: &str) -> Result<Option<EnrollmentCredential>> {
        let value: Option<String> = self.read(|db| {
            Ok(db
                .query_row(
                    "SELECT value FROM messaging_connection_secrets WHERE reference=?1",
                    [reference],
                    |row| row.get(0),
                )
                .optional()?)
        })?;
        value
            .as_deref()
            .map(|text| {
                EnrollmentCredential::new(hermes_engine::secret::unseal(text)?).map_err(Into::into)
            })
            .transpose()
    }

    fn record_actor(&self, connection: &str, observation: StoredActor) -> Result<bool> {
        let actor = &observation.evidence;
        let aliases = actor
            .aliases
            .iter()
            .map(ActorAlias::sender_id)
            .collect::<Result<Vec<_>, _>>()?;
        self.write(|tx| {
            let old: Option<StoredActor> = tx
                .query_row(
                    "SELECT data FROM messaging_actor_evidence WHERE connection=?1 AND actor=?2",
                    params![connection, actor.actor],
                    |row| decode(row, 0),
                )
                .optional()?;
            if let Some(old) = old {
                if old.evidence.kind != actor.kind {
                    return Ok(false);
                }
                if old.transport_scope == observation.transport_scope
                    && old.evidence.revision >= actor.revision
                {
                    return Ok(&old.evidence == actor);
                }
            }
            let owner =
                |id: &str| {
                    tx.query_row(
                "SELECT actor FROM messaging_actor_aliases WHERE connection=?1 AND alias=?2",
                params![connection, id], |row| row.get::<_, String>(0),
            ).optional()
                };
            if owner(&actor.actor)?.is_some_and(|id| id != actor.actor) {
                return Ok(false);
            }
            for alias in &aliases {
                if owner(alias)?.is_some_and(|id| id != actor.actor)
                    || tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM messaging_actor_evidence
                     WHERE connection=?1 AND actor=?2 AND actor<>?3)",
                        params![connection, alias, actor.actor],
                        |row| row.get::<_, bool>(0),
                    )?
                {
                    return Ok(false);
                }
            }
            for alias in aliases {
                tx.execute(
                    "INSERT OR IGNORE INTO messaging_actor_aliases (connection,alias,actor)
                     VALUES (?1,?2,?3)",
                    params![connection, alias, actor.actor],
                )?;
            }
            tx.execute(
                "INSERT INTO messaging_actor_evidence (connection,actor,data,revision)
                 VALUES (?1,?2,?3,?4) ON CONFLICT(connection,actor)
                 DO UPDATE SET data=excluded.data,revision=excluded.revision",
                params![
                    connection,
                    actor.actor,
                    serde_json::to_string(&observation)?,
                    i64::try_from(actor.revision)?
                ],
            )?;
            Ok(true)
        })
    }

    fn invalidate_actor_evidence(
        &self,
        connection: &str,
        transport: &str,
        room: Option<&Room>,
    ) -> Result<()> {
        self.write(|tx| {
            if let Some(room) = room {
                let kind = match room.kind {
                    RoomKind::Direct => "direct",
                    RoomKind::Group => "group",
                };
                tx.execute(
                    "UPDATE messaging_actor_evidence SET data = json_set(data, '$.evidence.memberships',
                       json((SELECT json_group_array(json(member.value))
                        FROM json_each(data, '$.evidence.memberships') member
                        WHERE NOT (json_extract(member.value, '$.room.kind') IS ?3
                           AND json_extract(member.value, '$.room.chat') IS ?4
                           AND json_extract(member.value, '$.room.thread') IS ?5))))
                     WHERE connection = ?1 AND json_extract(data, '$.transport_scope') = ?2
                       AND EXISTS (SELECT 1 FROM json_each(data, '$.evidence.memberships') member
                         WHERE json_extract(member.value, '$.room.kind') IS ?3
                           AND json_extract(member.value, '$.room.chat') IS ?4
                           AND json_extract(member.value, '$.room.thread') IS ?5)",
                    params![connection, transport, kind, room.chat, room.thread],
                )?;
            } else {
                tx.execute(
                    "UPDATE messaging_actor_evidence SET data = json_set(data, '$.evidence.expires_at', 0)
                     WHERE connection = ?1 AND json_extract(data, '$.transport_scope') = ?2",
                    params![connection, transport],
                )?;
            }
            Ok(())
        })
    }

    fn actor(&self, connection: &str, id: &str) -> Result<Option<StoredActor>> {
        self.read(|db| {
            Ok(db.query_row(
                "SELECT data FROM messaging_actor_evidence
                 WHERE connection=?1 AND actor=COALESCE(
                   (SELECT actor FROM messaging_actor_aliases WHERE connection=?1 AND alias=?2),?2)",
                params![connection, id], |row| decode(row, 0),
            ).optional()?)
        })
    }

    fn resource_evidence(&self, connection: &str, event: &str) -> Result<Option<ResourceEvidence>> {
        self.read(|db| {
            Ok(db.query_row(
                "SELECT evidence FROM messaging_resource_evidence WHERE connection=?1 AND event=?2",
                params![connection, event], |row| decode(row, 0),
            ).optional()?)
        })
    }

    fn delivery_resource(
        &self,
        connection: &str,
        delivery: &str,
    ) -> Result<Option<ResourceSource>> {
        self.read(|db| {
            let evidence: Option<ResourceEvidence> = db
                .query_row(
                    "SELECT r.evidence FROM messaging_resource_delivery d
                 JOIN messaging_resource_evidence r ON r.connection=d.connection AND r.event=d.event
                 WHERE d.connection=?1 AND d.delivery=?2",
                    params![connection, delivery],
                    |row| decode(row, 0),
                )
                .optional()?;
            Ok(evidence.map(|record| record.source))
        })
    }

    fn intake_resource(
        &self,
        connection: &str,
        revision: i64,
        candidate: Candidate,
        evidence: &ResourceEvidence,
        advance: Option<&Advance>,
    ) -> Result<Result<IntakeReceipt, GatewayError>> {
        self.write(|tx| {
            if !store::check_advance(tx, connection, advance)? {
                return Ok(Err(GatewayError::CheckpointChanged));
            }
            let id = &candidate.event.id;
            let conflict = || Ok(Ok(IntakeReceipt {
                id: id.clone(), route: None, disposition: Disposition::Conflict, repeated: false,
            }));
            let previous: Option<ResourceEvidence> = tx.query_row(
                "SELECT evidence FROM messaging_resource_evidence WHERE connection=?1 AND event=?2",
                params![connection, id], |row| decode(row, 0),
            ).optional()?;
            if previous.is_none() {
                // A reserved resource identity cannot adopt an ordinary chat
                // input staged under the wrong trust boundary.
                let exists = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM messaging_ingress WHERE connection=?1 AND event=?2)",
                    params![connection, id], |row| row.get::<_, bool>(0),
                )?;
                if exists {
                    return conflict();
                }
                tx.execute(
                    "INSERT INTO messaging_resource_evidence (connection,event,evidence)
                     VALUES (?1,?2,?3)",
                    params![connection, id, serde_json::to_string(evidence)?],
                )?;
            } else if previous.as_ref() != Some(evidence) {
                return conflict();
            }
            let receipt = store::append(tx, connection, revision, candidate, false)?;
            store::write_advance(tx, connection, advance)?;
            Ok(Ok(receipt))
        })
    }

    pub(super) fn paired(&self, connection: &str, sender: &str, room: &Room) -> Result<bool> {
        if room.kind != RoomKind::Direct || room.thread.is_some() {
            return Ok(false);
        }
        self.read(|db| {
            Ok(db.query_row(
                "SELECT EXISTS(SELECT 1 FROM messaging_pairing WHERE connection=?1 AND actor=?2
                 AND room=?3 AND authorized=1)",
                params![connection, sender, room.chat],
                |row| row.get(0),
            )?)
        })
    }

    fn begin_pairing(
        &self,
        connection: &str,
        actor: &str,
        room: &Room,
    ) -> Result<PairingChallenge> {
        let challenge = PairingChallenge {
            code: crate::new_id(),
            expires_at: time_ms() + MAX_LEASE_MS,
        };
        self.write(|tx| {
            tx.execute(
                "DELETE FROM messaging_pairing WHERE connection=?1 AND authorized=0 AND expires_at<=?2",
                params![connection, time_ms()],
            )?;
            let existing = tx.query_row(
                "SELECT challenge,expires_at FROM messaging_pairing WHERE connection=?1
                 AND actor=?2 AND room=?3 AND authorized=0 ORDER BY expires_at DESC LIMIT 1",
                params![connection, actor, room.chat],
                |row| Ok(PairingChallenge { code: row.get(0)?, expires_at: row.get(1)? }),
            ).optional()?;
            if let Some(existing) = existing {
                return Ok(existing);
            }
            let pending: u32 = tx.query_row(
                "SELECT count(*) FROM messaging_pairing WHERE connection=?1 AND authorized=0",
                [connection], |row| row.get(0),
            )?;
            ensure!(pending < 128, "Too many pending pairing challenges");
            tx.execute(
                "INSERT INTO messaging_pairing (connection,challenge,actor,room,expires_at)
                 VALUES (?1,?2,?3,?4,?5)",
                params![connection, challenge.code, actor, room.chat, challenge.expires_at],
            )?;
            Ok(challenge)
        })
    }

    pub(super) fn approve_pairing(&self, connection: &str, code: &str) -> Result<bool> {
        self.write(|tx| {
            Ok(tx.execute(
                "UPDATE messaging_pairing SET authorized=1
                 WHERE connection=?1 AND challenge=?2 AND authorized=0 AND expires_at>?3",
                params![connection, code, time_ms()],
            )? != 0)
        })
    }

    pub(super) fn revoke_pairing(&self, connection: &str, actor: &str, chat: &str) -> Result<bool> {
        self.write(|tx| {
            Ok(tx.execute(
                "DELETE FROM messaging_pairing WHERE connection=?1 AND actor=?2 AND room=?3",
                params![connection, actor, chat],
            )? != 0)
        })
    }
}

impl Ingress {
    /// Record identity proven by the adapter under this verified connection.
    /// This API accepts evidence from the active adapter only, never event body fields.
    pub async fn record_actor(&self, actor: ActorEvidence) -> Result<(), GatewayError> {
        if !valid_id(&actor.actor)
            || actor.expires_at < 0
            || actor.revision > i64::MAX as u64
            || actor.aliases.len() > 64
            || actor.aliases.iter().any(|a| a.sender_id().is_err())
            || actor.memberships.len() > 256
            || actor.memberships.iter().any(|m| {
                !valid_id(&m.room.chat)
                    || m.room
                        .thread
                        .as_ref()
                        .is_some_and(|thread| !valid_id(thread))
                    || m.roles.len() > 128
                    || m.roles.iter().any(|role| !valid_id(role))
            })
        {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |_| {
                let transport_scope = db(key("actor-transport", &scope.installation()))?;
                if db(scope.store.record_actor(
                    &scope.connection,
                    StoredActor {
                        transport_scope,
                        evidence: actor,
                    },
                ))? {
                    Ok(())
                } else {
                    Err(GatewayError::Forbidden)
                }
            })
        })
        .await?;
        self.notify();
        Ok(())
    }

    /// Public children inherit the proved parent audience. The caller must
    /// hold the active slot and have a confirmed provider destination receipt.
    pub fn inherit_public_thread_membership(
        &self,
        parent: &Room,
        child: &Room,
        actor: &str,
    ) -> Result<(), GatewayError> {
        let ActorObservation::Verified(mut evidence) = self.actor_observation(actor)? else {
            return Ok(());
        };
        if evidence.memberships.iter().any(|membership| membership.room == *child) {
            return Ok(());
        }
        let membership = evidence.memberships.iter().find(|membership| membership.room == *parent)
            .ok_or(GatewayError::Forbidden)?;
        let roles = membership.roles.clone();
        if evidence.memberships.len() == 256 {
            let oldest = evidence.memberships.iter().position(|membership| membership.room != *parent)
                .ok_or(GatewayError::Invalid)?;
            evidence.memberships.remove(oldest);
        }
        evidence.memberships.push(MemberEvidence { room: child.clone(), roles });
        evidence.revision = evidence.revision.saturating_add(1);
        let transport_scope = db(key("actor-transport", &self.installation()))?;
        if !db(self.store.record_actor(&self.connection, StoredActor { transport_scope, evidence }))? {
            return Err(GatewayError::Forbidden);
        }
        Ok(())
    }

    /// Withdraw stale membership evidence after a transport gap or an exact
    /// room authority change. Aliases and actor revisions keep their bindings.
    pub async fn invalidate_actor_evidence(&self, room: Option<Room>) -> Result<(), GatewayError> {
        if room.as_ref().is_some_and(|room| {
            !valid_id(&room.chat)
                || room.thread.as_deref().is_some_and(|thread| !valid_id(thread))
        }) {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        blocking(move || {
            scope.apply_evidence_invalidation(|| {
                let transport = db(key("actor-transport", &scope.installation()))?;
                db(scope.store.invalidate_actor_evidence(
                    &scope.connection, &transport, room.as_ref(),
                ))
            })
        })
        .await?;
        self.notify();
        Ok(())
    }

    pub(super) fn actor_observation(&self, id: &str) -> Result<ActorObservation, GatewayError> {
        let Some(stored) = db(self.store.actor(&self.connection, id))? else {
            return Ok(ActorObservation::Missing);
        };
        let scope = db(key("actor-transport", &self.installation()))?;
        Ok(if stored.transport_scope == scope {
            ActorObservation::Verified(stored.evidence)
        } else {
            ActorObservation::Stale
        })
    }

    pub fn effective_sender(
        &self,
        policy: &AccessPolicy,
        room: &Room,
        source: &Sender,
    ) -> Result<Option<(Sender, ActorKind)>, GatewayError> {
        let evidence = match self.actor_observation(&source.id)? {
            ActorObservation::Verified(evidence) => evidence,
            ActorObservation::Stale => return Ok(None),
            ActorObservation::Missing => {
                return Ok((!policy.scope.require_actor_evidence)
                    .then(|| (source.clone(), ActorKind::Person)));
            }
        };
        // If this alias was withdrawn, retain the reserved association but
        // reject the observation instead of falling back to an unbound identity.
        if source.id != evidence.actor
            && !evidence
                .aliases
                .iter()
                .any(|alias| alias.sender_id().as_ref() == Ok(&source.id))
        {
            return Ok(None);
        }
        let membership = evidence.memberships.iter().find(|item| item.room == *room);
        if evidence.expires_at <= time_ms() || membership.is_none() {
            return Ok(None);
        }
        Ok(Some((
            Sender {
                id: evidence.actor,
                roles: membership
                    .filter(|_| evidence.kind == ActorKind::Person)
                    .map_or_else(BTreeSet::new, |m| m.roles.clone()),
            },
            evidence.kind,
        )))
    }

    pub(super) fn normalize_event(
        &self,
        event: &mut super::InboundEvent,
    ) -> Result<(), GatewayError> {
        if let ActorObservation::Verified(evidence) = self.actor_observation(&event.sender.id)?
            && (event.sender.id == evidence.actor
                || evidence
                    .aliases
                    .iter()
                    .any(|alias| alias.sender_id().as_ref() == Ok(&event.sender.id)))
        {
            // Keep transport aliases in the evidence table. Canonicalizing here
            // gives equivalent aliases the same input identity and route.
            event.sender.id = evidence.actor;
        }
        Ok(())
    }

    pub fn authorized_identity(
        &self,
        policy: &AccessPolicy,
        room: &Room,
        sender: &Sender,
    ) -> Result<Option<(Sender, ActorKind)>, GatewayError> {
        let Some((verified, kind)) = self.effective_sender(policy, room, sender)? else {
            return Ok(None);
        };
        let paired = policy.scope.pair_direct
            && kind == ActorKind::Person
            && db(self.store.paired(&self.connection, &verified.id, room))?;
        Ok(policy
            .grants_sender(room, &verified, paired)
            .then_some((verified, kind)))
    }

    pub fn authorize_sender(
        &self,
        policy: &AccessPolicy,
        room: &Room,
        sender: &Sender,
    ) -> Result<bool, GatewayError> {
        Ok(self.authorized_identity(policy, room, sender)?.is_some())
    }

    pub fn authorize_operator(
        &self,
        policy: &AccessPolicy,
        room: &Room,
        sender: &Sender,
    ) -> Result<bool, GatewayError> {
        Ok(self
            .authorized_identity(policy, room, sender)?
            .is_some_and(|(verified, kind)| {
                kind == ActorKind::Person && policy.operator(&verified)
            }))
    }

    pub async fn authorize_resource(
        &self,
        operation: &str,
        resource: &str,
        room: &Room,
        sender: &Sender,
    ) -> Result<(), GatewayError> {
        self.checked(true, |state| {
            if self.resource_allowed(&state.policy, operation, resource, room, sender)? {
                Ok(())
            } else {
                Err(GatewayError::Forbidden)
            }
        })
    }

    fn resource_allowed(
        &self,
        policy: &AccessPolicy,
        operation: &str,
        resource: &str,
        room: &Room,
        sender: &Sender,
    ) -> Result<bool, GatewayError> {
        let Some(ResourcePolicy {
            actors,
            resources,
            destinations,
        }) = policy.scope.events.get(operation)
        else {
            return Ok(false);
        };
        if !resources.contains(resource)
            || !destinations.contains(room)
            || !matches!(
                self.actor_observation(&sender.id)?,
                ActorObservation::Verified(_)
            )
        {
            return Ok(false);
        }
        Ok(self
            .authorized_identity(policy, room, sender)?
            .is_some_and(|(actor, kind)| kind == ActorKind::Person && actors.contains(&actor.id)))
    }

    pub(super) fn recheck_resource(
        &self,
        policy: &AccessPolicy,
        event: &InboundEvent,
    ) -> Result<Option<ResourceSource>, GatewayError> {
        if !resource_event_id(&event.id) {
            return Ok(None);
        }
        let evidence = db(self.store.resource_evidence(&self.connection, &event.id))?
            .ok_or(GatewayError::Forbidden)?;
        if !matches!(event.body, InboundBody::Message(_))
            || !self.resource_allowed(
                policy,
                &evidence.source.operation,
                &evidence.source.resource,
                &event.room,
                &event.sender,
            )?
        {
            return Err(GatewayError::Forbidden);
        }
        Ok(Some(evidence.source))
    }

    pub(super) fn recheck_resource_delivery(
        &self,
        policy: &AccessPolicy,
        delivery: &str,
        room: &Room,
        sender: &Sender,
    ) -> Result<Option<ResourceSource>, GatewayError> {
        let source = db(self.store.delivery_resource(&self.connection, delivery))?;
        if let Some(resource) = &source
            && !self.resource_allowed(
                policy,
                &resource.operation,
                &resource.resource,
                room,
                sender,
            )?
        {
            return Err(GatewayError::Forbidden);
        }
        Ok(source)
    }

    /// Admit a non-chat event with its authenticated operation and resource
    /// bound to the durable receipt. The returned identity is the native
    /// resource-event key; the provider identifier remains in its evidence.
    pub async fn submit_resource_event(
        &self,
        source: ResourceSource,
        mut event: InboundEvent,
        advance: Option<Advance>,
    ) -> Result<IntakeReceipt, GatewayError> {
        if !super::runtime::valid_input_id(&event.id)
            || !valid_id(&source.operation)
            || !valid_id(&source.resource)
            || !matches!(event.body, InboundBody::Message(_))
            || !super::runtime::valid_advance(advance.as_ref())
        {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        let result = blocking(move || {
            scope.checked(true, |state| {
                let decision = if scope.resource_allowed(
                    &state.policy,
                    &source.operation,
                    &source.resource,
                    &event.room,
                    &event.sender,
                )? {
                    match scope.route_event(&state.policy, &event)? {
                        Ok(route) => Ok(db(source.scope_route(route))?),
                        Err(disposition) => Err(disposition),
                    }
                } else {
                    Err(Disposition::Forbidden)
                };
                let evidence = ResourceEvidence {
                    source,
                    provider_event: event.id.clone(),
                };
                event.id = db(key(RESOURCE_EVENT, &evidence))?;
                let advance = scope.scoped_advance(advance)?;
                let candidate = scope.intake_candidate(&state.policy, event, decision, None)?;
                db(scope.store.intake_resource(
                    &scope.connection,
                    state.revision,
                    candidate,
                    &evidence,
                    advance.as_ref(),
                ))?
            })
        })
        .await?;
        self.notify();
        Ok(result)
    }

    pub async fn begin_pairing(
        &self,
        room: Room,
        sender: Sender,
    ) -> Result<PairingChallenge, GatewayError> {
        if room.kind != RoomKind::Direct
            || room.thread.is_some()
            || !valid_id(&room.chat)
            || !valid_id(&sender.id)
        {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |state| {
                if !state.policy.scope.pair_direct || state.policy.room_policy(&room).is_none() {
                    return Err(GatewayError::Forbidden);
                }
                let Some((sender, ActorKind::Person)) =
                    scope.effective_sender(&state.policy, &room, &sender)?
                else {
                    return Err(GatewayError::Forbidden);
                };
                if state.policy.scope.denied_users.contains(&sender.id)
                    || !state.policy.scope.denied_roles.is_disjoint(&sender.roles)
                {
                    return Err(GatewayError::Forbidden);
                }
                db(scope
                    .store
                    .begin_pairing(&scope.connection, &sender.id, &room))
            })
        })
        .await
    }
}
