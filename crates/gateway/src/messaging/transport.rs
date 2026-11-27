use super::{
    AccessPolicy, EffectTransportPin, EffectTransportRef, SendAttempt,
    profile::{ActorKind, ActorObservation, EnrollmentCredential, VerifiedInstallation, time_ms},
    runtime::{Ingress, blocking, db, valid_id},
    store::Store,
    types::{GatewayError, Room, Sender},
};
use anyhow::Result;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::fs::File;

pub(super) const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS messaging_addresses (
    connection TEXT NOT NULL, reference TEXT NOT NULL, address TEXT NOT NULL,
    scope TEXT NOT NULL, secret TEXT, revision INTEGER NOT NULL, revoked INTEGER NOT NULL,
    PRIMARY KEY(connection, reference)
);
";

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct InstallationKey {
    pub installation: String,
    pub tenant: Option<String>,
    pub application: Option<String>,
}

impl From<&VerifiedInstallation> for InstallationKey {
    fn from(proof: &VerifiedInstallation) -> Self {
        Self {
            installation: proof.installation.clone(),
            tenant: proof.tenant.clone(),
            application: proof.application.clone(),
        }
    }
}

/// Exclusive receiver lease. A route is selected only after the receiver adapter
/// authenticates exactly one installation, using trusted transport evidence.
/// Constructing an InstallationKey from unverified request fields is insufficient.
pub struct SharedReceiver {
    pub(super) identity: String,
    pub(super) routes: Vec<(InstallationKey, Ingress)>,
    pub(super) _lease: File,
}

pub struct ReceiverLease {
    pub(super) scope: Ingress,
    pub(super) _lease: File,
}

impl ReceiverLease {
    pub fn check(&self) -> Result<(), GatewayError> {
        self.scope.checked(true, |_| Ok(()))
    }
}

impl SharedReceiver {
    pub fn route_verified(&self, matches: &[InstallationKey]) -> Result<Ingress, GatewayError> {
        let route = self.match_installation(matches).ok_or(GatewayError::Forbidden)?;
        route.checked(true, |_| Ok(()))?;
        Ok(route.clone())
    }

    pub(super) fn match_installation(&self, matches: &[InstallationKey]) -> Option<&Ingress> {
        let [installation] = matches else { return None };
        // bind_receiver rejects duplicate verified installation keys.
        self.routes.iter().find(|(key, _)| key == installation).map(|(_, route)| route)
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransportAddress {
    pub reference: String,
    pub endpoint: String,
    pub resource: String,
    pub operation: String,
    pub actor: Option<String>,
    pub room: Option<Room>,
    pub expires_at: i64,
    pub revision: u64,
}

#[derive(Clone, Eq, PartialEq)]
pub struct TransportUse {
    pub reference: String,
    pub endpoint: String,
    pub resource: String,
    pub operation: String,
    pub actor: Option<String>,
    pub room: Option<Room>,
}

/// A checked credential for one endpoint, resource and operation. It must be
/// used immediately under the current lease, without following a redirect or
/// sharing it with another transport.
pub struct TransportAuthority {
    pub endpoint: reqwest::Url,
    credential: Option<EnrollmentCredential>,
}

impl TransportAuthority {
    pub fn credential(&self) -> Option<&str> {
        self.credential.as_ref().map(EnrollmentCredential::secret)
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq)]
struct AddressScope {
    origin: String,
    device: Option<String>,
}

impl AddressScope {
    fn matches(&self, installation: &VerifiedInstallation) -> bool {
        installation.origin.as_deref() == Some(&self.origin) && self.device == installation.device
    }
}

fn safe_endpoint(endpoint: &str, origin: &str) -> Option<reqwest::Url> {
    let url = reqwest::Url::parse(endpoint).ok()?;
    (url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
        && url.origin().ascii_serialization() == origin)
        .then_some(url)
}

impl TransportAddress {
    fn same_binding(&self, other: &Self) -> bool {
        self.reference == other.reference
            && self.resource == other.resource
            && self.operation == other.operation
            && self.actor == other.actor
            && self.room == other.room
    }

    fn matches(&self, request: &TransportUse) -> bool {
        self.reference == request.reference
            && self.endpoint == request.endpoint
            && self.resource == request.resource
            && self.operation == request.operation
            && self.actor == request.actor
            && self.room == request.room
    }
}

impl Store {
    fn put_address(
        &self,
        connection: &str,
        address: &TransportAddress,
        scope: &AddressScope,
        secret: Option<String>,
    ) -> Result<bool> {
        self.write(|tx| {
            let current: Option<(TransportAddress, bool)> = tx.query_row(
                "SELECT address,revoked FROM messaging_addresses WHERE connection=?1 AND reference=?2",
                params![connection, address.reference],
                |row| Ok((super::store::decode(row, 0)?, row.get(1)?)),
            ).optional()?;
            if let Some((old, revoked)) = current {
                if !old.same_binding(address) || address.revision < old.revision {
                    return Ok(false);
                }
                if address.revision == old.revision {
                    return Ok(!revoked && old == *address);
                }
            }
            tx.execute(
                "INSERT INTO messaging_addresses (connection,reference,address,scope,secret,revision,revoked)
                 VALUES (?1,?2,?3,?4,?5,?6,0) ON CONFLICT(connection,reference) DO UPDATE SET
                 address=excluded.address,scope=excluded.scope,secret=excluded.secret,
                 revision=excluded.revision,revoked=0",
                params![connection, address.reference, serde_json::to_string(address)?,
                    serde_json::to_string(scope)?, secret, i64::try_from(address.revision)?],
            )?;
            Ok(true)
        })
    }

    fn address(
        &self,
        connection: &str,
        reference: &str,
    ) -> Result<Option<(TransportAddress, AddressScope, Option<String>)>> {
        self.read(|db| {
            Ok(db
                .query_row(
                    "SELECT address,scope,secret FROM messaging_addresses
                 WHERE connection=?1 AND reference=?2 AND revoked=0",
                    params![connection, reference],
                    |row| {
                        Ok((
                            super::store::decode(row, 0)?,
                            super::store::decode(row, 1)?,
                            row.get(2)?,
                        ))
                    },
                )
                .optional()?)
        })
    }

    fn revoke_address(&self, connection: &str, reference: &str, revision: u64) -> Result<bool> {
        self.write(|tx| {
            // Set the serialized revision as well as the row revision. A stale
            // observed grant must never recreate a revoked address.
            Ok(tx.execute(
                "UPDATE messaging_addresses SET revoked=1,secret=NULL,revision=?3,
                 address=json_set(address,'$.revision',?3)
                 WHERE connection=?1 AND reference=?2 AND revision<?3",
                params![connection, reference, i64::try_from(revision)?],
            )? != 0)
        })
    }
}

struct CheckedEffectAddress {
    pin: EffectTransportPin,
    endpoint: reqwest::Url,
    sealed_credential: Option<String>,
}

impl Ingress {
    /// Called while the owner holds the connection lease. The pin is safe to
    /// persist with an attempt because it contains no endpoint or credential.
    pub(super) fn authorize_effect_transport(
        &self,
        policy: &AccessPolicy,
        required: &EffectTransportRef,
        room: &Room,
        source_actor: Option<&Sender>,
    ) -> Result<Option<EffectTransportPin>, GatewayError> {
        Ok(self
            .checked_effect_address(policy, required, room, source_actor)?
            .map(|value| value.pin))
    }

    fn checked_effect_address(
        &self,
        policy: &AccessPolicy,
        required: &EffectTransportRef,
        room: &Room,
        source_actor: Option<&Sender>,
    ) -> Result<Option<CheckedEffectAddress>, GatewayError> {
        let Some((address, scope, sealed_credential)) =
            db(self.store.address(&self.connection, &required.reference))?
        else {
            return Ok(None);
        };
        let Some(installation) = self.installation() else {
            return Ok(None);
        };
        let actor = source_actor.map(|sender| sender.id.as_str());
        if !scope.matches(installation)
            || address.expires_at <= time_ms()
            || address.operation != required.operation
            || address.resource != required.resource
            || address.room.as_ref() != Some(room)
            || address
                .actor
                .as_deref()
                .is_some_and(|owner| Some(owner) != actor)
            || !self.grant_current(policy, address.actor.as_deref(), Some(room))?
        {
            return Ok(None);
        }
        let Some(endpoint) = safe_endpoint(&address.endpoint, &scope.origin) else {
            return Ok(None);
        };
        let pin = EffectTransportPin {
            requirement: required.clone(),
            fingerprint: db(super::store::key("effect-address", &(&address, &scope)))?,
            revision: address.revision,
        };
        Ok(Some(CheckedEffectAddress {
            pin,
            endpoint,
            sealed_credential,
        }))
    }

    /// Resolve only the address pinned to the currently leased attempt. The
    /// adapter must use the authority immediately and cannot follow redirects.
    pub async fn delivery_authority(
        &self,
        attempt: &SendAttempt,
    ) -> Result<TransportAuthority, GatewayError> {
        let ingress = self.clone();
        let attempt = attempt.clone();
        blocking(move || {
            ingress.checked(true, |state| {
                let (stored, actor) = db(ingress.store.dispatch_transport(
                    &ingress.connection,
                    &ingress.generation,
                    &attempt,
                ))?
                .ok_or(GatewayError::Forbidden)?;
                let live = ingress
                    .checked_effect_address(
                        &state.policy,
                        &stored.requirement,
                        &attempt.payload.room,
                        actor.as_ref(),
                    )?
                    .filter(|proof| proof.pin == stored)
                    .ok_or(GatewayError::Forbidden)?;
                let credential = live
                    .sealed_credential
                    .map(|value| {
                        EnrollmentCredential::new(db(hermes_engine::secret::unseal(&value))?)
                    })
                    .transpose()?;
                Ok(TransportAuthority {
                    endpoint: live.endpoint,
                    credential,
                })
            })
        })
        .await
    }

    fn grant_current(
        &self,
        policy: &super::AccessPolicy,
        actor: Option<&str>,
        room: Option<&Room>,
    ) -> Result<bool, GatewayError> {
        match (actor, room) {
            (Some(id), Some(room)) => {
                // A delegated credential needs stronger authentication than
                // an otherwise allowed basic/anonymous transport sender.
                let evidence = self.actor_observation(id)?;
                if !matches!(evidence, ActorObservation::Verified(actor)
                    if actor.actor == id && actor.kind == ActorKind::Person)
                {
                    return Ok(false);
                }
                self.authorize_sender(
                    policy,
                    room,
                    &Sender {
                        id: id.to_owned(),
                        roles: Default::default(),
                    },
                )
            }
            (None, Some(room)) => Ok(policy.room_policy(room).is_some()),
            (None, None) => Ok(true),
            (Some(_), None) => Ok(false),
        }
    }

    /// Save a provider-verified address/credential. The factory's connector
    /// owns provider authentication, while the owner fences subsequent use.
    pub async fn record_transport_address(
        &self,
        address: TransportAddress,
        credential: Option<EnrollmentCredential>,
    ) -> Result<(), GatewayError> {
        if !valid_id(&address.reference)
            || !valid_id(&address.resource)
            || !valid_id(&address.operation)
            || address.endpoint.len() > 4096
            || address.actor.as_ref().is_some_and(|actor| !valid_id(actor))
            || address.expires_at <= time_ms()
            || address.revision > i64::MAX as u64
        {
            return Err(GatewayError::Invalid);
        }
        let ingress = self.clone();
        blocking(move || {
            let installation = ingress.installation().ok_or(GatewayError::Unsupported)?;
            let origin = installation
                .origin
                .clone()
                .ok_or(GatewayError::Unsupported)?;
            safe_endpoint(&address.endpoint, &origin).ok_or(GatewayError::Forbidden)?;
            let scope = AddressScope {
                origin,
                device: installation.device.clone(),
            };
            let secret = credential
                .as_ref()
                .map(|credential| hermes_engine::secret::seal(ingress.root(), credential.secret()))
                .transpose();
            let secret = db(secret)?;
            ingress.checked(true, |state| {
                if !ingress.grant_current(
                    &state.policy,
                    address.actor.as_deref(),
                    address.room.as_ref(),
                )? || !db(ingress.store.put_address(
                    &ingress.connection,
                    &address,
                    &scope,
                    secret,
                ))? {
                    return Err(GatewayError::Forbidden);
                }
                Ok(())
            })
        })
        .await
    }

    pub async fn revoke_transport_address(
        &self,
        reference: &str,
        revision: u64,
    ) -> Result<bool, GatewayError> {
        if !valid_id(reference) || revision > i64::MAX as u64 {
            return Err(GatewayError::Invalid);
        }
        let ingress = self.clone();
        let reference = reference.to_owned();
        blocking(move || {
            ingress.checked(true, |_| {
                db(ingress
                    .store
                    .revoke_address(&ingress.connection, &reference, revision))
            })
        })
        .await
    }

    pub async fn transport_authority(
        &self,
        use_: &TransportUse,
    ) -> Result<TransportAuthority, GatewayError> {
        let ingress = self.clone();
        let use_ = use_.clone();
        blocking(move || {
            ingress.checked(true, |state| {
                let (address, scope, secret) =
                    db(ingress.store.address(&ingress.connection, &use_.reference))?
                        .ok_or(GatewayError::Forbidden)?;
                let proof = ingress.installation().ok_or(GatewayError::Forbidden)?;
                if address.expires_at <= time_ms()
                    || !scope.matches(proof)
                    || !address.matches(&use_)
                    || !ingress.grant_current(
                        &state.policy,
                        address.actor.as_deref(),
                        address.room.as_ref(),
                    )?
                {
                    return Err(GatewayError::Forbidden);
                }
                let endpoint =
                    safe_endpoint(&use_.endpoint, &scope.origin).ok_or(GatewayError::Forbidden)?;
                let credential = match secret {
                    Some(value) => Some(EnrollmentCredential::new(db(
                        hermes_engine::secret::unseal(&value),
                    )?)?),
                    None => None,
                };
                Ok(TransportAuthority {
                    endpoint,
                    credential,
                })
            })
        })
        .await
    }
}
