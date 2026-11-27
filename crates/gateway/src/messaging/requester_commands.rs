use super::{InvocationOutcome, record_requester_invocation};
use crate::messaging::{
    AccessPolicy, ActorKind, Advance, Disposition, GatewayError, Ingress, Room, Sender,
    profile::ActorObservation,
    runtime::{blocking, db, valid_advance, valid_id, valid_input_id},
    store,
};
use rusqlite::params;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequesterCommand {
    Status,
    Help,
    Identity,
}

/// A command from an authenticated private-response transport. The receipt
/// has no conversation route and can never enter the session or outbox queues.
#[derive(Clone, Deserialize, Serialize)]
pub struct RequesterInvocation {
    pub id: String,
    pub room: Room,
    pub sender: Sender,
    pub command: RequesterCommand,
}

pub struct RequesterAuthority {
    pub actor: String,
    pub operator: bool,
}

impl Ingress {
    fn requester_authority(
        &self,
        policy: &AccessPolicy,
        request: &RequesterInvocation,
    ) -> Result<Option<RequesterAuthority>, GatewayError> {
        // The transport must have recorded current evidence for this exact
        // requester and room. Roles in the command input confer no authority.
        if !matches!(
            self.actor_observation(&request.sender.id)?,
            ActorObservation::Verified(_)
        ) {
            return Ok(None);
        }
        let Some((actor, ActorKind::Person)) =
            self.effective_sender(policy, &request.room, &request.sender)?
        else {
            return Ok(None);
        };
        let Some(grant) = policy.requester_command_grant(&request.room, &actor) else {
            return Ok(None);
        };
        match self.check_source_protection(policy, &request.room, &actor.id, &request.id) {
            Ok(_) => {}
            Err(GatewayError::ProtectionUnavailable) => return Ok(None),
            Err(error) => return Err(error),
        }
        let operator =
            policy.operators.contains(&actor.id) || !grant.operator_roles.is_disjoint(&actor.roles);
        Ok(Some(RequesterAuthority {
            actor: actor.id,
            operator,
        }))
    }

    fn requester_audience(
        &self,
        request: &RequesterInvocation,
        actor: &str,
    ) -> Result<String, GatewayError> {
        db(store::key(
            "requester-command",
            &(
                &self.connection,
                self.installation(),
                &request.room,
                actor,
                request.command,
            ),
        ))
    }

    /// Commit a private native command and the provider checkpoint together.
    /// Commands on this path can report status or identity and cannot operate
    /// on an existing conversation.
    pub async fn commit_requester_command(
        &self,
        request: RequesterInvocation,
        advance: Advance,
    ) -> Result<Disposition, GatewayError> {
        if !valid_input_id(&request.id)
            || !valid_id(&request.sender.id)
            || !valid_id(&request.room.chat)
            || request
                .room
                .thread
                .as_deref()
                .is_some_and(|thread| !valid_id(thread))
            || !valid_advance(Some(&advance))
        {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |state| {
                let authority = scope.requester_authority(&state.policy, &request)?;
                let actor = authority.as_ref().map_or(request.sender.id.as_str(), |verified| verified.actor.as_str());
                let audience = scope.requester_audience(&request, actor)?;
                let advance = scope.scoped_advance(Some(advance))?;
                db(scope.store.write(|tx| {
                    if !store::check_advance(tx, &scope.connection, advance.as_ref())? {
                        return Ok(Err(GatewayError::CheckpointChanged));
                    }
                    let disposition = if let Some(receipt) = store::ingress_replay(
                        tx, &scope.connection, &request.id, &audience,
                    )? {
                        receipt.disposition
                    } else {
                        let (disposition, stage) = if authority.is_some() {
                            record_requester_invocation(
                                tx, &scope.connection, &request.id, &audience, actor,
                                InvocationOutcome::Ready,
                            )?;
                            (Disposition::Accepted, "done")
                        } else {
                            (Disposition::Forbidden, "refused")
                        };
                        tx.execute(
                            "INSERT INTO messaging_ingress
                             (connection,event,fingerprint,disposition,control,policy_revision,stage,reason)
                             VALUES (?1,?2,?3,?4,1,?5,?6,'requester_command')",
                            params![scope.connection, request.id, audience,
                                serde_json::to_string(&disposition)?, state.revision, stage],
                        )?;
                        disposition
                    };
                    store::write_advance(tx, &scope.connection, advance.as_ref())?;
                    Ok(Ok(disposition))
                }))?
            })
        })
        .await
    }

    /// Read only the authority of this exact, durably accepted invocation.
    /// Use immediately before sending the requester's private response.
    pub fn requester_command_response(
        &self,
        request: &RequesterInvocation,
    ) -> Result<RequesterAuthority, GatewayError> {
        self.checked(true, |state| {
            let authority = self
                .requester_authority(&state.policy, request)?
                .ok_or(GatewayError::Forbidden)?;
            let audience = self.requester_audience(request, &authority.actor)?;
            let recorded = db(self.store.read(|tx| {
                Ok(tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM messaging_native_invocations
                       WHERE connection = ?1 AND event = ?2 AND route = ?3 AND actor = ?4)",
                    params![self.connection, request.id, audience, authority.actor],
                    |row| row.get::<_, bool>(0),
                )?)
            }))?;
            recorded.then_some(authority).ok_or(GatewayError::Forbidden)
        })
    }
}
