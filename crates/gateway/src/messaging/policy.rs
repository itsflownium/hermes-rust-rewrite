use super::{SpokenReplyMode, types::{AccessPolicy, Room, RoomKind, RoomPolicy, Sender}};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Owner policy layered from the account to its room and then to the exact thread.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScopePolicy {
    pub denied_users: BTreeSet<String>,
    pub denied_roles: BTreeSet<String>,
    pub denied_destinations: BTreeSet<String>,
    pub direct: RoomRules,
    pub rooms: BTreeMap<String, RoomRules>,
    pub threads: BTreeMap<String, BTreeMap<String, RoomRules>>,
    pub require_actor_evidence: bool,
    #[serde(alias = "background_commands_disabled")]
    pub scoped_commands_disabled: bool,
    pub spoken_reply_default: Option<SpokenReplyMode>,
    pub pair_direct: bool,
    pub events: BTreeMap<String, ResourcePolicy>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoomRules {
    pub enabled: Option<bool>,
    /// An explicit allow set further restricts inherited account grants.
    pub users: Option<BTreeSet<String>>,
    pub roles: Option<BTreeSet<String>>,
    pub denied_users: BTreeSet<String>,
    pub denied_roles: BTreeSet<String>,
    /// The nearest explicit grant replaces broader allow choices. Denials and
    /// inherited restrictions still apply.
    pub grant: Option<RoomGrant>,
    pub requester_commands: Option<RequesterCommandGrant>,
    pub mention_required: Option<bool>,
    pub per_participant: Option<bool>,
    pub turn: TurnPolicy,
    pub delivery: Option<DeliveryPolicy>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoomGrant {
    pub open: bool,
    pub paired: bool,
    pub users: BTreeSet<String>,
    pub roles: BTreeSet<String>,
}

/// An explicit command grant for an authenticated requester. It has no
/// authority over conversation turns, deliveries or interactive prompts.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RequesterCommandGrant {
    pub users: BTreeSet<String>,
    pub roles: BTreeSet<String>,
    pub operator_roles: BTreeSet<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TurnPolicy {
    pub model: Option<String>,
    pub prompt: Option<String>,
    pub skills: Option<BTreeSet<String>>,
    pub tools: Option<bool>,
    pub approval: Option<ApprovalPolicy>,
    pub approval_reuse: Option<ApprovalReuse>,
}

impl TurnPolicy {
    pub fn overlay(&mut self, next: &Self) {
        if next.model.is_some() {
            self.model.clone_from(&next.model);
        }
        if next.prompt.is_some() {
            self.prompt.clone_from(&next.prompt);
        }
        if next.skills.is_some() {
            self.skills.clone_from(&next.skills);
        }
        self.tools = next.tools.or(self.tools);
        self.approval = next.approval.or(self.approval);
        self.approval_reuse = next.approval_reuse.or(self.approval_reuse);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalPolicy {
    Manual,
    Smart,
    Off,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalReuse {
    Session,
    Persistent,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DeliveryPolicy {
    pub submitted: bool,
    pub mentions: bool,
    pub notifications: bool,
    pub protection: Option<String>,
}

/// Explicit authorization for non-chat work, scoped to an authenticated
/// operation, resource, individual actor, and returned audience.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResourcePolicy {
    pub actors: BTreeSet<String>,
    pub resources: BTreeSet<String>,
    pub destinations: Vec<Room>,
}

impl AccessPolicy {
    pub(super) fn spoken_reply_default(&self, room: &Room) -> Option<SpokenReplyMode> {
        self.room_policy(room).filter(|policy| policy.spoken_replies)?;
        self.scope.spoken_reply_default
    }

    fn rules(&self, room: &Room) -> impl Iterator<Item = &RoomRules> {
        (room.kind == RoomKind::Direct)
            .then_some(&self.scope.direct)
            .into_iter()
            .chain(self.scope.rooms.get(&room.chat))
            .chain(
                room.thread
                    .as_ref()
                    .and_then(|thread| self.scope.threads.get(&room.chat)?.get(thread)),
            )
    }

    pub(super) fn room_policy(&self, room: &Room) -> Option<RoomPolicy> {
        if self.scope.denied_destinations.contains(&room.chat)
            || room.thread.as_ref().is_some_and(|thread| self.scope.denied_destinations.contains(thread))
        {
            return None;
        }
        let mut current = match room.kind {
            RoomKind::Direct => RoomPolicy {
                tools: self.direct_tools,
                spoken_replies: self.direct_spoken_replies,
                ..RoomPolicy::default()
            },
            RoomKind::Group => match self.rooms.get(&room.chat) {
                Some(policy) => policy.clone(),
                None if self.rules(room).any(|rules| rules.enabled == Some(true)) => {
                    RoomPolicy::default()
                }
                None => return None,
            },
        };
        for rules in self.rules(room) {
            if rules.enabled == Some(false) {
                return None;
            }
            if let Some(mention) = rules.mention_required {
                current.mention_required = mention;
            }
            if let Some(split) = rules.per_participant {
                current.per_participant = split;
            }
            if let Some(tools) = rules.turn.tools {
                current.tools = tools;
            }
        }
        Some(current)
    }

    pub fn grants_sender(&self, room: &Room, sender: &Sender, paired: bool) -> bool {
        let grant = self.rules(room).filter_map(|rule| rule.grant.as_ref()).last();
        let allowed = grant.map_or_else(
            || paired || self.allows(sender),
            |grant| grant.open || (grant.paired && paired) || grant.users.contains(&sender.id)
                || !grant.roles.is_disjoint(&sender.roles),
        );
        self.room_policy(room).is_some()
            && (!self.scope.denied_users.contains(&sender.id)
                && self.scope.denied_roles.is_disjoint(&sender.roles))
            && allowed
            && self.rules(room).all(|rules| {
                !rules.denied_users.contains(&sender.id)
                    && rules.denied_roles.is_disjoint(&sender.roles)
                    && (rules.users.is_none() && rules.roles.is_none()
                        || rules
                            .users
                            .as_ref()
                            .is_some_and(|users| users.contains(&sender.id))
                        || rules
                            .roles
                            .as_ref()
                            .is_some_and(|roles| !roles.is_disjoint(&sender.roles)))
            })
    }

    pub(super) fn permits_membership_lookup(&self, room: &Room, actor: &str) -> bool {
        self.room_policy(room).is_some()
            && !self.scope.denied_users.contains(actor)
            && self.rules(room).all(|rule| !rule.denied_users.contains(actor))
    }

    pub fn allows_scoped_command(&self, room: &Room, actor: &Sender) -> bool {
        !self.scope.scoped_commands_disabled
            && self.requester_commands_for(room).is_none_or(|grant| {
                grant.users.contains(&actor.id) || !grant.roles.is_disjoint(&actor.roles)
            })
    }

    fn requester_commands_for(&self, room: &Room) -> Option<&RequesterCommandGrant> {
        self.rules(room).filter_map(|rule| rule.requester_commands.as_ref()).last()
    }

    pub fn requester_command_grant(
        &self, room: &Room, actor: &Sender,
    ) -> Option<&RequesterCommandGrant> {
        let grant = self.requester_commands_for(room)?;
        (self.room_policy(room).is_some()
            && !self.scope.denied_users.contains(&actor.id)
            && self.scope.denied_roles.is_disjoint(&actor.roles)
            && self.rules(room).all(|rule| !rule.denied_users.contains(&actor.id)
                && rule.denied_roles.is_disjoint(&actor.roles))
            && (grant.users.contains(&actor.id) || !grant.roles.is_disjoint(&actor.roles)))
            .then_some(grant)
    }

    pub fn turn_policy(&self, room: &Room) -> TurnPolicy {
        let mut result = TurnPolicy::default();
        for rules in self.rules(room) {
            result.overlay(&rules.turn);
        }
        result
    }

    pub fn delivery_policy(&self, room: &Room) -> DeliveryPolicy {
        self.rules(room)
            .filter_map(|rules| rules.delivery.as_ref())
            .last()
            .cloned()
            .unwrap_or_default()
    }
}
