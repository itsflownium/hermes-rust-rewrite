use crate::{Connection, lock};
use hermes_gateway_core::messaging::{ConnectionFailure, Ingress};
use serde_json::json;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock, Weak},
};

#[derive(Clone)]
pub(crate) struct Member {
    pub connection: Arc<Connection>,
    pub ingress: Ingress,
}

#[derive(Default)]
struct Members {
    ready: bool,
    by_workspace: BTreeMap<String, Member>,
}

#[derive(Default)]
pub(crate) struct Hub(Mutex<Members>);

impl Hub {
    pub fn ready(&self) -> bool {
        lock(&self.0).ready
    }

    pub fn set_ready(&self, ready: bool) {
        let mut members = lock(&self.0);
        members.ready = ready;
        for member in members.by_workspace.values() {
            member.set_ready(ready);
        }
    }

    pub fn member(&self, team: &str) -> Option<Member> {
        let entry = lock(&self.0).by_workspace.get(team).cloned()?;
        entry.ingress.checked(true, |_| Ok(())).ok()?;
        Some(entry)
    }
}

impl Member {
    fn set_ready(&self, ready: bool) {
        let inactive = crate::intake::is_inactive(&self.connection, &self.ingress);
        let ready = ready && matches!(inactive, Ok(false));
        if self.ingress.report_transport_ready(ready).is_ok() {
            let mut status = lock(&self.connection.verification);
            if inactive == Ok(true) {
                status["socket"] = json!("uninstalled");
            } else if ready || status["socket"] == "ready" {
                status["socket"] = json!(if ready { "ready" } else { "waiting" });
            }
        }
    }
}

type HubKey = (PathBuf, String, Option<String>);
static HUBS: OnceLock<Mutex<BTreeMap<HubKey, Weak<Hub>>>> = OnceLock::new();

pub(crate) struct Registration {
    pub hub: Arc<Hub>,
    member: Member,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let workspace = &self.member.connection.id.team;
        let mut members = lock(&self.hub.0);
        if members
            .by_workspace
            .get(workspace)
            .is_some_and(|registered| Arc::ptr_eq(&registered.connection, &self.member.connection))
        {
            members.by_workspace.remove(workspace);
        }
    }
}

pub(crate) fn register(
    connection: Arc<Connection>,
    ingress: Ingress,
) -> Result<Registration, ConnectionFailure> {
    let key = (
        connection.root.clone(),
        connection.id.app.clone(),
        connection.settings.api_origin.clone(),
    );
    let hub = {
        let mut registry = lock(HUBS.get_or_init(Mutex::default));
        registry.retain(|_, hub| hub.strong_count() != 0);
        if let Some(hub) = registry.get(&key).and_then(Weak::upgrade) {
            hub
        } else {
            let hub = Arc::new(Hub::default());
            registry.insert(key, Arc::downgrade(&hub));
            hub
        }
    };
    let member = Member {
        connection,
        ingress,
    };
    let workspace = member.connection.id.team.clone();
    {
        let mut members = lock(&hub.0);
        if members
            .by_workspace
            .get(&workspace)
            .is_some_and(|current| current.ingress.checked(true, |_| Ok(())).is_ok())
        {
            return Err(ConnectionFailure::Configuration);
        }
        member.set_ready(members.ready);
        members.by_workspace.insert(workspace, member.clone());
    }
    Ok(Registration { hub, member })
}
