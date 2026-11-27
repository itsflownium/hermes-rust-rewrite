//! Profile-level methods of the published gateway contract, over the native library.
//! Reading needs a native session; changing the profile needs its private local client.
use super::*;
use hermes_engine::{cron, plugins, settings::ProfileSettings, skill_catalog, tools};

pub(super) const METHODS: &[&str] = &[
    "agents.list",
    "command.resolve",
    "complete.path",
    "config.show",
    "cron.manage",
    "i18n.languages",
    "insights.get",
    "llm.oneshot",
    "mcp.servers.list",
    "model.options",
    "plugins.list",
    "profiles.create",
    "profiles.describe",
    "profiles.list",
    "projects.create",
    "projects.for_cwd",
    "projects.get",
    "projects.list",
    "projects.set_active",
    "projects.update",
    "reload.mcp",
    "rollback.diff",
    "rollback.list",
    "rollback.restore",
    "session.archive",
    "session.compress",
    "session.context_breakdown",
    "session.delete",
    "session.most_recent",
    "session.usage",
    "setup.runtime_check",
    "setup.status",
    "skills.reload",
    "system.battery",
    "tools.list",
    "toolsets.list",
];

/// Methods that change the profile or the computer's files.
const PRIVATE: &[&str] = &[
    "profiles.create", "projects.create", "projects.set_active", "projects.update", "rollback.restore",
    "session.archive", "session.delete", "llm.oneshot", "complete.path", "rollback.diff",
];

/// Languages with a translation in the desktop app.
const LANGUAGES: &[(&str, &str)] = &[("en", "English")];

async fn blocking<T: Send + 'static>(work: impl FnOnce() -> Result<T> + Send + 'static) -> std::result::Result<T, RpcError> {
    tokio::task::spawn_blocking(work).await.map_err(RpcError::internal)?.map_err(RpcError::internal)
}

pub(super) async fn model_choices(config: hermes_engine::Config) -> (Vec<String>, Option<String>) {
    match hermes_engine::models::discover(config.clone()).await {
        Ok(models) => (models.into_iter().map(|model| model.id).collect(), None),
        Err(error) => (vec![config.model], Some(format!("{error:#}"))),
    }
}

fn epoch(stamp: &str) -> f64 {
    chrono::DateTime::parse_from_rfc3339(stamp).map_or(0.0, |at| at.timestamp_millis() as f64 / 1000.0)
}

fn project(value: &hermes_library::Project) -> Value {
    let slug: String = value.name.to_lowercase().chars().map(|c| if c.is_alphanumeric() { c } else { '-' }).collect();
    json!({"id":value.id, "slug":slug, "name":value.name, "primary_path":value.path, "archived":false, "created_at":0,
        "folders":[{"path":value.path}]})
}

/// Rough tokens, as the engine counts them.
fn tokens(bytes: usize) -> usize {
    bytes.div_ceil(4)
}

impl Shared {
    pub(super) async fn profile_method(self: &Arc<Self>, method: &str, params: &Value, peer: &Peer) -> RpcResult {
        let unknown = || RpcError::new(-32601, "method_not_found", format!("Method not implemented by the Rust gateway: {method}"));
        // Clients on any connection read -32601 as a missing method, so the name is checked before access.
        if !METHODS.contains(&method) {
            return Err(unknown());
        }
        if !peer.canonical {
            return Err(RpcError::unsupported("A native session is required"));
        }
        let changes = method == "cron.manage" && params["action"].as_str().is_some_and(|action| action != "list");
        if !peer.local_native && (changes || PRIVATE.contains(&method)) {
            return Err(RpcError::new(4403, "permission_denied", "This needs the private profile client"));
        }
        let library = self.library.clone();
        match method {
            "session.archive" => {
                let id = optional_string(params, "session_id")?.or(optional_string(params, "session_key")?).ok_or_else(|| RpcError::invalid("session_id is required"))?;
                let archived = params["archived"].as_bool().unwrap_or(true);
                self.catalog_edit("catalog.session.set", &json!({"session_id":id,"archived":archived}), peer).await?;
                Ok(json!({"archived":archived,"session_key":id}))
            }
            "session.delete" => {
                let id = required_string(params, "session_id")?;
                // The library deletes only archived chats, so memory providers see both steps.
                if self.library.session(id).is_ok_and(|summary| !summary.archived) {
                    self.catalog_edit("catalog.session.set", &json!({"session_id":id,"archived":true}), peer).await?;
                }
                self.catalog_edit("catalog.session.delete", &json!({"session_id":id}), peer).await?;
                Ok(json!({"deleted":id}))
            }
            "session.most_recent" => {
                let sessions = blocking(move || library.sessions(None)).await?;
                let latest = sessions.into_iter().filter(|s| !s.archived && s.bot.is_none()).max_by(|a, b| a.recency().cmp(b.recency()))
                    .ok_or_else(|| RpcError::new(4004, "not_found", "There are no conversations"))?;
                Ok(json!({"session_id":latest.id,"title":latest.title,"started_at":epoch(&latest.created_at),"source":"rust"}))
            }
            "session.compress" => self.compress_session(params, peer).await,
            "session.usage" | "session.context_breakdown" => {
                let session = self.resolve(params).await?;
                let model = lock(&session.config).model.clone();
                let history = lock(&session.view).history.clone();
                let summarized = history.active_summary().map_or(0, |summary| summary.through);
                let summary = history.active_summary().map_or(0, |summary| tokens(summary.text.len()));
                let (mut messages, mut results) = (0, 0);
                for turn in &history.turns[summarized..] {
                    messages += tokens(turn.prompt.len() + turn.context.len());
                    for entry in &turn.entries {
                        match entry {
                            Entry::User { text } | Entry::Assistant { text, .. } => messages += tokens(text.len()),
                            Entry::Tool { result, .. } => results += tokens(result.len()),
                        }
                    }
                }
                let definitions = tokens(serde_json::to_string(&tools::tool_specs()).unwrap_or_default().len());
                let used = messages + results + summary + definitions;
                let window = hermes_engine::context_window(&model);
                let percent = used * 100 / window.max(1);
                if method == "session.usage" {
                    return Ok(json!({"model":model,"calls":history.turns.len(),"compressions":usize::from(summarized > 0),
                        "context_used":used,"context_max":window,"context_percent":percent,"context_source":"estimate","context_estimated":true}));
                }
                let category = |id: &str, label: &str, color: &str, tokens: usize| json!({"id":id,"label":label,"color":color,"tokens":tokens});
                Ok(json!({"categories":[category("messages", "Messages", "blue", messages), category("tools", "Tool results", "green", results),
                    category("summary", "Summary", "purple", summary), category("definitions", "Tool definitions", "gray", definitions)],
                    "context_used":used,"context_max":window,"context_percent":percent,"estimated_total":used,"context_estimated":true,"context_source":"estimate","model":model}))
            }
            "profiles.list" => {
                let profiles = blocking(move || library.profiles()).await?;
                Ok(json!({"profiles":profiles.iter().filter(|p| !p.is_bot).map(|p| json!({"name":p.name,"path":p.path,"is_default":p.is_default,"model":p.model,"skill_count":p.skill_count})).collect::<Vec<_>>(),"bot_mode_protocol":false}))
            }
            "profiles.create" => {
                let name = required_string(params, "name")?.to_owned();
                let soul = optional_string(params, "soul")?.map(str::to_owned);
                let (profile, wrote) = blocking(move || {
                    let profile = library.create_profile(&name)?;
                    if let Some(soul) = &soul { library.set_profile_soul(&name, soul)?; }
                    Ok((profile, soul.is_some()))
                }).await?;
                Ok(json!({"ok":true,"name":profile.name,"path":profile.path,"soul_written":wrote,"model_set":false,"mirrored":{},"warnings":[]}))
            }
            "profiles.describe" => {
                let name = optional_string(params, "name")?.unwrap_or(&self.profile_name).to_owned();
                blocking(move || {
                    let home = library.profile_path(&name)?;
                    ensure!(home.is_dir(), "No such profile");
                    let profile = library.profiles()?.into_iter().find(|p| p.name == name);
                    let skills = skill_catalog::status(&home)?;
                    let settings = ProfileSettings::load(&home)?;
                    Ok(json!({"name":name,"soul":library.profile_soul(&name)?,"model":{"default":profile.map(|p| p.model).unwrap_or_default()},
                        "skills":skills.installed.iter().map(|id| json!({"name":id,"enabled":!skills.disabled.contains(id)})).collect::<Vec<_>>(),
                        "toolsets":toolsets(&settings)}))
                }).await
            }
            "projects.list" => {
                let (projects, active) = blocking(move || Ok((library.projects()?, library.selected_project()?))).await?;
                Ok(json!({"projects":projects.iter().map(project).collect::<Vec<_>>(),"active_id":active.map(|p| p.id)}))
            }
            "projects.get" => {
                let id = required_string(params, "id")?.to_owned();
                let found = blocking(move || library.projects()).await?.into_iter().find(|p| p.id == id);
                found.map(|p| json!({"project":project(&p)})).ok_or_else(|| RpcError::new(4004, "not_found", "Project not found"))
            }
            "projects.create" => {
                let name = optional_string(params, "name")?.map(str::to_owned);
                let path = optional_string(params, "primary_path")?.or(params["folders"][0].as_str())
                    .ok_or_else(|| RpcError::invalid("A project folder is required"))?.to_owned();
                let select = params["use"].as_bool().unwrap_or(false);
                let created = blocking(move || {
                    let mut created = library.add_project(Path::new(&path))?;
                    if let Some(name) = name.filter(|name| !name.trim().is_empty()) { created = library.rename_project(&created.id, &name)?; }
                    if select { library.select_project(Some(&created.id))?; }
                    Ok(created)
                }).await?;
                Ok(json!({"project":project(&created)}))
            }
            "projects.update" => {
                let (id, name) = (required_string(params, "id")?.to_owned(), required_string(params, "name")?.to_owned());
                let renamed = blocking(move || library.rename_project(&id, &name)).await?;
                Ok(json!({"project":project(&renamed)}))
            }
            "projects.set_active" => {
                let id = optional_string(params, "id")?.map(str::to_owned);
                let selected = id.clone();
                blocking(move || library.select_project(selected.as_deref())).await?;
                Ok(json!({"active_id":id}))
            }
            "projects.for_cwd" => {
                let cwd = optional_string(params, "cwd")?.unwrap_or_default().to_owned();
                let folder = cwd.clone();
                let found = blocking(move || {
                    let folder = Path::new(&folder).canonicalize().unwrap_or_else(|_| folder.into());
                    Ok(library.projects()?.into_iter().filter(|p| folder.starts_with(&p.path)).max_by_key(|p| p.path.as_os_str().len()))
                }).await?;
                Ok(json!({"project":found.as_ref().map(project),"cwd":cwd}))
            }
            "tools.list" | "toolsets.list" => {
                let settings = blocking(move || ProfileSettings::load(library.root())).await?;
                Ok(json!({"toolsets":toolsets(&settings)}))
            }
            "plugins.list" => {
                let installed = blocking(move || plugins::installed(library.root())).await?;
                let inventory = self.plugins.inventory().await;
                Ok(json!({"plugins":installed.iter().map(|plugin| {
                    let live = inventory.iter().find(|status| status.id == plugin.name);
                    json!({"name":plugin.name,"version":plugin.version,"enabled":plugin.enabled,"key":plugin.name,
                        "commands":live.map_or(0, |status| status.commands.len()),"error":live.and_then(|status| status.issue.clone())})
                }).collect::<Vec<_>>(),"toolsets":[],"portable_mcp_servers":installed.iter().filter(|p| p.enabled).flat_map(|p| p.servers.clone()).filter(|kind| kind != "python").collect::<Vec<_>>()}))
            }
            "mcp.servers.list" => {
                let mut servers = Vec::new();
                let root = library.root().to_owned();
                let summary = blocking(move || hermes_engine::connector::summary(&root)).await?;
                if !summary.url.is_empty() {
                    servers.push(json!({"name":"mcp","transport":"http","url":summary.url,"args":[],"env":[],"auth":if summary.has_token {"bearer"} else {"none"},"enabled":true,"source":"profile"}));
                }
                for plugin in blocking(move || plugins::installed(library.root())).await? {
                    for server in plugin.servers.iter().filter(|kind| *kind != "python") {
                        servers.push(json!({"name":server,"transport":"plugin","args":[],"env":[],"enabled":plugin.enabled,"source":"plugin","plugin":plugin.name}));
                    }
                }
                Ok(json!({"servers":servers}))
            }
            "model.options" => {
                let config = self.provider.clone();
                let (listed, warning) = model_choices(config.clone()).await;
                Ok(json!({"providers":[{"slug":"current","name":config.base_url,"total_models":listed.len(),"models":listed,"is_current":true,"api_url":config.base_url,"authenticated":true,"warning":warning}],"model":config.model,"provider":"current"}))
            }
            "setup.status" => {
                let ready = self.provider.validate().is_ok();
                Ok(json!({"provider_configured":ready,"ready":ready,"profile":self.profile_name,"ok":true}))
            }
            "setup.runtime_check" => Ok(match self.provider.validate() {
                Ok(()) => json!({"ok":true,"model":self.provider.model,"source":"profile","profile":self.profile_name}),
                Err(error) => json!({"ok":false,"error":error.to_string(),"error_code":"provider_unconfigured","profile":self.profile_name}),
            }),
            "config.show" => {
                let settings = blocking(move || ProfileSettings::load(library.root())).await?;
                let value = serde_json::to_value(settings).map_err(RpcError::internal)?;
                let rows = value.as_object().into_iter().flatten().map(|(key, value)| json!([key, value.as_str().map_or_else(|| value.to_string(), str::to_owned)])).collect::<Vec<_>>();
                Ok(json!({"sections":[{"title":"Profile","rows":rows}]}))
            }
            "llm.oneshot" => {
                let input = optional_string(params, "input")?.unwrap_or_default().to_owned();
                let instructions = optional_string(params, "instructions")?.unwrap_or("Answer the request.").to_owned();
                if input.is_empty() || input.len() + instructions.len() > 256 * 1024 {
                    return Err(RpcError::invalid("Enter up to 256 KiB of input"));
                }
                let text = hermes_engine::oneshot(self.provider.clone(), &instructions, &input).await.map_err(RpcError::internal)?;
                Ok(json!({"text":text}))
            }
            "cron.manage" => self.manage_cron(params).await,
            "rollback.list" | "rollback.diff" | "rollback.restore" => {
                let session = self.attached(params, peer).await?;
                let workspace = lock(&session.settings).cwd.clone();
                let (method, hash) = (method.to_owned(), optional_string(params, "hash")?.map(str::to_owned));
                let whole = params["file_path"].is_null();
                blocking(move || {
                    let settings = ProfileSettings::load(library.root())?;
                    if !settings.checkpoints_enabled {
                        return Ok(json!({"enabled":false,"checkpoints":[],"success":false,"reason":"Checkpoints are off for this profile"}));
                    }
                    let store = hermes_engine::checkpoint::Store::open(library.root(), settings.checkpoint_max_snapshots)?;
                    // Only this conversation's folder: another chat's files are not its to change.
                    let own: Vec<_> = store.list()?.into_iter().filter(|item| workspace.as_ref().is_some_and(|folder| item.workspace == *folder)).collect();
                    if method == "rollback.list" {
                        return Ok(json!({"enabled":true,"checkpoints":own.iter().map(|item| json!({"hash":item.id,"timestamp":item.created_at,"message":item.label})).collect::<Vec<_>>()}));
                    }
                    let hash = hash.context("hash is required")?;
                    ensure!(own.iter().any(|item| item.id == hash), "No such checkpoint in this conversation");
                    let plan = store.prepare_restore(&hash)?;
                    if method == "rollback.diff" {
                        return Ok(json!({"stat":plan.description,"diff":plan.preview}));
                    }
                    ensure!(whole, "Checkpoints hold one file each; restore it without file_path");
                    let restored = store.restore(&plan, true)?;
                    Ok(json!({"success":true,"restored_to":restored.id,"file":restored.path,"restored_files":[restored.path]}))
                }).await
            }
            "complete.path" => {
                let word = optional_string(params, "word")?.unwrap_or_default().to_owned();
                let base = match optional_string(params, "cwd")? {
                    Some(cwd) => PathBuf::from(cwd),
                    None => library.selected_project().map_err(RpcError::internal)?.map(|p| p.path).unwrap_or_else(|| library.root().to_owned()),
                };
                let items = blocking(move || Ok(complete_path(&base, &word))).await?;
                Ok(json!({"items":items}))
            }
            "command.resolve" => {
                let name = required_string(params, "name")?;
                let catalog = self.commands_catalog(params, peer).await?;
                let path = format!("/{}", name.trim_start_matches('/').to_ascii_lowercase().replace('_', "-"));
                let found = &catalog["commands"][&path];
                if found.is_null() { return Err(RpcError::new(4004, "unknown_command", "No such command")); }
                Ok(json!({"canonical":path,"description":found["description"],"category":"Plugin commands"}))
            }
            "skills.reload" => {
                let status = blocking(move || skill_catalog::status(library.root())).await?;
                // Skills are read for every turn, so there is nothing cached to drop.
                Ok(json!({"output":format!("{} skills installed", status.installed.len()),"result":{"added":[],"removed":[],"total":status.installed.len()}}))
            }
            "reload.mcp" => Ok(json!({"status":"reloaded","message":"MCP servers are connected for each turn"})),
            "agents.list" => {
                let live: Vec<_> = self.sessions.lock().await.values().cloned().collect();
                let mut rows = Vec::new();
                for session in live {
                    for row in session.processes.list().await.map_err(RpcError::internal)? {
                        rows.push(json!({"session_id":row["session_id"],"command":row["command"],"status":row["status"],"uptime":row["uptime"].as_u64().unwrap_or(0)}));
                    }
                }
                Ok(json!({"processes":rows}))
            }
            "insights.get" => {
                let days = params["days"].as_u64().unwrap_or(30).clamp(1, 3650);
                let since = chrono::Utc::now() - chrono::Duration::days(days as i64);
                blocking(move || {
                    let recent: Vec<_> = library.sessions(None)?.into_iter().filter(|s| epoch(s.recency()) >= since.timestamp() as f64).collect();
                    let messages: usize = recent.iter().filter_map(|s| hermes_engine::read_history(&s.directory).ok())
                        .map(|history| history.turns.iter().map(|turn| 1 + turn.entries.len()).sum::<usize>()).sum();
                    Ok(json!({"days":days,"sessions":recent.len(),"messages":messages}))
                }).await
            }
            "i18n.languages" => Ok(json!({"languages":LANGUAGES.iter().map(|(id, name)| json!({"id":id,"endonym":name,"rtl":false,"source":"bundled","surfaces":["desktop"]})).collect::<Vec<_>>()})),
            "system.battery" => Ok(blocking(|| Ok(battery())).await?.unwrap_or_else(|| json!({"available":false}))),
            _ => Err(unknown()),
        }
    }

    async fn compress_session(self: &Arc<Self>, params: &Value, peer: &Peer) -> RpcResult {
        let session = self.attached(params, peer).await?;
        let mut engine = session.engine.try_lock().map_err(|_| RpcError::new(4001, "session_busy", "Wait for the current response"))?;
        let config = lock(&session.config).clone();
        let before = engine.history().turns.len();
        let mut history = engine.history().clone();
        let changed = hermes_engine::compress_history(&mut history, config, self.library.root()).await.map_err(RpcError::internal)?;
        if changed {
            let mut view = lock(&session.view);
            if view.running || view.history.turns != history.turns {
                return Err(RpcError::new(4001, "session_busy", "The conversation changed while it was being summarized"));
            }
            view.authority.persist(&session.writer, Some(&history)).map_err(RpcError::internal)?;
            engine.accept_committed_history(history.clone());
            view.history = history.clone();
            view.revision += 1;
        }
        let through = history.active_summary().map_or(0, |summary| summary.through);
        Ok(json!({"status":if changed {"compressed"} else {"noop"},"compressed":changed,"removed":0,"before_messages":before,"after_messages":before - through,
            "summary":{"noop":!changed,"headline":if changed {format!("Summarized {through} earlier messages")} else {"Nothing to summarize".into()}}}))
    }

    async fn manage_cron(self: &Arc<Self>, params: &Value) -> RpcResult {
        let store = self.scheduler.clone();
        let action = optional_string(params, "action")?.unwrap_or("list").to_owned();
        let name = optional_string(params, "name")?.map(str::to_owned);
        let disabled = params["include_disabled"].as_bool().unwrap_or(true);
        let deliver = optional_string(params, "deliver")?.unwrap_or("local");
        if let Some(route) = deliver.strip_prefix("channel:")
            && !self.messaging.authorized_route(route).map_err(RpcError::internal)?
        {
            return Err(RpcError::invalid("Choose a route on this profile's messaging gateway"));
        }
        let draft = cron::Draft {
            deliver: deliver.to_owned(),
            name: name.clone().unwrap_or_default(),
            prompt: optional_string(params, "prompt")?.unwrap_or_default().into(),
            schedule: optional_string(params, "schedule")?.unwrap_or("0 9 * * *").into(),
            repeat: params["repeat"].as_u64().and_then(|n| u32::try_from(n).ok()),
            ..Default::default()
        };
        let shared = self.clone();
        let result = blocking(move || {
            let find = || -> Result<String> {
                let wanted = name.as_deref().context("name is required")?;
                let jobs = store.jobs()?;
                Ok(jobs.iter().find(|job| job.id == wanted || job.name == wanted).context("No such job")?.id.clone())
            };
            let now = chrono::Utc::now();
            let id = match action.as_str() {
                "list" => None,
                "add" | "create" => Some(store.create(draft, now)?.id),
                "pause" => { let id = find()?; store.pause(&id, now)?; Some(id) }
                "resume" => { let id = find()?; store.resume(&id, now)?; Some(id) }
                "remove" | "delete" => { let id = find()?; store.remove(&id)?; Some(id) }
                "run" => {
                    let id = find()?;
                    let _admissions = lock(&shared.admissions);
                    ensure!(!shared.shutdown.is_cancelled(), "Gateway is shutting down");
                    store.claim_gateway_now(&id, &new_id(), now)?;
                    Some(id)
                }
                other => anyhow::bail!("Unknown cron action {other}"),
            };
            let jobs: Vec<_> = store.jobs()?.into_iter().filter(|job| disabled || job.enabled).map(|job| json!({
                "job_id":job.id,"name":job.name,"prompt_preview":job.prompt.chars().take(120).collect::<String>(),"model":job.model,"provider":job.provider,
                "schedule":job.draft().schedule,"repeat":job.repeat.times,"deliver":job.deliver,"next_run_at":job.next_run_at,"last_run_at":job.last_run_at,
                "last_status":job.last_status,"last_error":job.last_error,"enabled":job.enabled,"state":job.state,"paused_at":job.paused_at,"workdir":job.workdir,
                "script":job.draft().script,
            })).collect();
            Ok(json!({"success":true,"count":jobs.len(),"jobs":jobs,"job_id":id,"gateway_running":true}))
        }).await?;
        self.scheduler_notify.notify_one();
        Ok(result)
    }
}

fn toolsets(settings: &ProfileSettings) -> Vec<Value> {
    let groups: [(&str, &str, &[&str]); 4] = [
        ("file", "Read and change files in the project folder", &["list_directory", "read_file", "write_file", "search_files"]),
        ("terminal", "Run commands", &["terminal", "process_manage"]),
        ("skills", "Use installed skills", &["skills_list", "skill_view"]),
        ("memory", "Remember across conversations", &["memory"]),
    ];
    groups.iter().map(|(name, description, names)| json!({"name":name,"description":description,"tool_count":names.len(),
        "enabled":settings.tool_enabled(names[0]),"tools":names})).collect()
}

fn complete_path(base: &Path, word: &str) -> Vec<Value> {
    let word = word.trim_start_matches('@');
    let (folder, prefix) = word.rsplit_once('/').map_or(("", word), |(folder, prefix)| (folder, prefix));
    let mut names: Vec<_> = fs::read_dir(base.join(folder)).into_iter().flatten().flatten()
        .map(|entry| (entry.file_name().to_string_lossy().into_owned(), entry.path().is_dir()))
        .filter(|(name, _)| name.to_lowercase().starts_with(&prefix.to_lowercase()) && (prefix.starts_with('.') || !name.starts_with('.')))
        .collect();
    names.sort();
    names.into_iter().take(50).map(|(name, dir)| {
        let path = if folder.is_empty() { name.clone() } else { format!("{folder}/{name}") };
        json!({"text":if dir { format!("{path}/") } else { path },"display":name,"kind":if dir {"folder"} else {"file"}})
    }).collect()
}

#[cfg(target_os = "macos")]
fn battery() -> Option<Value> {
    let output = std::process::Command::new("/usr/bin/pmset").args(["-g", "batt"]).output().ok()?;
    battery_report(&String::from_utf8_lossy(&output.stdout))
}

#[cfg(any(target_os = "macos", test))]
fn battery_report(text: &str) -> Option<Value> {
    let percent = text.split('%').next()?.rsplit(|c: char| !c.is_ascii_digit()).next()?.parse::<u8>().ok()?;
    Some(json!({"available":true,"percent":percent,"plugged":text.contains("AC Power")}))
}

#[cfg(target_os = "linux")]
fn battery() -> Option<Value> {
    let mut plugged = false;
    let mut percent = None;
    for entry in fs::read_dir("/sys/class/power_supply").ok()?.flatten() {
        let read = |name: &str| fs::read_to_string(entry.path().join(name)).map(|text| text.trim().to_owned()).unwrap_or_default();
        match read("type").as_str() {
            "Battery" => percent = percent.or_else(|| read("capacity").parse::<u8>().ok()),
            "Mains" | "USB" => plugged |= read("online") == "1",
            _ => {}
        }
    }
    percent.map(|percent| json!({"available":true,"percent":percent,"plugged":plugged}))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn battery() -> Option<Value> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(local_native: bool) -> Peer {
        let (tx, _rx) = mpsc::channel(64);
        Peer { id: new_id(), tx, stop: CancellationToken::new(), canonical: true, local_native }
    }

    #[tokio::test]
    async fn cron_manage_refuses_a_new_run_after_the_owner_starts_draining() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config { base_url: "http://127.0.0.1:1/v1".into(), model: "fixture".into(), api_key: String::new() };
        let gateway = Gateway::start(GatewayConfig::new(dir.path(), config)).await.unwrap();
        let job = gateway.shared.scheduler.create(cron::Draft {
            prompt: "report".into(), paused: true, ..Default::default()
        }, chrono::Utc::now()).unwrap();
        {
            let _admissions = lock(&gateway.shared.admissions);
            gateway.shared.shutdown.cancel();
        }
        let error = gateway.shared.dispatch("cron.manage", &json!({"action":"run","name":job.id}), &peer(true)).await.unwrap_err();
        assert!(error.message.contains("shutting down"));
        assert!(gateway.shared.scheduler.runs(&job.id).unwrap().is_empty());
        gateway.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn every_advertised_method_answers_in_its_published_shape() {
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("Design");
        fs::create_dir(&folder).unwrap();
        fs::write(folder.join("notes.md"), "x").unwrap();
        let config = Config { base_url: "http://127.0.0.1:1/v1".into(), model: "fixture".into(), api_key: String::new() };
        let gateway = Gateway::start(GatewayConfig::new(dir.path().join("data"), config)).await.unwrap();
        let shared = gateway.shared.clone();
        let local = peer(true);
        let call = async |method: &str, params: Value| shared.dispatch(method, &params, &local).await;

        let made = call("projects.create", json!({"name":"Design work","folders":[folder],"use":true})).await.unwrap();
        let id = made["project"]["id"].as_str().unwrap().to_owned();
        assert_eq!(call("projects.list", json!({})).await.unwrap()["active_id"], id);
        assert_eq!(call("projects.update", json!({"id":id,"name":"Renamed"})).await.unwrap()["project"]["name"], "Renamed");
        assert_eq!(call("projects.get", json!({"id":id})).await.unwrap()["project"]["name"], "Renamed");
        assert_eq!(call("projects.for_cwd", json!({"cwd":folder})).await.unwrap()["project"]["id"], id);
        assert_eq!(call("complete.path", json!({"cwd":folder,"word":"@no"})).await.unwrap()["items"][0]["text"], "notes.md");
        call("projects.set_active", json!({"id":null})).await.unwrap();

        assert!(call("profiles.create", json!({"name":"work","soul":"Be brief."})).await.unwrap()["soul_written"] == true);
        assert_eq!(call("profiles.describe", json!({"name":"work"})).await.unwrap()["soul"], "Be brief.");
        assert!(call("profiles.list", json!({})).await.unwrap()["profiles"].as_array().unwrap().iter().any(|p| p["name"] == "work"));

        let job = call("cron.manage", json!({"action":"create","name":"daily","prompt":"report","schedule":"0 9 * * *"})).await.unwrap();
        assert_eq!(job["jobs"][0]["name"], "daily");
        assert_eq!(call("cron.manage", json!({"action":"pause","name":"daily"})).await.unwrap()["jobs"][0]["enabled"], false);
        assert_eq!(call("cron.manage", json!({"action":"remove","name":"daily"})).await.unwrap()["count"], 0);

        let session = call("session.create", json!({"title":"Chat"})).await.unwrap()["session_id"].as_str().unwrap().to_owned();
        call("session.resume", json!({"session_id":session})).await.unwrap();
        let of = || json!({"session_id":session});
        assert_eq!(call("session.most_recent", json!({})).await.unwrap()["session_id"], session);
        assert_eq!(call("session.usage", of()).await.unwrap()["context_estimated"], true);
        assert_eq!(call("session.context_breakdown", of()).await.unwrap()["categories"].as_array().unwrap().len(), 4);
        assert_eq!(call("session.compress", of()).await.unwrap()["status"], "noop");
        assert_eq!(call("rollback.list", of()).await.unwrap()["enabled"], false);
        call("session.close", of()).await.unwrap();
        assert_eq!(call("session.archive", of()).await.unwrap()["archived"], true);
        assert_eq!(call("session.delete", of()).await.unwrap()["deleted"], session);

        let unanswered = ["llm.oneshot", "command.resolve"];
        for method in METHODS.iter().filter(|name| !name.starts_with("session.") && !name.starts_with("rollback.") && !unanswered.contains(name)
            && !["profiles.create", "projects.create", "projects.update", "projects.get"].contains(name))
        {
            call(method, json!({})).await.unwrap_or_else(|error| panic!("{method}: {}", error.message));
        }

        // Another authenticated client may look, and may not change the profile.
        let other = peer(false);
        for (method, params) in [("projects.create", json!({"folders":[folder]})), ("profiles.create", json!({"name":"x"})), ("cron.manage", json!({"action":"run","name":"a"})), ("session.delete", of())] {
            assert!(shared.dispatch(method, &params, &other).await.is_err(), "{method}");
        }
        assert!(shared.dispatch("projects.list", &json!({}), &other).await.is_ok());
        assert_eq!(METHODS.len(), METHODS.iter().collect::<std::collections::BTreeSet<_>>().len());
        gateway.shutdown().await.unwrap();
    }

    #[test]
    fn reads_the_battery_report() {
        let text = "Now drawing from 'AC Power'\n -InternalBattery-0 (id=1)\t87%; charging; 0:40 remaining";
        assert_eq!(battery_report(text).unwrap(), json!({"available":true,"percent":87,"plugged":true}));
        assert!(battery_report("Now drawing from 'AC Power'").is_none());
    }
}
