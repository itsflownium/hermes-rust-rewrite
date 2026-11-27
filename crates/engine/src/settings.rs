//! Profile-scoped controls used by native sessions, including sessions opened
//! through the gateway. The field names correspond to the desktop settings.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, fs, io::Write, path::{Path,PathBuf}, sync::LazyLock, collections::BTreeMap};

pub static PERSONALITIES: LazyLock<BTreeMap<String,String>> = LazyLock::new(||
    serde_json::from_str(include_str!("personalities.json")).expect("built-in personality resources"));

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum MemoryProvider {
    #[default]
    Auto,
    Reliquary,
    Notes,
    External,
    Off,
    Plugin(String),
}

impl MemoryProvider {
    pub fn resolve(&self, native_enrolled: bool) -> Self {
        match self {
            Self::Auto if native_enrolled => Self::Reliquary,
            Self::Auto => Self::Notes,
            explicit => explicit.clone(),
        }
    }

    pub fn as_id(&self) -> Cow<'_, str> {
        match self {
            Self::Auto => "auto".into(),
            Self::Reliquary => "reliquary".into(),
            Self::Notes => "notes".into(),
            Self::External => "external".into(),
            Self::Off => "off".into(),
            Self::Plugin(id) => format!("plugin:{id}").into(),
        }
    }

    pub fn from_id(value: &str) -> Result<Self> {
        Ok(match value {
            "auto" => Self::Auto,
            "reliquary" => Self::Reliquary,
            "notes" => Self::Notes,
            "external" => Self::External,
            "off" => Self::Off,
            _ => {
                let id = value.strip_prefix("plugin:").context("Unknown memory provider")?;
                ensure!(crate::plugins::valid_name(id), "Invalid memory plugin ID");
                Self::Plugin(id.into())
            }
        })
    }
}

impl Serialize for MemoryProvider {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_id().as_ref())
    }
}

impl<'de> Deserialize<'de> for MemoryProvider {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Self::from_id(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl std::fmt::Display for MemoryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Auto => "Automatic",
            Self::Reliquary => "Reliquary",
            Self::Notes => "Markdown notes",
            Self::External => "External (MCP)",
            Self::Off => "Off",
            Self::Plugin(id) => id,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ProfileSettings {
    pub memory_provider: MemoryProvider,
    pub memory_provider_revision: u64,
    /// Reserve this profile's HTTP MCP connector for the External memory setting.
    pub external_memory_connector: bool,
    /// Legacy embedding selection used before the profile saves Switchboard routes.
    pub memory_embedding: Option<MemoryEmbedding>,
    /// Saved endpoint used for native Reliquary Perception decisions.
    pub memory_perception_endpoint: Option<String>,
    pub checkpoints_enabled: bool,
    pub checkpoint_max_snapshots: usize,
    pub redact_secrets: bool,
    pub clock_tools: bool,
    pub file_tools: bool,
    pub terminal_tools: bool,
    pub skill_tools: bool,
    pub memory_tool: bool,
    pub mcp_tools: bool,
    pub personality: String,
    pub timezone: String,
    pub default_directory: Option<PathBuf>,
    pub memory_enabled: bool,
    pub user_profile_enabled: bool,
    pub memory_char_limit: usize,
    pub user_char_limit: usize,
    pub approval_timeout: u64,
    /// `manual` asks before every tool; `smart` runs read-only tools and asks before
    /// changes; `off` never asks (approval-mode-menu.tsx).
    pub approval_mode: String,
    pub max_turns: usize,
    pub file_read_max_chars:usize,
    pub tool_output_bytes: usize,
    pub tool_output_lines: usize,
    pub tool_output_line_length: usize,
    pub auto_archive: bool,
    pub auto_archive_days: u32,
    pub keep_awake: bool,
    /// Which defaults the saved file was written with. Not a user setting.
    pub settings_version: u32,
}
/// Saved files hold every field, so a changed default must be told apart from a choice.
const SETTINGS_VERSION: u32 = 1;
const DEFAULT_MAX_TURNS: usize = 90;
/// The default before version 1, too few rounds for most tasks.
const FORMER_MAX_TURNS: usize = 8;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemoryEmbedding {
    pub endpoint_id: String,
    pub dimensions: u32,
}
impl Default for ProfileSettings {
    fn default()->Self {Self {memory_provider: MemoryProvider::Auto, memory_provider_revision: 0, external_memory_connector: false, memory_embedding: None, memory_perception_endpoint: None, checkpoints_enabled:false,checkpoint_max_snapshots:20,redact_secrets:true,clock_tools:true,terminal_tools:true,file_tools:true,skill_tools:true,memory_tool:true,mcp_tools:true, personality:"default".into(), timezone:String::new(), default_directory:None,
        memory_enabled:true,user_profile_enabled:true,memory_char_limit:crate::memory::DEFAULT_MEMORY_CHAR_LIMIT,
        user_char_limit:crate::memory::DEFAULT_USER_CHAR_LIMIT, approval_timeout:300, approval_mode:"smart".into(), max_turns:DEFAULT_MAX_TURNS, settings_version:SETTINGS_VERSION,
        file_read_max_chars:100_000,tool_output_bytes:50000,tool_output_lines:2000,tool_output_line_length:2000,
        auto_archive:false,auto_archive_days:3,keep_awake:false }}
}
impl ProfileSettings {
    pub fn load(root:&Path)->Result<Self>{
        match fs::File::open(root.join("agent-settings.json")) {
            Err(e) if e.kind()==std::io::ErrorKind::NotFound=>Ok(Self::default()),
            file=>{
                let saved:serde_json::Value=serde_json::from_reader(file?)?;
                let version=saved["settings_version"].as_u64().unwrap_or(0);
                let mut value:Self=serde_json::from_value(saved)?;
                if version==0 && value.max_turns==FORMER_MAX_TURNS {value.max_turns=DEFAULT_MAX_TURNS;}
                value.settings_version=SETTINGS_VERSION;
                value.validate()?;Ok(value)
            }
        }
    }
    pub fn validate(&self)->Result<()>{
        ensure!((1..=100).contains(&self.checkpoint_max_snapshots),"Checkpoint retention must be between 1 and 100 snapshots");
        ensure!(self.personality=="default" || PERSONALITIES.contains_key(&self.personality),"Unknown assistant personality");
        ensure!(self.timezone.is_empty() || self.timezone.parse::<chrono_tz::Tz>().is_ok(),"Enter an IANA timezone, for example Australia/Sydney");
        if let Some(dir)=&self.default_directory {ensure!(dir.is_absolute(),"The default project folder must be an absolute path");}
        ensure!((1..=1_000_000).contains(&self.memory_char_limit) && (1..=1_000_000).contains(&self.user_char_limit),"Memory limits must be between 1 and 1,000,000 characters");
        ensure!(["manual","smart","off"].contains(&self.approval_mode.as_str()),"Approval mode must be manual, smart or off");
        ensure!((1..=86400).contains(&self.approval_timeout),"Approval timeout must be between 1 and 86,400 seconds");
        ensure!((1..=1000).contains(&self.max_turns),"Maximum iterations must be between 1 and 1,000");
        ensure!((1..=1_000_000).contains(&self.file_read_max_chars),"File read limit must be between 1 and 1,000,000 characters");
        ensure!((512..=4_194_304).contains(&self.tool_output_bytes),"Tool output size must be between 512 bytes and 4 MiB");
        ensure!((1..=100_000).contains(&self.tool_output_lines) && (32..=1_000_000).contains(&self.tool_output_line_length),"Enter 1–100,000 output lines and 32–1,000,000 characters per line");
        ensure!((1..=36500).contains(&self.auto_archive_days),"Auto-archive requires a positive number of days (up to 36,500)");
        Ok(())
    }
    pub fn update(root:&Path, changes:&serde_json::Map<String, serde_json::Value>)->Result<Self>{
        Self::update_locked(root, |previous| {
            let mut current=serde_json::to_value(previous)?;
            let fields=current.as_object_mut().context("Invalid profile settings")?;
            for (key,value) in changes {
                ensure!(!matches!(key.as_str(),"memory_provider_revision"|"settings_version") && fields.contains_key(key),"Unknown profile setting {key}");
                fields.insert(key.clone(),value.clone());
            }
            let mut next:Self=serde_json::from_value(current)?;
            if next.memory_provider != previous.memory_provider {
                next.advance_memory_provider_revision()?;
            }
            Ok(next)
        })
    }

    /// Retain the saved choice while invalidating deliveries admitted under a removed activation.
    pub fn invalidate_plugin_memory(root: &Path, plugin_id: &str) -> Result<Self> {
        Self::update_locked(root, |previous| {
            let mut next = previous.clone();
            if matches!(&next.memory_provider, MemoryProvider::Plugin(id) if id == plugin_id) {
                next.advance_memory_provider_revision()?;
            }
            Ok(next)
        })
    }

    fn advance_memory_provider_revision(&mut self) -> Result<()> {
        self.memory_provider_revision = self.memory_provider_revision.checked_add(1)
            .context("Memory provider revision is exhausted")?;
        Ok(())
    }

    fn update_locked(root: &Path, modify: impl FnOnce(&Self) -> Result<Self>) -> Result<Self> {
        let lock=fs::OpenOptions::new().create(true).truncate(false).read(true).write(true).open(root.join("agent-settings.lock"))?;
        fs2::FileExt::lock_exclusive(&lock)?;
        let previous = Self::load(root)?;
        let next = modify(&previous)?;
        if next != previous {
            next.save(root)?;
        }
        Ok(next)
    }
    fn save(&self,root:&Path)->Result<()> {
        self.validate()?;
        let mut file=tempfile::NamedTempFile::new_in(root)?;
        serde_json::to_writer_pretty(&mut file,self)?; file.write_all(b"\n")?;file.as_file().sync_all()?;
        crate::history::persist_with_retry(file, &root.join("agent-settings.json"))
            .context("Could not save profile settings")?;
        Ok(())
    }
    /// Whether a tool call must wait for the user's answer under the approval mode.
    pub fn needs_approval(&self,name:&str,mutates:bool,external:bool)->bool {
        match self.approval_mode.as_str() {
            "off"=>false,
            "smart"=>mutates||external||name=="memory",
            _=>true,
        }
    }
    pub fn tool_enabled(&self,name:&str)->bool {
        match name {
            "get_current_time"=>self.clock_tools,
            "terminal"|"process_manage"=>self.terminal_tools,
            "list_directory"|"read_file"|"write_file"|"search_files"|crate::channel_attachments::TOOL=>self.file_tools,
            "skills_list"|"skill_view"=>self.skill_tools,
            "memory"|"memory_query"|"refine_query"|"traverse_graph"|"archive_search"|"archive_read"|"echo_read"=>self.memory_tool,
            "delegate_task"=>true,
            _=>self.mcp_tools,
        }
    }
    pub(crate) fn connect_profile_mcp(&self) -> bool {
        self.mcp_tools && (!self.external_memory_connector
            || (self.memory_provider == MemoryProvider::External && self.memory_tool))
    }

    pub(crate) fn memory_config(&self, scheduled: bool, native_enrolled: bool) -> crate::memory::MemoryConfig {
        let notes = !scheduled && self.memory_provider.resolve(native_enrolled) == MemoryProvider::Notes;
        crate::memory::MemoryConfig {
            memory_enabled: notes && self.memory_enabled,
            user_profile_enabled: notes && self.user_profile_enabled,
            memory_char_limit: self.memory_char_limit,
            user_char_limit: self.user_char_limit,
        }
    }
    pub(crate) fn bound_tool_result(&self,text:String)->String {
        let Ok(mut data)=serde_json::from_str::<serde_json::Value>(&text) else {return text;};
        fn clip(value:&mut serde_json::Value, settings:&ProfileSettings, clipped:&mut bool){
            match value {
                serde_json::Value::String(content) => {
                    if content.lines().count()<=settings.tool_output_lines && content.lines().all(|line|line.chars().count()<=settings.tool_output_line_length){return}
                    *content=content.lines().take(settings.tool_output_lines).map(|line|line.chars().take(settings.tool_output_line_length).collect::<String>()).collect::<Vec<_>>().join("\n");
                    *clipped=true;
                }
                serde_json::Value::Array(values)=>for value in values{clip(value,settings,clipped)},
                serde_json::Value::Object(values)=>for value in values.values_mut(){clip(value,settings,clipped)},
                _=>{}
            }
        }
        let mut clipped=false;clip(&mut data,self,&mut clipped);
        let encoded=if clipped {serde_json::json!({"truncated":true,"result":data}).to_string()}else{text};
        if encoded.len()<=self.tool_output_bytes { return encoded; }
        // Measure the escaped prefix against the serialized envelope. A quote,
        // backslash or control character can expand when embedded in JSON.
        let mut result=serde_json::json!({"truncated":true,"output":"","original_bytes":encoded.len()});
        let available=self.tool_output_bytes.saturating_sub(result.to_string().len());
        let mut used=0;
        let mut prefix=String::new();
        for ch in encoded.chars() {
            let bytes=match ch { '"' | '\\' | '\n' | '\r' | '\t' | '\u{0008}' | '\u{000c}'=>2, c if c<' '=>6, c=>c.len_utf8() };
            if used+bytes>available {break;}
            prefix.push(ch);used+=bytes;
        }
        result["output"]=prefix.into();
        result.to_string()
    }
}

#[cfg(test)]
mod memory_provider_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn existing_profile_keeps_its_notes_preferences_when_switching_providers() {
        let profile = tempfile::tempdir().unwrap();
        fs::write(profile.path().join("agent-settings.json"), r#"{"user_profile_enabled":false}"#).unwrap();
        let settings = ProfileSettings::load(profile.path()).unwrap();
        assert_eq!(settings.memory_provider, MemoryProvider::Auto);
        assert!(settings.memory_config(false, false).memory_enabled);
        assert!(!settings.memory_config(false, false).user_profile_enabled);
        assert!(!settings.memory_config(false, true).memory_enabled);

        for name in ["reliquary", "off", "external", "notes"] {
            let changes = json!({"memory_provider": name});
            ProfileSettings::update(profile.path(), changes.as_object().unwrap()).unwrap();
            let saved = ProfileSettings::load(profile.path()).unwrap();
            assert_eq!(json!(saved.memory_provider), name);
            for enrolled in [false, true] {
                let notes = saved.memory_config(false, enrolled);
                assert_eq!(notes.memory_enabled, name == "notes");
                assert!(!notes.user_profile_enabled);
                assert!(!saved.memory_config(true, enrolled).memory_enabled);
            }
        }
        let unknown = json!({"memory_provider": "typo"});
        assert!(ProfileSettings::update(profile.path(), unknown.as_object().unwrap()).is_err());
        assert_eq!(ProfileSettings::load(profile.path()).unwrap().memory_provider, MemoryProvider::Notes);
    }

    #[test]
    fn only_a_designated_memory_connector_follows_the_provider_choice() {
        let mut settings = ProfileSettings::default();
        for mode in [MemoryProvider::Auto, MemoryProvider::Reliquary, MemoryProvider::Notes,
                     MemoryProvider::Off, MemoryProvider::External, MemoryProvider::Plugin("recall".into())] {
            let external = mode == MemoryProvider::External;
            settings.memory_provider = mode;
            settings.external_memory_connector = false;
            assert!(settings.connect_profile_mcp());
            settings.external_memory_connector = true;
            assert_eq!(settings.connect_profile_mcp(), external);
        }
        settings.mcp_tools = false;
        assert!(!settings.connect_profile_mcp());
        settings.mcp_tools = true;
        settings.memory_tool = false;
        assert!(!settings.connect_profile_mcp());
        settings.external_memory_connector = false;
        assert!(settings.connect_profile_mcp());
    }

    #[test]
    fn selection_and_activation_changes_persist_a_new_revision() {
        let profile = tempfile::tempdir().unwrap();
        let root = profile.path();
        let select = |name: &str| {
            ProfileSettings::update(root, json!({"memory_provider": name}).as_object().unwrap())
        };
        let plugin = select("plugin:recall").unwrap();
        assert_eq!(plugin.memory_provider, MemoryProvider::Plugin("recall".into()));
        assert_eq!(plugin.memory_provider_revision, 1);
        assert_eq!(select("plugin:recall").unwrap().memory_provider_revision, 1);
        assert_eq!(select("off").unwrap().memory_provider_revision, 2);
        assert_eq!(select("plugin:recall").unwrap().memory_provider_revision, 3);
        assert_eq!(ProfileSettings::invalidate_plugin_memory(root, "other").unwrap().memory_provider_revision, 3);
        assert_eq!(ProfileSettings::invalidate_plugin_memory(root, "recall").unwrap().memory_provider_revision, 4);
        assert!(select("plugin:../recall").is_err());
        let saved = ProfileSettings::load(root).unwrap();
        assert_eq!(saved.memory_provider.as_id(), "plugin:recall");
        assert_eq!(saved.memory_provider_revision, 4);
    }
}

#[cfg(test)]
mod approval_tests {
    use super::*;
    #[test]
    fn the_former_iteration_limit_is_replaced_only_where_it_was_the_default() {
        let root=tempfile::tempdir().unwrap();
        let write=|text:&str|std::fs::write(root.path().join("agent-settings.json"),text).unwrap();
        write(r#"{"max_turns":8}"#);
        assert_eq!(ProfileSettings::load(root.path()).unwrap().max_turns,90);
        write(r#"{"max_turns":20}"#);
        assert_eq!(ProfileSettings::load(root.path()).unwrap().max_turns,20);
        let chosen=serde_json::json!({"max_turns":8});
        ProfileSettings::update(root.path(),chosen.as_object().unwrap()).unwrap();
        assert_eq!(ProfileSettings::load(root.path()).unwrap().max_turns,8);
    }
    #[test]
    fn approval_modes() {
        let mut s=ProfileSettings::default();
        assert_eq!(s.approval_mode,"smart");
        assert!(!s.needs_approval("read_file",false,false));
        assert!(s.needs_approval("write_file",true,false));
        assert!(s.needs_approval("remote_tool",false,true));
        assert!(s.needs_approval("memory",false,false));
        s.approval_mode="manual".into(); assert!(s.needs_approval("get_current_time",false,false));
        s.approval_mode="off".into(); assert!(!s.needs_approval("write_file",true,true));
        s.approval_mode="sometimes".into(); assert!(s.validate().is_err());
    }
}
