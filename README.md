# Hermes Desktop — Rust rewrite

A native rewrite of the [Hermes Agent](https://github.com/NousResearch/hermes-agent) desktop app in Rust. The UI is [Iced](https://iced.rs) and the agent engine (turns, streaming, tools, approvals, MCP, subagents) is Rust too, so the app is one native program: no Electron, Tauri, Node or bundled Chromium, and no separate Python backend to install. The only Python is the CPython interpreter embedded to run Python plugins (see [Python plugins](#python-plugins)).

This is an independent community project, not an official Nous Research release. The upstream pull request is [NousResearch/hermes-agent#159125](https://github.com/NousResearch/hermes-agent/pull/159125).

## Download

| Platform | Download |
| --- | --- |
| macOS (Apple Silicon) | [Hermes-macOS.zip](https://github.com/itsflownium/hermes-rust-rewrite/releases/latest/download/Hermes-macOS.zip) |
| Windows (x64) | [Hermes-windows-AMD64.zip](https://github.com/itsflownium/hermes-rust-rewrite/releases/latest/download/Hermes-windows-AMD64.zip) |
| Linux (x86_64) | [Hermes-linux-x86_64.tar.gz](https://github.com/itsflownium/hermes-rust-rewrite/releases/latest/download/Hermes-linux-x86_64.tar.gz) |

Every build comes from [the CI workflow](.github/workflows/build.yml), which runs the test suite on all three platforms first. No build is notarized or signed with a developer certificate. On macOS, the first time you open the app, right-click it and choose **Open**. On Linux, install WebKitGTK 4.1 and GTK 3 from your distribution.

## Rust vs Electron benchmark

The same machine, the same source baseline, and a returning user in both cases (each app's data folder was isolated and already set up).

| | Electron app (stock) | Rust rewrite | Difference |
| --- | --- | --- | --- |
| Download (zipped app) | 148 MB | **39 MB** | 3.8× smaller |
| App on disk | 346 MB | **65 MB** | 5.3× smaller |
| Installed on first launch | ≈1.2 GB Python runtime, tools and environment | **nothing** | — |
| Time to first window | 2.36 s | **1.73 s** | 27% faster |
| Memory at idle (app) | 262 MB | **214 MB** | |
| Memory at idle (app + its backend) | **563 MB** (262 + 301 for the Python gateway) | **214 MB** | 2.6× less |
| Idle CPU (app + backend) | 4.5% (0.4% + 4.1% gateway) | **1.2%** | 3.8× less |
| Processes | 6 (4 Electron + gateway + launcher) | **1** | |

Measured on an Apple M4 with 16 GB of memory and macOS 27.0.1. Each figure is the median of runs 2–6 (run 1 is a discarded warm-up). The raw results are in [`benchmarks/results/`](benchmarks/results).

**How it is measured** ([`benchmarks/bench.swift`](benchmarks/bench.swift)):

- **Time to first window:** from process start until a normal window larger than 100×100 belonging to the app is on screen. The window list is polled every 5 ms.
- **Memory:** the physical footprint reported by `/usr/bin/footprint` (what Activity Monitor calls *Memory*), summed over every process the app started, 15 s after launch.
- **Idle CPU:** CPU time used by the whole process tree over the next 10 s, divided by 10 s.

The Electron app starts its Python gateway as a detached background process, outside its own process tree. It was measured separately with the same tools ([`electron-gateway-macos.json`](benchmarks/results/electron-gateway-macos.json)) and is counted in the "app + backend" rows. The Rust engine runs inside the app process.

Caveats:

- One Rust run took 21.5 s to show its window. It is excluded by the median but still listed in the raw results; it looks like the window opened on another macOS Space.
- These figures predate the built-in Python for plugins. The bundled CPython runtime adds about 17 MB to the macOS download (≈56 MB zipped now). It only runs while a Python plugin is enabled, so memory and CPU at idle are unchanged.
- These are macOS numbers only. The Windows and Linux builds are tested in CI and have not been benchmarked.

To reproduce:

```bash
swift benchmarks/bench.swift rust 6 /path/to/Hermes.app/Contents/MacOS/hermes-desktop > benchmarks/results/rust-macos.jsonl
swift benchmarks/bench.swift electron 6 /path/to/Electron/Hermes.app/Contents/MacOS/Hermes > benchmarks/results/electron-macos.jsonl
python3 benchmarks/summarize.py
```

## What's in it

- **Chat:**
  - streamed Markdown, tool approvals;
  - `@file`, `@folder`, `@diff` and `@url` references;
  - attachments, a prompt queue, group chats, subagents;
  - chats in their own windows, with the chat list kept in sync across windows.
- **Accounts and models:**
  - Nous Portal, OpenAI Codex, xAI, MiniMax and Qwen device sign-ins;
  - API keys, custom endpoints;
  - model picker, reasoning presets.
- **Capabilities:**
  - skills and MCP servers (HTTP, stdio, OAuth 2.1);
  - the plugin catalog, with Agent Plugins v1 packages running natively;
  - Python plugins, run in the built-in Python interpreter.
- **Routines (cron):** templates, a sidebar section with countdowns, and one chat per routine that streams its run live.
- **Pages:** artifacts (with HTML preview in the OS webview), messaging, webhooks, profiles, agents, command center, star map.
- **Floating windows:**
  - HUD mode with native frosted glass, idle fade and click-through;
  - quick entry (⌘⇧Space);
  - a desktop pet with AI pet generation.
- **Voice (macOS):** dictation and voice conversation. The wake word uses the "hey hermes" model when the Python agent is installed.
- **Connections:** local and remote gateways (token or OAuth), SSH, Hermes Cloud discovery.

[`FEATURE_PARITY.md`](FEATURE_PARITY.md) lists every screen against the Electron app and says what is verified live, covered by tests, or only built.

## Python plugins

Hermes plugins written in Python (`plugin.yaml` + `__init__.py` with `register(ctx)`) run without installing Python or the Python Hermes agent:

- `hermes-python-host`, a small Rust program, embeds CPython through [PyO3](https://pyo3.rs). Each enabled Python plugin runs in its own host process, so a crashing plugin can't take the app down.
- Each package ships a pinned, checksummed CPython 3.13 runtime from [python-build-standalone](https://github.com/astral-sh/python-build-standalone), with tests, IDLE, Tk and headers removed:

  | Platform | Interpreter library | Where it ships |
  | --- | --- | --- |
  | Windows | `python313.dll` | `python\` next to `Hermes.exe` |
  | macOS | `libpython3.13.dylib` | `Hermes.app/Contents/Resources/python` |
  | Linux | `libpython3.13.so.1.0` | `lib/hermes/python` |

- The plugin `ctx` is implemented in Rust: `register_tool` (with `check_fn`, `requires_env` and async handlers), `register_hook` and `get_config`. Other `register_*` calls are skipped with a note on stderr.
- These hooks run around every tool call: `pre_tool_call` (it can block the call), `transform_tool_result` and `post_tool_call`.
- A plugin's `python_dependencies` are installed as wheels into the plugin's own data folder when you install it. Nothing runs at install time.
- A plugin that imports Hermes internals (`hermes_cli`, `tools`, ...) falls back to an installed Hermes Python agent, if there is one.

## Build from source

You need a recent stable Rust (`rustup update stable`) and a Python 3 with its shared library for PyO3 to link against (python.org's macOS installer, or a distribution's `python3-dev`). To use the same pinned runtime as the release builds, set `PYO3_PYTHON` first: `export PYO3_PYTHON=$(scripts/python-runtime.sh)` (on Windows, `$env:PYO3_PYTHON = ./scripts/python-runtime.ps1`). On Linux you also need the development packages listed in the [CI workflow](.github/workflows/build.yml).

```bash
cargo run --locked -p hermes-desktop
```

```bash
cargo test --locked --workspace
```

The packaging scripts are `scripts/package-macos.sh`, `scripts/package-linux.sh` and `scripts/package-windows.ps1`. Pushing a `v*` tag builds all three platforms and publishes them as a GitHub Release.

## Layout

| Path | What |
| --- | --- |
| `crates/engine` | Agent engine: providers (OpenAI-compatible, Anthropic, Responses), tools, MCP, subagents, accounts, plugins, references |
| `crates/library` | Local storage: sessions, projects, bots and groups, webhooks, profiles |
| `crates/gateway` | JSON-RPC gateway server and client, webhook receiver |
| `crates/desktop` | The Iced app |
| `crates/python-host` | Embedded CPython that runs Python plugins |
| `benchmarks/` | Benchmark harness and results |

## Not done yet

- **Not tried by hand:** Windows and Linux (CI builds and tests only), SSH against a real host, and connecting to a reachable cloud agent.
- **Missing:**
  - dictation on Windows and Linux;
  - desktop pages contributed by plugins;
  - notarization, installers and auto-update.

## License

MIT; see [LICENSE](LICENSE). The app is derived from Hermes Agent by Nous Research, also MIT. Its notice is in [LICENSE-HERMES-AGENT](LICENSE-HERMES-AGENT). The Hermes name, wordmark and icon belong to Nous Research.
