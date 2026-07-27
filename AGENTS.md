# Repository Guidelines

## Project Overview

**rts** (Rust Task Spooler) is a task scheduling system for deep learning workloads, written in Rust. It is a modern replacement for [ts](https://github.com/justanhduc/task-spooler), API-compatible while fixing pain points around task deletion, slot management, GPU assignment, and build complexity. The project is a single-crate binary (`cargo build` produces one executable), targeting Linux with `nix` for signal-based process control.

- **License**: MIT
- **Version**: 0.1.0
- **Edition**: Rust 2024 (requires Rust ≥ 1.85)

## Architecture & Data Flow

```
┌──────────────┐   HTTP (JSON)   ┌────────────────────────────────┐
│  CLI client  │ ───────────────→ │  axum server (port 20110)     │
│  (reqwest)   │                 │  Router + 6 handler functions   │
└──────────────┘                 │  State: Arc<ServerState>       │
                                 │  ┌──────────────────────────┐  │
                                 │  │ ServerState (mutexed)    │  │
                                 │  │  num_slots / used_slots  │  │
                                 │  │  tasks: BTreeMap<u32,Task>│  │
                                 │  │  task_id_counter         │  │
                                 │  │  tx (watch::Sender)      │  │
                                 │  └──────────┬───────────────┘  │
                                 │             │ watch channel     │
                                 │  ┌──────────▼───────────────┐  │
                                 │  │ rx_worker (background)   │  │
                                 │  │  - spawns subprocesses    │  │
                                 │  │  - manages slots          │  │
                                 │  │  - resolves dependencies  │  │
                                 │  │  - captures exit codes    │  │
                                 │  └──────────────────────────┘  │
                                 └────────────────────────────────┘
```

**Data flow**:
1. CLI parses args via `clap` → constructs HTTP request → sends to `127.0.0.1:{RTS_SERVER_PORT}` (env var, default `20110`)
2. Server handler (e.g. `push_task`) receives JSON → creates a `Task` → calls `ServerState::push_task()` → sends `ChannelMessage` on `watch::Sender`
3. `rx_worker` receives `ChannelMessage` via `watch::Receiver` → `try_create_tasks` scans for runnable tasks → `create_task` spawns a `tokio::process::Command` subprocess → monitors exit code → updates task status + sends follow-up `ChannelMessage`
4. CLI polls or queries state via GET endpoints (`/tasks/list`, `/tasks/info`, etc.)

**Key design decisions**:
- Single binary (no separate server/client binaries) — `rts server` starts server, bare `rts` auto-detects and connects
- In-memory task store (`BTreeMap<u32, Task>`) — no database
- `tokio::sync::watch` channel as the internal event bus between handlers and worker
- Server and CLI share the same crate; `lib.rs` re-exports `cli`, `errors`, `server`

## Key Directories

| Path | Purpose |
|------|---------|
| `src/main.rs` | Binary entry point — parses CLI args, dispatches to server or client |
| `src/lib.rs` | Crate root — declares public modules `cli`, `errors`, `server` |
| `src/server.rs` | Server bootstrap — watch channel, axum router, spawns `rx_worker`, `tokio::try_join!` |
| `src/server/state.rs` | `ServerState`, `Task`, `TaskStatus`, `TaskAction`, `ChannelMessage` (TaskId removed, `ChannelMessage.task_id` is now `Option<u32>`) |
| `src/server/workers.rs` | Core worker loop (`rx_worker`) — spawns subprocesses, manages slots, resolves dependencies |
| `src/server/scheme.rs` | JSON DTOs — `PushTaskRequest`, `ConfigureRequest`, `ListTaskResponse`, `TaskIdRequest`, `RemoveTaskRequest` |
| `src/server/handlers.rs` | All six axum handler functions merged into a single file (previously `src/server/handle/` directory) |
| `src/cli.rs` | CLI client — `reqwest` calls to server, also exports `get_server_host()`, `is_server_alive()` |
| `src/cli/args.rs` | `clap` derive structs — `Args`, `Commands`, `DoTaskMode`, `DependTaskMode` |
| `src/errors.rs` | Re-exports `CliError`, `ServerError`; defines shared `ResponseError {code, message}` |
| `src/errors/cli.rs` | `CliError` enum — `Http`, `Request`, `Io`, `Json`, `Env` variants — concrete error type with `Display` + `Error` + `From` impls |
| `src/errors/server.rs` | `ServerError` enum — `InvalidJson`, `InternalError`, `InvalidParams` — implements `IntoResponse` |

## Development Commands

| Action | Command |
|--------|---------|
| Build | `cargo build` |
| Build release | `cargo build --release` |
| Run all tests | `cargo test` |
| Run specific test | `cargo test test_push_task` |
| Lint (clippy) | `cargo clippy` |
| Format | `cargo fmt` |
| Start server | `cargo run -- server` |
| Push a task | `cargo run -- run -- echo "hello"` |
| List tasks | `cargo run -- list` |
| Get task info | `cargo run -- do -i <id>` |
| Tail task log | `cargo run -- do -t <id>` |
| Remove a task | `cargo run -- do -r <id>` |
| Kill a task | `cargo run -- do -k <id>` |
| Set slot count | `cargo run -- config -S <n>` |

No Makefile, justfile, shell scripts, CI/CD, or Docker configurations exist.

## Code Conventions & Common Patterns

### Naming
- **snake_case** for functions, variables, modules, fields (`push_task`, `num_slots`, `task_id_counter`)
- **PascalCase** for types and enums (`ServerState`, `TaskStatus`, `PushTaskRequest`)
- **PascalCase** for enum variants (`TaskStatus::Pending`, `ServerError::InvalidParams`)
- Constants: `RTS_SERVER_PORT` (uppercase snake_case via `env::var`)

### Error Handling
- **Server side**: Functions return `Result<(), ServerError>`. `ServerError` implements `IntoResponse` → maps to HTTP status codes + JSON `ResponseError {code, message}`.
  - `InvalidJson` → 400 BAD_REQUEST
  - `InvalidParams` → 400 BAD_REQUEST
  - `InternalError` → 500 INTERNAL_SERVER_ERROR
- **CLI side**: Functions return `Result<(), CliError>`. `CliError` is an enum with `Http`, `Request`, `Io`, `Json`, `Env` variants — no `Box<dyn Error>`. `RtsClient` struct encapsulates `reqwest::Client` and server host for all HTTP operations.
- Handler pattern: extract `State<Arc<ServerState>>` + `Json<T>` → call state method → return `Result<(), ServerError>`
- No `anyhow`, `thiserror`, or `eyre` — manual `Display` + `Error` impls

### Async Patterns
- **Runtime**: `tokio` with `rt-multi-thread`, `macros` (`#[tokio::main]`, `#[tokio::test]`), `process`
- **Shared state**: `Arc<ServerState>` where each field is a `tokio::sync::Mutex<T>` (separate per-field locking)
- **Internal event bus**: `tokio::sync::watch::channel(ChannelMessage)` — handlers send, `rx_worker` receives
- **Concurrent spawn**: `tokio::try_join!(axum::serve(...), rx_worker_fut)` in `server.rs`
- **Process spawning**: `tokio::process::Command` with `stdin(Stdio::null())`, `stdout(Stdio::piped())`, `stderr(Stdio::piped())`
- **Signal handling**: `nix::sys::signal::kill(pid, SIGTERM)` for task termination

### State Management
- `ServerState` is the central data structure, shared via `Arc<ServerState>` in axum's `with_state()`
- Individual fields use fine-grained `tokio::sync::Mutex`:
  - `tasks: Mutex<BTreeMap<u32, Task>>` — ordered by task ID (creation order)
  - `num_slots: Mutex<u32>`, `used_slots: Mutex<u32>` — concurrency control
  - `task_id_counter: Mutex<u32>` — monotonic ID generator
  - `tx: Sender<ChannelMessage>` — immutable after construction (no Mutex)
- `push_task()` acquires `tasks` and `task_id_counter` locks together, validates dependency IDs, wires bidirectional `dependencies`/`required` links between tasks

### Task Dependency Model
- **`dependencies: HashMap<u32, TaskStatus>`** — tasks this task waits for. Populated at push time from the request's `dependencies` vec.
- **`required: Vec<u32>`** — inverse: tasks that depend on this task. Updated in-place on dependents when pushing.
- **`not_safely_depends: bool`** — if true, the task runs even if a dependency fails (`not safely` = don't require success)
- The worker resolves: a task is runnable when all `dependencies` entries have terminal statuses (`Completed`, `Failed`, `Killed`, `Skipped`)

### Handler Convention
After the module flatten refactor (Step 7), all 6 handler functions live in `src/server/handlers.rs` (was `src/server/handle/` directory with 6 files + barrel). Every handler follows:
```rust
pub async fn handler_name(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<RequestType>,
) -> Result<(), ServerError> {
    // validate, act on state, return Ok(())
}
```
Tests use `#[tokio::test]` with `Result<(), Box<dyn Error>>` return. Setup: create a `watch::channel`, construct `Arc<ServerState>`, call handler directly, assert on state and channel messages. Each handler's tests live in a `mod tests { mod handler_tests { ... } }` block at the end of `handlers.rs`.

### Log Handling
- Task stdout/stderr written to `log_path` (default: `/tmp/rtx/{id}`)
- `cat` reads forward via `BufReader`; `tail` reads backward via `rev_buf_reader`
- Empty log path → task output is discarded (`Stdio::null()`)

### Comments & Language
- Source comments and doc strings are in **Chinese**
- Test code comments are in Chinese
- README is in Chinese

## Important Files

| File | Role |
|------|------|
| `Cargo.toml` | Single-crate package; no workspace, no features, no dev-dependencies |
| `src/main.rs` | Binary entry — `#[tokio::main]` + `clap::Parser::parse()` → dispatch to `cli::RtsClient::new()` methods |
| `src/lib.rs` | Crate root — `pub mod cli; pub mod errors; pub mod server;` |
| `src/server.rs` | `pub async fn server(server_host)`, router, `tokio::try_join!` |
| `src/server/state.rs` | Core types: `ServerState`, `Task`, `TaskStatus`, `ChannelMessage`, `TaskAction` (TaskId removed) |
| `src/server/workers.rs` | `rx_worker`, `create_task`, `try_create_tasks`, `update_required_status` — the scheduling engine |
| `src/server/scheme.rs` | All request/response DTOs used by HTTP endpoints |
| `src/server/handlers.rs` | All six axum handlers merged into single file (previously `src/server/handle/` directory + barrel) |
| `src/cli.rs` | `RtsClient` struct + `handle_do_command` — shared HTTP layer; all CLI operations as methods |
| `src/cli/args.rs` | `clap` derive structs defining the CLI surface |
| `src/errors/server.rs` | `ServerError` enum (`InvalidJson`, `InternalError`, `InvalidParams`) + `IntoResponse` impl |
| `src/errors/cli.rs` | `CliError` enum (`Http`, `Request`, `Io`, `Json`, `Env`) — concrete error type, no `Box<dyn Error>` |
| `.gitignore` | Only ignores `/target` |

The server reads `RTS_SERVER_PORT` env var to bind; defaults to `20110`. The CLI reads the same var to connect. Task logs default to `/tmp/rtx/{task_id}`.

## Runtime & Tooling Preferences

- **Runtime**: `tokio` — multi-thread, `#[tokio::main]`, `tokio::process` for subprocess spawning
- **Package manager**: Cargo (no workspace, single crate)
- **Build**: `cargo build` / `cargo build --release`
- **Formatter**: `cargo fmt` (default `rustfmt` settings)
- **Linter**: `cargo clippy` (default settings; no `clippy.toml`)
- **No CI/CD**, no Docker, no Makefile/justfile
- **Rust edition 2024** — the `Cargo.toml` must stay at `edition = "2024"`
- **No nightly features** — all dependencies are stable-compatible

## Testing & QA

### Framework
- Rust built-in `#[cfg(test)]` + `#[tokio::test]` for async tests
- No external test frameworks (`rstest`, `proptest`, `criterion`)
- No `tests/` integration test directory — all tests are inline unit tests

### Test Locations (11 tests across 7 files)

| File | Tests | What's Covered |
|------|-------|----------------|
| `src/server/workers.rs` | `test_rx_work`, `test_exit_code`, `test_dependence` | Worker task execution, exit codes, dependency resolution |
| `src/server/handlers.rs` (merged) | `test_push_task`, `test_push_task_invalid_dependency` (push_tests), `test_remove_running_task`, `test_remove_unrun_task`, `test_remove_all_task` (remove_tests), `test_list_tasks` (info_tests), `test_list_tasks` (kill_tests), `test_list_tasks` (list_tests), `test_configure` (configure_tests), `test_display_no_panic` (errors/server.rs) | All handler tests, dependency validation regression, Display non-recursion regression |

### Test Conventions
- Tests create isolated `ServerState` via `watch::channel()` — no global state
- Async tests return `Result<(), Box<dyn Error>>`
- Tests assert on both state (`state.tasks.lock().await`) and channel messages (`rx.changed().await`)
- Worker tests use `tokio::time::sleep(Duration::from_millis(...))` for timing (potential flakiness)
- Test helper `init_worker()` exists inside `workers.rs::tests` only (not shared)

### Run Tests
```bash
cargo test                    # all tests
cargo test test_push_task     # single test
```

### Gaps
- **No CLI tests** — `src/cli.rs` and `src/cli/args.rs` have no test coverage
- **No core state tests** — `src/server/state.rs` has no inline tests (tested only indirectly via handlers)
- **No error type tests** — `src/errors/` has no tests
- **No integration tests** — no `tests/` directory, no axum server spun up in tests
- **No CI** — no automated test running on push/PR
- **Sleep-based timing** in worker tests may be flaky on slow machines