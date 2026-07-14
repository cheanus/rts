use chrono::{DateTime, Local};
use std::collections::BTreeMap;
use std::path::PathBuf;
use tokio::sync::{Mutex, watch::Sender};

pub struct ServerState {
    pub num_slots: Mutex<u32>,
    pub used_slots: Mutex<u32>,
    pub task_id_counter: Mutex<u32>,
    pub tasks: Mutex<BTreeMap<u32, Task>>,
    pub tx: Sender<ChannelMessage>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Task {
    pub label: Option<String>,
    pub status: TaskStatus,
    pub command: String,
    pub path: Option<PathBuf>,
    pub create_time: DateTime<Local>,
    pub start_time: Option<DateTime<Local>>,
    pub end_time: Option<DateTime<Local>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TaskStatus {
    #[default]
    Pending,
    Running,
    Completed,
    Failed,
    // Skipped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskId {
    Old(u32),
    New,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskAction {
    Run,
    // Remove,
    Complete,
    Fail,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ChannelMessage {
    pub task_id: Option<TaskId>,
    pub task_action: TaskAction,
}
