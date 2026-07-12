use std::path::PathBuf;
use tokio::sync::{Mutex, watch::Sender};

pub struct ServerState {
    pub num_slots: Mutex<u32>,
    pub used_slots: Mutex<u32>,
    pub task_id_counter: Mutex<u32>,
    pub tasks: Mutex<Vec<Task>>,
    pub tx: Mutex<Sender<ChannelMessage>>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Task {
    pub id: u32,
    pub label: Option<String>,
    pub status: TaskStatus,
    pub command: String,
    pub path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TaskId {
    Old(u32),
    New,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TaskAction {
    Run,
    // Remove,
    Complete,
    Fail,
}

pub struct ChannelMessage {
    pub task_id: Option<TaskId>,
    pub task_action: TaskAction,
}
