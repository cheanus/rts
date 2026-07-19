use super::state::Task;
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct PushTaskRequest {
    pub label: Option<String>,
    pub command: String,
    pub log_path: Option<PathBuf>,
    pub current_dir: PathBuf,
    pub envs: HashMap<String, String>,
    pub not_safely_depends: bool,
    pub dependencies: Vec<u32>,
}

#[derive(serde::Deserialize)]
pub struct ConfigureRequest {
    pub num_slots: u32,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ListTaskResponse {
    pub num_slots: u32,
    pub used_slots: u32,
    pub tasks: BTreeMap<u32, Task>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct TaskIdRequest {
    pub task_id: u32,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct RemoveTaskRequest {
    pub task_id: u32,
    pub is_all: bool,
}
