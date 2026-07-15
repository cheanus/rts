use super::state::Task;
use std::collections::BTreeMap;

#[derive(serde::Deserialize)]
pub struct PushTaskRequest {
    pub label: Option<String>,
    pub command: String,
    pub path: Option<String>,
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

#[derive(serde::Deserialize)]
pub struct TaskInfoRequest {
    pub task_id: u32,
}
