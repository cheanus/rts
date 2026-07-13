use super::state::Task;
use std::collections::HashMap;

#[derive(serde::Deserialize)]
pub struct PushTaskRequest {
    pub label: Option<String>,
    pub command: String,
}

#[derive(serde::Deserialize)]
pub struct ConfigureRequest {
    pub num_slots: u32,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ListTaskResponse {
    pub num_slots: u32,
    pub used_slots: u32,
    pub tasks: HashMap<u32, Task>,
}
