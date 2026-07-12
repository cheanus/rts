use super::state::Task;

#[derive(Clone, serde::Deserialize)]
pub struct PushTaskRequest {
    pub label: Option<String>,
    pub command: String,
}

#[derive(Clone, serde::Deserialize)]
pub struct ConfigureRequest {
    pub num_slots: u32,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ListTaskResponse {
    pub num_slots: u32,
    pub used_slots: u32,
    pub tasks: Vec<Task>,
}
