use super::state::TaskStatus;

#[derive(Clone, serde::Deserialize)]
pub struct PushTaskRequest {
    pub label: Option<String>,
    pub command: String,
}

#[derive(Clone, serde::Deserialize)]
pub struct ConfigureRequest {
    pub num_slot: Option<u32>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ListTaskResponse {
    pub id: u32,
    pub label: Option<String>,
    pub status: TaskStatus,
    pub command: String,
}
