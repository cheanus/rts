#[derive(Clone, serde::Deserialize)]
pub struct PushTaskRequest {
    pub label: Option<String>,
    pub command: String,
}

#[derive(Clone, serde::Deserialize)]
pub struct ConfigureRequest {
    pub num_slot: Option<u32>,
}
