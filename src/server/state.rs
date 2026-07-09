use tokio::sync::Mutex;

pub struct ServerState {
    pub num_slots: Mutex<u32>,
    pub used_slots: Mutex<u32>,
    pub task_id_counter: Mutex<u32>,
    pub tasks: Mutex<Vec<Task>>,
}

#[derive(Clone, serde::Deserialize)]
pub struct Task {
    pub id: u32,
    pub label: Option<String>,
    pub status: TaskStatus,
    pub command: String,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Clone, Copy)]
pub struct ChannelMessage {
    pub task_id: u32,
    pub task_status: TaskStatus,
}
