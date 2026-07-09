use tokio::sync::Mutex;


pub struct ServerState {
    pub num_slots: Mutex<u32>,
    pub used_slots: Mutex<u32>,
    pub task_id_counter: Mutex<u32>,
    pub tasks: Mutex<Vec<Task>>,
}

#[derive(Clone, serde::Serialize)]
pub struct Task {
    pub id: u32,
    pub label: Option<String>,
    pub command: String,
    pub status: TaskStatus,
}

#[derive(Clone, serde::Serialize)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
}
