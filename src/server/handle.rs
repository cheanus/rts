use super::errors::ServerError;
use super::scheme::{ConfigureRequest, ListTaskResponse, PushTaskRequest};
use super::state::{ServerState, Task, TaskStatus};
use axum::Json;
use axum::extract::State;
use serde_json::Value;
use std::sync::Arc;

pub async fn list_tasks(State(state): State<Arc<ServerState>>) -> Result<Json<Value>, ServerError> {
    let tasks = state.tasks.lock().await;
    let tasks_json: Vec<Value> = tasks
        .iter()
        .map(|task| {
            serde_json::json!(ListTaskResponse {
                id: task.id,
                label: task.label.clone(),
                status: task.status.clone(),
                command: task.command.clone(),
            })
        })
        .collect();
    Ok(Json(serde_json::json!(tasks_json)))
}

pub async fn push_task(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<PushTaskRequest>,
) -> Result<(), ServerError> {
    let mut task_id_counter = state.task_id_counter.lock().await;
    let task = Task {
        id: *task_id_counter,
        label: request.label.clone(),
        command: request.command,
        status: TaskStatus::Pending,
    };
    *task_id_counter += 1;
    state.tasks.lock().await.push(task);
    Ok(())
}

pub async fn configure(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<ConfigureRequest>,
) -> Result<(), ServerError> {
    if let Some(num_slot) = request.num_slot {
        let mut num_slot_lock = state.num_slots.lock().await;
        *num_slot_lock = num_slot;
    }
    Ok(())
}
