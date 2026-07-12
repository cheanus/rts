use super::errors::ServerError;
use super::scheme::{ConfigureRequest, ListTaskResponse, PushTaskRequest};
use super::state::{ServerState, Task, TaskId, TaskStatus};
use crate::server::state::{ChannelMessage, TaskAction};
use axum::Json;
use axum::extract::State;
use std::sync::Arc;

pub async fn list_tasks(
    State(state): State<Arc<ServerState>>,
) -> Result<Json<ListTaskResponse>, ServerError> {
    let tasks = state.tasks.lock().await;
    let num_slots = state.num_slots.lock().await;
    let used_slots = state.used_slots.lock().await;
    let list_tasks_json = ListTaskResponse {
        num_slots: *num_slots,
        used_slots: *used_slots,
        tasks: tasks.clone(),
    };
    Ok(Json(list_tasks_json))
}

pub async fn push_task(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<PushTaskRequest>,
) -> Result<(), ServerError> {
    let mut task_id_counter = state.task_id_counter.lock().await;
    let task = Task {
        id: *task_id_counter,
        label: request.label,
        status: TaskStatus::Pending,
        command: request.command,
        path: None,
    };
    *task_id_counter += 1;
    state.tasks.lock().await.push(task);
    let tx = state.tx.lock().await;
    tx.send(ChannelMessage {
        task_id: Some(TaskId::New),
        task_action: TaskAction::Run,
    })
    .map_err(|e| ServerError::InternalError(e.to_string()))
}

pub async fn configure(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<ConfigureRequest>,
) -> Result<(), ServerError> {
    let num_slots = request.num_slots;
    let mut old_num_slots = state.num_slots.lock().await;
    if *old_num_slots < num_slots {
        // 有新槽位则检查新任务
        *old_num_slots = num_slots;
        let tx = state.tx.lock().await;
        tx.send(ChannelMessage {
            task_id: Some(TaskId::New),
            task_action: TaskAction::Run,
        })
        .map_err(|e| ServerError::InternalError(e.to_string()))?;
    } else {
        *old_num_slots = num_slots;
    }
    println!("Configured num_slots to {}", num_slots);
    Ok(())
}
