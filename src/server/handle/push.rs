use crate::server::errors::ServerError;
use crate::server::scheme::PushTaskRequest;
use crate::server::state::{ChannelMessage, TaskAction};
use crate::server::state::{ServerState, Task, TaskId, TaskStatus};
use axum::Json;
use axum::extract::State;
use std::path::PathBuf;
use std::sync::Arc;

pub async fn push_task(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<PushTaskRequest>,
) -> Result<(), ServerError> {
    let path = match request.path {
        Some(p) => Some(PathBuf::from(p)),
        None => None,
    };
    let task = Task {
        label: request.label,
        status: TaskStatus::Pending,
        command: request.command,
        path,
    };
    let mut task_id_counter = state.task_id_counter.lock().await;
    // 由于 state.tasks 是 BTreeMap，所以各 task 是按创建时间排序的
    state.tasks.lock().await.insert(*task_id_counter, task);

    *task_id_counter += 1;

    let tx = state.tx.lock().await;
    tx.send(ChannelMessage {
        task_id: Some(TaskId::New),
        task_action: TaskAction::Run,
    })
    .map_err(|e| ServerError::InternalError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::state::{ChannelMessage, TaskAction};
    use std::error::Error;
    use std::str::FromStr;
    use std::{collections::BTreeMap, path::PathBuf};
    use tokio::sync::{Mutex, watch};

    #[tokio::test]
    async fn test_push_task() -> Result<(), Box<dyn Error>> {
        let (tx, mut rx) = watch::channel(ChannelMessage {
            task_id: None,
            task_action: TaskAction::Complete,
        });
        let state = Arc::new(ServerState {
            num_slots: Mutex::new(1),
            used_slots: Mutex::new(1),
            task_id_counter: Mutex::new(4),
            tasks: Mutex::new(BTreeMap::new()),
            tx: Mutex::new(tx),
        });

        push_task(
            State(Arc::clone(&state)),
            Json(PushTaskRequest {
                label: Some("test".to_string()),
                command: "echo hi".to_string(),
                path: Some("/tmp/rtx/test_push".to_string()),
            }),
        )
        .await?;

        assert_eq!(
            state.tasks.lock().await.get(&4),
            Some(&Task {
                label: Some("test".to_string()),
                status: TaskStatus::Pending,
                command: "echo hi".to_string(),
                path: Some(PathBuf::from_str("/tmp/rtx/test_push")?),
            })
        );

        rx.changed().await?; // 等待 rx 收信
        let message = rx.borrow();
        assert_eq!(
            *message,
            ChannelMessage {
                task_id: Some(TaskId::New),
                task_action: TaskAction::Run,
            }
        );
        Ok(())
    }
}
