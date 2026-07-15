use crate::server::errors::ServerError;
use crate::server::scheme::PushTaskRequest;
use crate::server::state::{ServerState, Task, TaskStatus};
use axum::Json;
use axum::extract::State;
use chrono::Local;
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
        create_time: Local::now(),
        ..Default::default()
    };
    state.push_task(task).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::state::{ChannelMessage, TaskAction, TaskId};
    use std::error::Error;
    use std::path::PathBuf;
    use std::str::FromStr;
    use tokio::sync::watch;

    #[tokio::test]
    async fn test_push_task() -> Result<(), Box<dyn Error>> {
        let (tx, mut rx) = watch::channel(ChannelMessage {
            task_id: None,
            task_action: TaskAction::Complete,
        });
        let state = Arc::new(ServerState::new(1, tx));
        // 传送 task
        push_task(
            State(Arc::clone(&state)),
            Json(PushTaskRequest {
                label: Some("test".to_string()),
                command: "echo hi".to_string(),
                path: Some("/tmp/rtx/test_push".to_string()),
            }),
        )
        .await?;
        // 检查字段
        let task0 = state.tasks.lock().await.get(&0).unwrap().clone();
        assert_eq!(task0.label, Some("test".to_string()));
        assert_eq!(task0.status, TaskStatus::Pending);
        assert_eq!(task0.command, "echo hi".to_string());
        assert_eq!(task0.path, Some(PathBuf::from_str("/tmp/rtx/test_push")?));

        rx.changed().await?; // 等待 rx 收信
        let message = *rx.borrow();
        assert_eq!(
            message,
            ChannelMessage {
                task_id: Some(TaskId::New),
                task_action: TaskAction::Run,
            }
        );
        Ok(())
    }
}
