use crate::errors::ServerError;
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
    let log_path = match request.log_path {
        Some(p) => Some(PathBuf::from(p)),
        None => None,
    };
    let task = Task {
        label: request.label,
        status: TaskStatus::Pending,
        command: request.command,
        log_path,
        current_dir: PathBuf::from(request.current_dir),
        envs: request.envs,
        create_time: Local::now(),
        not_safely_depends: request.not_safely_depends,
        ..Default::default()
    };
    state.push_task(task, &request.dependencies).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::state::{ChannelMessage, TaskAction, TaskId};
    use std::collections::HashMap;
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
        let request = PushTaskRequest {
            label: Some("test".to_string()),
            command: "echo hi".to_string(),
            log_path: Some(PathBuf::from_str("/tmp/rtx/test_push")?),
            current_dir: PathBuf::from_str("/")?,
            envs: HashMap::from([("PYTHONPATH".to_string(), "/".to_string())]),
            not_safely_depends: false,
            dependencies: Vec::new(),
        };
        push_task(State(Arc::clone(&state)), Json(request.clone())).await?;
        // 检查字段
        {
            let tasks = state.tasks.lock().await;
            let task0 = tasks.get(&0).unwrap();
            assert_eq!(task0.label, request.label);
            assert_eq!(task0.status, TaskStatus::Pending);
            assert_eq!(task0.command, request.command);
            assert_eq!(
                task0.log_path,
                Some(PathBuf::from_str("/tmp/rtx/test_push")?)
            );
            assert_eq!(task0.current_dir, request.current_dir);
            assert_eq!(task0.envs, request.envs);
        }

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
