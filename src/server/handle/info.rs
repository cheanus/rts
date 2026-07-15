use crate::server::errors::ServerError;
use crate::server::scheme::TaskInfoRequest;
use crate::server::state::{ServerState, Task};
use axum::Json;
use axum::extract::{Query, State};
use std::sync::Arc;

pub async fn get_task_info(
    State(state): State<Arc<ServerState>>,
    Query(request): Query<TaskInfoRequest>,
) -> Result<Json<Task>, ServerError> {
    let tasks = state.tasks.lock().await;
    match tasks.get(&request.task_id) {
        Some(task) => Ok(Json(task.clone())),
        None => Err(ServerError::InvalidParams(format!(
            "No task with ID {}",
            request.task_id
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::state::{ChannelMessage, TaskAction, TaskStatus};
    use std::error::Error;
    use std::path::PathBuf;
    use tokio::sync::watch;

    #[tokio::test]
    async fn test_list_tasks() -> Result<(), Box<dyn Error>> {
        // 初始化
        let (tx, _rx) = watch::channel(ChannelMessage {
            task_id: None,
            task_action: TaskAction::Complete,
        });
        let state = Arc::new(ServerState::new(1, tx));
        // 创建测试样本
        let task = Task {
            status: TaskStatus::Running,
            command: "echo hi".into(),
            log_path: Some(PathBuf::from("/tmp/a")),
            ..Default::default()
        };
        {
            let state_clone = Arc::clone(&state);
            let mut tasks = state_clone.tasks.lock().await;
            tasks.insert(0, task.clone());
        }
        // 调用结果
        let Json(result) =
            get_task_info(State(state), Query(TaskInfoRequest { task_id: 0 })).await?;

        assert_eq!(result, task);
        Ok(())
    }
}
