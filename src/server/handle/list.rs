use crate::server::errors::ServerError;
use crate::server::scheme::ListTaskResponse;
use crate::server::state::ServerState;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::state::{ChannelMessage, Task, TaskAction, TaskStatus};
    use std::error::Error;
    use std::{collections::BTreeMap, path::PathBuf};
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
        let mut tasks = BTreeMap::new();
        tasks.insert(
            0,
            Task {
                status: TaskStatus::Running,
                command: "echo hi".into(),
                path: Some(PathBuf::from("/tmp/a")),
                ..Default::default()
            },
        );
        tasks.insert(
            1,
            Task {
                label: Some("higher".to_string()),
                status: TaskStatus::Pending,
                command: "sleep 10".into(),
                ..Default::default()
            },
        );
        *state.tasks.lock().await = tasks.clone();
        // 调用结果
        let Json(result) = list_tasks(State(Arc::clone(&state))).await?;

        assert_eq!(result.num_slots, 1);
        assert_eq!(result.used_slots, 0);
        assert_eq!(result.tasks, tasks);
        Ok(())
    }
}
