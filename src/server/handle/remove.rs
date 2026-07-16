use crate::errors::ServerError;
use crate::server::scheme::RemoveTaskRequest;
use crate::server::state::{ServerState, TaskStatus};
use axum::extract::{Query, State};
use std::sync::Arc;

pub async fn remove_task(
    State(state): State<Arc<ServerState>>,
    Query(request): Query<RemoveTaskRequest>,
) -> Result<(), ServerError> {
    let mut tasks = state.tasks.lock().await;
    if request.is_all {
        let id_to_remove: Vec<u32> = tasks
            .iter()
            .filter(|(_, task)| !matches!(task.status, TaskStatus::Pending | TaskStatus::Running))
            .map(|(id, _)| *id)
            .collect();
        for id in id_to_remove {
            if !tasks.remove(&id).is_some() {
                eprintln!("No task {} need to be remove", id);
            };
        }
        Ok(())
    } else {
        let task_id = request.task_id;
        match tasks.get(&task_id) {
            Some(task) if task.status == TaskStatus::Running => Err(ServerError::InvalidParams(
                format!("Task {task_id} is running!"),
            )),
            Some(_) => {
                tasks.remove(&task_id);
                Ok(())
            }
            None => Err(ServerError::InvalidParams(format!(
                "No task with ID {}",
                request.task_id
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::state::{ChannelMessage, Task, TaskAction, TaskStatus};
    use std::error::Error;
    use tokio::sync::watch;

    #[tokio::test]
    async fn test_remove_running_task() -> Result<(), Box<dyn Error>> {
        // 初始化
        let (tx, _rx) = watch::channel(ChannelMessage {
            task_id: None,
            task_action: TaskAction::Complete,
        });
        let state = Arc::new(ServerState::new(1, tx));
        // 创建测试样本
        {
            let task = Task {
                status: TaskStatus::Running,
                command: "echo hi".into(),
                ..Default::default()
            };
            let mut tasks = state.tasks.lock().await;
            tasks.insert(0, task);
        }
        // 尝试删除
        let result = remove_task(
            State(state),
            Query(RemoveTaskRequest {
                task_id: 0,
                is_all: false,
            }),
        )
        .await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unrun_task() -> Result<(), Box<dyn Error>> {
        // 初始化
        let (tx, _rx) = watch::channel(ChannelMessage {
            task_id: None,
            task_action: TaskAction::Complete,
        });
        let state = Arc::new(ServerState::new(1, tx));
        // 创建测试样本
        {
            let task = Task {
                status: TaskStatus::Completed,
                command: "echo hi".into(),
                ..Default::default()
            };
            let mut tasks = state.tasks.lock().await;
            tasks.insert(0, task);
        }
        // 尝试删除
        let result = remove_task(
            State(state),
            Query(RemoveTaskRequest {
                task_id: 0,
                is_all: false,
            }),
        )
        .await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_all_task() -> Result<(), Box<dyn Error>> {
        // 初始化
        let (tx, _rx) = watch::channel(ChannelMessage {
            task_id: None,
            task_action: TaskAction::Complete,
        });
        let state = Arc::new(ServerState::new(1, tx));
        // 创建测试样本
        {
            let mut tasks = state.tasks.lock().await;
            tasks.insert(
                0,
                Task {
                    status: TaskStatus::Pending,
                    command: "echo hi".into(),
                    ..Default::default()
                },
            );
            tasks.insert(
                1,
                Task {
                    status: TaskStatus::Completed,
                    command: "echo hi".into(),
                    ..Default::default()
                },
            );
        }
        // 尝试删除
        let state_clone = Arc::clone(&state);
        let result = remove_task(
            State(state_clone),
            Query(RemoveTaskRequest {
                task_id: 0,
                is_all: true,
            }),
        )
        .await;
        assert!(result.is_ok());

        let tasks = state.tasks.lock().await;
        assert!(tasks.get(&0).is_some());
        assert!(tasks.get(&1).is_none());
        Ok(())
    }
}
