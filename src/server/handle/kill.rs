use crate::errors::ServerError;
use crate::server::scheme::TaskIdRequest;
use crate::server::state::ServerState;
use axum::extract::{Query, State};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use std::sync::Arc;

pub async fn kill_task(
    State(state): State<Arc<ServerState>>,
    Query(request): Query<TaskIdRequest>,
) -> Result<(), ServerError> {
    let tasks = state.tasks.lock().await;
    let Some(task) = tasks.get(&request.task_id) else {
        return Err(ServerError::InvalidParams(format!(
            "No task with ID {}",
            request.task_id
        )));
    };
    let Some(pid) = task.pid else {
        return Err(ServerError::InternalError(format!(
            "The task may not has run"
        )));
    };
    let Ok(pid) = i32::try_from(pid) else {
        return Err(ServerError::InternalError(format!(
            "The task PID is too big!"
        )));
    };
    let pid = Pid::from_raw(pid);
    kill(pid, Signal::SIGTERM).map_err(|e| ServerError::InternalError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::state::{ChannelMessage, Task, TaskAction, TaskStatus};
    use std::error::Error;
    use tokio::process;
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
        let mut child = process::Command::new("sh")
            .arg("-c")
            .arg("sleep 10")
            .spawn()?;
        let task = Task {
            status: TaskStatus::Running,
            command: "sleep 10".into(),
            pid: child.id(),
            ..Default::default()
        };
        {
            let mut tasks = state.tasks.lock().await;
            tasks.insert(0, task.clone());
        }
        // 调用结果
        kill_task(State(state), Query(TaskIdRequest { task_id: 0 })).await?;

        let status = child.wait().await?;

        assert_eq!(status.success(), false);
        assert!(status.code().is_none());
        Ok(())
    }
}
