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
    use axum::extract;
    use std::error::Error;
    use std::{collections::HashMap, path::PathBuf};
    use tokio::sync::{Mutex, watch};

    #[tokio::test]
    async fn test_list_tasks() -> Result<(), Box<dyn Error>> {
        let (tx, _) = watch::channel(ChannelMessage {
            task_id: None,
            task_action: TaskAction::Complete,
        });
        let mut tasks = HashMap::new();
        tasks.insert(
            1,
            Task {
                label: None,
                status: TaskStatus::Running,
                command: "echo hi".into(),
                path: Some(PathBuf::from("/tmp/a")),
            },
        );
        tasks.insert(
            3,
            Task {
                label: Some("higher".to_string()),
                status: TaskStatus::Pending,
                command: "sleep 10".into(),
                path: None,
            },
        );
        let state = Arc::new(ServerState {
            num_slots: Mutex::new(1),
            used_slots: Mutex::new(1),
            task_id_counter: Mutex::new(4),
            tasks: Mutex::new(tasks.clone()),
            tx: Mutex::new(tx),
        });
        let extract::Json(result) = list_tasks(State(state)).await?;
        assert_eq!(result.num_slots, 1);
        assert_eq!(result.used_slots, 1);
        assert_eq!(result.tasks, tasks);
        Ok(())
    }
}
