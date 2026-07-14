use crate::server::errors::ServerError;
use crate::server::scheme::ConfigureRequest;
use crate::server::state::{ChannelMessage, TaskAction};
use crate::server::state::{ServerState, TaskId};
use axum::Json;
use axum::extract::State;
use std::sync::Arc;

pub async fn configure(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<ConfigureRequest>,
) -> Result<(), ServerError> {
    let num_slots = request.num_slots;
    let mut old_num_slots = state.num_slots.lock().await;
    if *old_num_slots < num_slots {
        // 有新槽位则检查新任务
        *old_num_slots = num_slots;
        let tx = &state.tx;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::state::{ChannelMessage, TaskAction};
    use std::collections::BTreeMap;
    use std::error::Error;
    use tokio::sync::{Mutex, watch};

    #[tokio::test]
    async fn test_configure() -> Result<(), Box<dyn Error>> {
        let (tx, mut rx) = watch::channel(ChannelMessage {
            task_id: None,
            task_action: TaskAction::Complete,
        });
        let state = Arc::new(ServerState {
            num_slots: Mutex::new(1),
            used_slots: Mutex::new(1),
            task_id_counter: Mutex::new(4),
            tasks: Mutex::new(BTreeMap::new()),
            tx,
        });
        let request = ConfigureRequest { num_slots: 2 };
        configure(State(Arc::clone(&state)), Json(request)).await?;
        assert_eq!(*state.num_slots.lock().await, 2);
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
