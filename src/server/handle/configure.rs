use crate::errors::ServerError;
use crate::server::scheme::ConfigureRequest;
use crate::server::state::ServerState;
use axum::Json;
use axum::extract::State;
use std::sync::Arc;

pub async fn configure(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<ConfigureRequest>,
) -> Result<(), ServerError> {
    let num_slots = request.num_slots;
    state.set_num_slots(num_slots).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::state::{ChannelMessage, TaskAction, TaskId};
    use std::error::Error;
    use tokio::sync::watch;

    #[tokio::test]
    async fn test_configure() -> Result<(), Box<dyn Error>> {
        let (tx, mut rx) = watch::channel(ChannelMessage {
            task_id: None,
            task_action: TaskAction::Complete,
        });
        let state = Arc::new(ServerState::new(1, tx));
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
