mod errors;
mod handle;
pub mod scheme;
mod state;
mod workers;

use axum::{Router, routing::get, routing::post};
use state::{ChannelMessage, ServerState, TaskAction};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, watch};

pub async fn server(server_host: String) {
    // 用 watch channel 传递进程状态
    let (tx, rx) = watch::channel(ChannelMessage {
        task_id: None,
        task_action: TaskAction::Complete,
    });

    let state = Arc::new(ServerState {
        num_slots: Mutex::new(1),
        used_slots: Mutex::new(0),
        task_id_counter: Mutex::new(0),
        tasks: Mutex::new(BTreeMap::new()),
        tx: tx,
    });

    // 创建 rx 处理线程
    let rx_worker_fut = workers::rx_worker(rx, Arc::clone(&state));

    let app = Router::new()
        .route("/health", get(|| async { "Hello, World!" }))
        .route("/tasks/list", get(handle::list_tasks))
        .route("/tasks/push", post(handle::push_task))
        .route("/configure", post(handle::configure))
        .with_state(state);

    let listener = TcpListener::bind(&server_host).await.unwrap_or_else(|e| {
        eprintln!("Cannot be bound to {}: {}", &server_host, e);
        std::process::exit(1);
    });
    println!("Server is running on http://{}", &server_host);
    tokio::try_join!(axum::serve(listener, app), rx_worker_fut).unwrap_or_else(|e| {
        eprintln!("The server cannot run: {}", e);
        std::process::exit(1);
    });
}
