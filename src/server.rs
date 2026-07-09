mod errors;
mod handle;
mod scheme;
mod state;

use axum::{Router, routing::get, routing::post};
use state::{ServerState, TaskStatus};
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

pub async fn server() {
    let state = Arc::new(ServerState {
        num_slots: Mutex::new(1),
        used_slots: Mutex::new(0),
        task_id_counter: Mutex::new(0),
        tasks: Mutex::new(Vec::new()),
    });

    // 创建 worker 线程
    let worker_state = state.clone();
    tokio::spawn(async move {
        // 循环更新服务器状态
        loop {
            let tasks = worker_state.tasks.lock().await.clone();
            for task in tasks.iter() {
                let num_slots = worker_state.num_slots.lock().await.clone();
                let used_slots = worker_state.used_slots.lock().await.clone();
                // 如果槽位空余，寻找 Pending 任务并执行
                if num_slots > used_slots && matches!(task.status, TaskStatus::Pending) {
                    let task_id = task.id;
                    let task_state = worker_state.clone();
                    // 执行命令前，更新状态
                    {
                        let mut inner_used_slots = task_state.used_slots.lock().await;
                        let mut inner_tasks = task_state.tasks.lock().await;
                        if let Some(inner_task) = inner_tasks.iter_mut().find(|t| t.id == task_id) {
                            inner_task.status = TaskStatus::Running;
                            *inner_used_slots += 1;
                        }
                    }
                    // 为任务分配子线程以执行
                    tokio::spawn(async move {
                        let mut inner_used_slots = task_state.used_slots.lock().await;
                        let mut inner_tasks = task_state.tasks.lock().await;
                        if let Some(inner_task) = inner_tasks.iter_mut().find(|t| t.id == task_id) {
                            // 执行命令
                            let mut sh = process::Command::new("sh");
                            sh.arg("-c").arg(&inner_task.command);
                            match sh.status() {
                                Ok(_) => inner_task.status = TaskStatus::Completed,
                                Err(_) => inner_task.status = TaskStatus::Failed,
                            }
                            // 命令结束，更新状态
                            *inner_used_slots -= 1;
                        }
                    });
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    let app = Router::new()
        .route("/health", get(|| async { "Hello, World!" }))
        .route("/tasks/list", get(handle::list_tasks))
        .route("/tasks/push", post(handle::push_task))
        .route("/configure", post(handle::configure))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:20110").await.unwrap();
    println!("Server is running on http://127.0.0.1:20110");
    axum::serve(listener, app).await.unwrap();
}
