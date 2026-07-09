mod errors;
mod handle;
pub mod scheme;
mod state;

use axum::{Router, routing::get, routing::post};
use state::{ChannelMessage, ServerState, TaskStatus};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::process;
use tokio::sync::{Mutex, watch};
use tokio::time::{self, Duration};

pub async fn server(server_host: String) {
    let state = Arc::new(ServerState {
        num_slots: Mutex::new(1),
        used_slots: Mutex::new(0),
        task_id_counter: Mutex::new(0),
        tasks: Mutex::new(Vec::new()),
    });

    // 用 watch channel 传递进程状态
    let (tx, rx) = watch::channel(ChannelMessage {
        task_id: 0,
        task_status: TaskStatus::Pending,
    });

    // 创建 worker 线程
    let worker_state = state.clone();
    tokio::spawn(async move {
        // 循环更新服务器状态
        loop {
            let mut tasks = worker_state.tasks.lock().await;
            for task in tasks.iter_mut() {
                let num_slots = worker_state.num_slots.lock().await.clone();
                let used_slots = worker_state.used_slots.lock().await.clone();
                // 如果槽位空余，寻找 Pending 任务并执行
                if num_slots > used_slots && matches!(task.status, TaskStatus::Pending) {
                    // 执行命令前，更新状态
                    task.status = TaskStatus::Running;
                    let mut inner_used_slots = worker_state.used_slots.lock().await;
                    *inner_used_slots += 1;
                    let child_result = process::Command::new("sh")
                        .arg("-c")
                        .arg(task.command.clone())
                        .spawn();
                    match child_result {
                        Ok(mut child) => {
                            let task_id = task.id;
                            let inner_tx = tx.clone();
                            // 启用一个新线程监控新进程中所执行的命令
                            tokio::spawn(async move {
                                let mut interval = time::interval(Duration::from_secs(1));
                                interval.tick().await; // skip first
                                loop {
                                    tokio::select! {
                                        _ = interval.tick() => {
                                            println!("[Monitor] task ID {} alive", task_id);
                                            // tx.send(...) 可更新更详细的状态
                                        }
                                        status = child.wait() => {
                                            let state = match status {
                                                Ok(s) if s.success() => TaskStatus::Completed,
                                                _ => TaskStatus::Failed,
                                            };
                                            let _ = inner_tx.send(ChannelMessage {
                                                task_id,
                                                task_status: state,
                                            });
                                            break;
                                        }
                                    }
                                }
                            });
                        }
                        Err(_) => {
                            task.status = TaskStatus::Failed;
                            continue;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    // 创建 rx 处理线程
    let mut rx_clone = rx.clone();
    let state_by_rx = state.clone();
    tokio::spawn(async move {
        while rx_clone.changed().await.is_ok() {
            let ChannelMessage {
                task_id,
                task_status,
            } = *rx_clone.borrow();
            let mut tasks = state_by_rx.tasks.lock().await;
            for task in tasks.iter_mut() {
                if task.id == task_id {
                    task.status = task_status;
                }
            }
            let mut used_slots = state_by_rx.used_slots.lock().await;
            *used_slots -= 1;
        }
    });

    let app = Router::new()
        .route("/health", get(|| async { "Hello, World!" }))
        .route("/tasks/list", get(handle::list_tasks))
        .route("/tasks/push", post(handle::push_task))
        .route("/configure", post(handle::configure))
        .with_state(state);

    let listener = TcpListener::bind(&server_host).await.unwrap();
    println!("Server is running on http://{}", &server_host);
    axum::serve(listener, app).await.unwrap();
}
