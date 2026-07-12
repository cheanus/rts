use super::state::{ChannelMessage, ServerState, Task, TaskAction, TaskStatus};
use crate::server::state::TaskId;
use std::sync::Arc;
use tokio::process;
use tokio::sync::{
    MutexGuard,
    watch::{Receiver, Sender},
};
use tokio::time::{self, Duration};

fn new_task(task_id: u32, command: &str, tx: Sender<ChannelMessage>) {
    let child_result = process::Command::new("sh").arg("-c").arg(command).spawn();
    match child_result {
        Ok(mut child) => {
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
                            let task_action = match status {
                                Ok(s) if s.success() => TaskAction::Complete,
                                _ => TaskAction::Fail,
                            };
                            tx.send(ChannelMessage {
                                task_id: Some(TaskId::Old(task_id)),
                                task_action,
                            }).unwrap();
                            break;
                        }
                    }
                }
            });
        }
        Err(_) => {
            tx.send(ChannelMessage {
                task_id: Some(TaskId::Old(task_id)),
                task_action: TaskAction::Fail,
            })
            .unwrap();
        }
    }
}

async fn try_new_task(
    mut used_slots: MutexGuard<'_, u32>,
    num_slots: u32,
    mut tasks: MutexGuard<'_, Vec<Task>>,
    tx: Sender<ChannelMessage>,
) {
    for task in tasks.iter_mut() {
        // 槽位满则 break
        if *used_slots >= num_slots {
            break;
        }
        if task.status == TaskStatus::Pending {
            *used_slots += 1;
            task.status = TaskStatus::Running;
            new_task(task.id, &task.command, tx.clone());
        }
    }
}

pub async fn rx_worker(
    tx: Sender<ChannelMessage>,
    mut rx: Receiver<ChannelMessage>,
    state: Arc<ServerState>,
) -> Result<(), std::io::Error> {
    while rx.changed().await.is_ok() {
        let ChannelMessage {
            task_id,
            task_action,
        } = *rx.borrow();

        let Some(task_id) = task_id else {
            break;
        };

        let mut tasks = state.tasks.lock().await;
        let num_slots = state.num_slots.lock().await.clone();
        let mut used_slots = state.used_slots.lock().await;
        match task_id {
            TaskId::New => {
                // 尝试添加、运行新任务
                if task_action == TaskAction::Run {
                    try_new_task(used_slots, num_slots, tasks, tx.clone()).await;
                }
            }
            TaskId::Old(task_id) => {
                // 更新结束或失败任务的状态
                for task in tasks.iter_mut() {
                    if task.id == task_id {
                        match task_action {
                            TaskAction::Complete => {
                                task.status = TaskStatus::Completed;
                                *used_slots -= 1;
                            }
                            TaskAction::Fail => {
                                task.status = TaskStatus::Failed;
                                *used_slots -= 1;
                            }
                            _ => {}
                        }
                        try_new_task(used_slots, num_slots, tasks, tx.clone()).await;
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}
