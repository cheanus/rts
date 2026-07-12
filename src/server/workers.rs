use super::state::{ChannelMessage, ServerState, Task, TaskAction, TaskStatus};
use crate::server::state::TaskId;
use std::error::Error;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::process;
use tokio::sync::{
    MutexGuard,
    watch::{Receiver, Sender},
};

fn send_task_action(tx: Sender<ChannelMessage>, task_id: u32, task_action: TaskAction) {
    tx.send(ChannelMessage {
        task_id: Some(TaskId::Old(task_id)),
        task_action,
    })
    .expect("Channel sender failed send message");
}

fn create_task(
    task_id: u32,
    command: &str,
    tx: Sender<ChannelMessage>,
) -> Result<PathBuf, Box<dyn Error>> {
    let log = NamedTempFile::new_in("/tmp/rtx")?;

    let mut child = process::Command::new("sh")
        .arg("-c")
        .arg(command)
        .stdout(Stdio::from(log.reopen()?))
        .stderr(Stdio::from(log.reopen()?))
        .spawn()?;
    // 启用一个新线程监控新进程中所执行的命令
    tokio::spawn(async move {
        let status = child.wait().await;
        let task_action = match status {
            Ok(s) if s.success() => TaskAction::Complete,
            _ => TaskAction::Fail,
        };
        send_task_action(tx, task_id, task_action);
    });
    let (_file, persistent_path) = log.keep()?;
    Ok(persistent_path)
}

async fn try_create_tasks(
    mut used_slots: MutexGuard<'_, u32>,
    num_slots: u32,
    mut tasks: MutexGuard<'_, Vec<Task>>,
    tx: &Sender<ChannelMessage>,
) {
    for task in tasks.iter_mut() {
        // 槽位满则 break
        if *used_slots >= num_slots {
            break;
        }
        if task.status == TaskStatus::Pending {
            *used_slots += 1;
            task.status = TaskStatus::Running;
            match create_task(task.id, &task.command, tx.clone()) {
                Ok(log_path) => task.path = Some(log_path),
                Err(_) => send_task_action(tx.clone(), task.id, TaskAction::Fail),
            }
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
        let num_slots = *state.num_slots.lock().await;
        let mut used_slots = state.used_slots.lock().await;
        match task_id {
            TaskId::New => {
                // 尝试添加、运行新任务
                if task_action == TaskAction::Run {
                    try_create_tasks(used_slots, num_slots, tasks, &tx).await;
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
                        try_create_tasks(used_slots, num_slots, tasks, &tx).await;
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}
