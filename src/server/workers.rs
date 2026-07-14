use super::state::{ChannelMessage, ServerState, Task, TaskAction, TaskStatus};
use crate::server::state::TaskId;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::{self, File};
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
    log_path: &Option<PathBuf>,
    tx: Sender<ChannelMessage>,
) -> Result<Option<PathBuf>, Box<dyn Error>> {
    // 创建 /tmp/rtx/ 临时目录
    fs::create_dir_all("/tmp/rtx").unwrap_or_else(|e| {
        eprintln!("Cannot create dir /tmp/rtx : {}", e);
        std::process::exit(1);
    });
    match log_path {
        // 有 log_path 则用作日志文件
        Some(log_path) => {
            let log = File::create(log_path)?;

            let mut child = process::Command::new("sh")
                .arg("-c")
                .arg(command)
                .stdout(Stdio::from(log.try_clone()?))
                .stderr(Stdio::from(log))
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
            Ok(None)
        }
        // 没 log_path 则创建临时日志文件
        None => {
            // 创建临时文件
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
            Ok(Some(persistent_path))
        }
    }
}

async fn try_create_tasks(
    mut used_slots: MutexGuard<'_, u32>,
    num_slots: u32,
    mut tasks: MutexGuard<'_, BTreeMap<u32, Task>>,
    tx: &Sender<ChannelMessage>,
) {
    for (task_id, task) in tasks.iter_mut() {
        // 槽位满则 break
        if *used_slots >= num_slots {
            break;
        }
        if task.status == TaskStatus::Pending {
            *used_slots += 1;
            task.status = TaskStatus::Running;
            match create_task(*task_id, &task.command, &task.path, tx.clone()) {
                Ok(Some(log_path)) => task.path = Some(log_path),
                Ok(None) => (),
                Err(_) => send_task_action(tx.clone(), *task_id, TaskAction::Fail),
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
            continue;
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
                let Some((_, task)) = tasks.iter_mut().find(|(i, _)| **i == task_id) else {
                    eprintln!("Cannot find task with ID {}", task_id);
                    continue;
                };
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
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::{Mutex, watch};
    use tokio::time;

    async fn is_tasks_status_eq(state: Arc<ServerState>, true_state: [(u32, TaskStatus); 3]) {
        let status_list: BTreeMap<u32, TaskStatus> = state
            .tasks
            .lock()
            .await
            .iter()
            .map(|(id, task)| (*id, task.status))
            .collect();
        assert_eq!(status_list, BTreeMap::from(true_state));
    }

    #[tokio::test]
    async fn test_rx_worker() -> Result<(), Box<dyn Error>> {
        // 创建信道
        let (tx, rx) = watch::channel(ChannelMessage {
            task_id: None,
            task_action: TaskAction::Complete,
        });
        // 创建示例任务
        let mut tasks = BTreeMap::new();
        for task_id in 0..3 {
            tasks.insert(
                task_id,
                Task {
                    command: format!("echo Hi task {task_id} && sleep 3"),
                    path: Some(PathBuf::from(format!("/tmp/rtx/test_worker_{task_id}"))),
                    ..Default::default()
                },
            );
        }
        // 创建全局 state
        let state = Arc::new(ServerState {
            num_slots: Mutex::new(2),
            used_slots: Mutex::new(0),
            task_id_counter: Mutex::new(3),
            tasks: Mutex::new(tasks),
            tx: Mutex::new(tx.clone()),
        });
        let tx_clone = tx.clone();
        let state_clone = Arc::clone(&state);
        // 运行 rx_worker 线程
        tokio::spawn(async move { rx_worker(tx_clone, rx, state_clone).await });

        // 发送新任务信号
        tx.send(ChannelMessage {
            task_id: Some(TaskId::New),
            task_action: TaskAction::Run,
        })?;
        time::sleep(Duration::from_millis(100)).await;
        // 检查任务状态
        is_tasks_status_eq(
            state,
            [
                (0, TaskStatus::Running),
                (1, TaskStatus::Running),
                (2, TaskStatus::Pending),
            ],
        )
        .await;
        // 检查日志文件内容
        assert_eq!(fs::read_to_string("/tmp/rtx/test_worker_0")?, "Hi task 0\n");
        assert_eq!(fs::read_to_string("/tmp/rtx/test_worker_1")?, "Hi task 1\n");
        Ok(())
    }
}
