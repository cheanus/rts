use super::state::{ChannelMessage, ServerState, Task, TaskAction, TaskStatus};
use crate::server::state::TaskId;
use chrono::Local;
use std::collections::{BTreeMap, HashMap};
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

fn send_task_action(tx: &Sender<ChannelMessage>, task_id: u32, task_action: TaskAction) {
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
    current_dir: &PathBuf,
    envs: &HashMap<String, String>,
    tx: Sender<ChannelMessage>,
) -> Result<(Option<u32>, PathBuf), Box<dyn Error>> {
    // 创建 /tmp/rtx/ 临时目录
    fs::create_dir_all("/tmp/rtx").unwrap_or_else(|e| {
        eprintln!("Cannot create dir /tmp/rtx : {}", e);
        std::process::exit(1);
    });
    let mut child: process::Child;
    let persistent_path: PathBuf;
    if let Some(log_path) = log_path {
        // 有 log_path 则用作日志文件
        let log = File::create(log_path)?;

        child = process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .current_dir(current_dir)
            .envs(envs)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        persistent_path = log_path.clone();
    } else {
        // 没 log_path 则创建临时日志文件
        // 创建临时文件
        let log = NamedTempFile::new_in("/tmp/rtx")?;

        child = process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .current_dir(current_dir)
            .envs(envs)
            .stdout(Stdio::from(log.reopen()?))
            .stderr(Stdio::from(log.reopen()?))
            .spawn()?;
        let (_file, path) = log.keep()?;
        persistent_path = path;
    }

    let pid = child.id();

    // 启用一个新线程监控新进程中所执行的命令
    tokio::spawn(async move {
        let status = child.wait().await;
        let task_action = match status {
            Ok(s) => {
                if s.success() {
                    TaskAction::Complete
                } else {
                    if let Some(code) = s.code() {
                        TaskAction::Fail(code)
                    } else {
                        TaskAction::Fail(1)
                    }
                }
            }
            _ => TaskAction::Fail(1),
        };
        send_task_action(&tx, task_id, task_action);
    });
    Ok((pid, persistent_path))
}

async fn try_create_tasks(
    mut used_slots: MutexGuard<'_, u32>,
    num_slots: u32,
    mut tasks: MutexGuard<'_, BTreeMap<u32, Task>>,
    tx: &Sender<ChannelMessage>,
) {
    for (task_id, task) in tasks.iter_mut() {
        try_create_task(&mut used_slots, num_slots, *task_id, task, tx).await;
    }
}

async fn try_create_task(
    used_slots: &mut MutexGuard<'_, u32>,
    num_slots: u32,
    task_id: u32,
    task: &mut Task,
    tx: &Sender<ChannelMessage>,
) {
    // 槽位满则 break
    if **used_slots >= num_slots {
        return;
    }
    if task.status == TaskStatus::Pending {
        **used_slots += 1;
        task.status = TaskStatus::Running;
        task.start_time = Some(Local::now());
        match create_task(
            task_id,
            &task.command,
            &task.log_path,
            &task.current_dir,
            &task.envs,
            tx.clone(),
        ) {
            Ok((pid, log_path)) => {
                task.pid = pid;
                task.log_path = Some(log_path);
            }
            Err(_) => send_task_action(tx, task_id, TaskAction::Fail(1)),
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
                        task.end_time = Some(Local::now());
                        task.exit_code = Some(0);
                        *used_slots -= 1;
                        try_create_tasks(used_slots, num_slots, tasks, &tx).await;
                    }
                    TaskAction::Fail(code) => {
                        task.status = TaskStatus::Failed;
                        task.end_time = Some(Local::now());
                        task.exit_code = Some(code);
                        *used_slots -= 1;
                        try_create_tasks(used_slots, num_slots, tasks, &tx).await;
                    }
                    TaskAction::Run => {
                        try_create_task(&mut used_slots, num_slots, task_id, task, &tx).await
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use std::time::Duration;
    use tokio::sync::watch;
    use tokio::time;

    async fn get_tasks<'a>(state: &'a Arc<ServerState>) -> MutexGuard<'a, BTreeMap<u32, Task>> {
        state.tasks.lock().await
    }

    #[tokio::test]
    async fn test_rx_work() -> Result<(), Box<dyn Error>> {
        // 创建信道
        let (tx, rx) = watch::channel(ChannelMessage {
            task_id: None,
            task_action: TaskAction::Complete,
        });
        // 创建全局 state
        let server_state = ServerState::new(2, tx.clone());
        let state = Arc::new(server_state);
        let state_clone = Arc::clone(&state);

        // 运行 rx_worker 线程
        tokio::spawn(async move { rx_worker(tx, rx, state_clone).await });

        // 创建示例任务
        for task_id in 0..3 {
            state
                .push_task(Task {
                    command: format!("echo Hi task {task_id} && sleep 0.1"),
                    log_path: Some(PathBuf::from(format!("/tmp/rtx/test_worker_{task_id}"))),
                    current_dir: PathBuf::from_str("/")?,
                    ..Default::default()
                })
                .await?;
        }

        // 检查任务状态
        {
            time::sleep(Duration::from_millis(50)).await;
            let tasks_now = get_tasks(&state).await;
            assert_eq!(tasks_now.get(&0).unwrap().status, TaskStatus::Running);
            assert_eq!(
                tasks_now.get(&0).unwrap().log_path,
                Some(PathBuf::from(format!("/tmp/rtx/test_worker_0")))
            );
            assert_eq!(tasks_now.get(&1).unwrap().status, TaskStatus::Running);
            assert_eq!(
                tasks_now.get(&1).unwrap().log_path,
                Some(PathBuf::from(format!("/tmp/rtx/test_worker_1")))
            );
            assert_eq!(tasks_now.get(&2).unwrap().status, TaskStatus::Pending);
            // 检查日志文件内容
            assert_eq!(fs::read_to_string("/tmp/rtx/test_worker_0")?, "Hi task 0\n");
            assert_eq!(fs::read_to_string("/tmp/rtx/test_worker_1")?, "Hi task 1\n");
        }

        {
            time::sleep(Duration::from_millis(100)).await;
            let tasks_now = get_tasks(&state).await;
            // 检查结束时间
            assert!(tasks_now.get(&0).unwrap().end_time.is_some());
            assert!(tasks_now.get(&1).unwrap().end_time.is_some());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_exit_code() -> Result<(), Box<dyn Error>> {
        // 创建信道
        let (tx, rx) = watch::channel(ChannelMessage {
            task_id: None,
            task_action: TaskAction::Complete,
        });
        // 创建全局 state
        let server_state = ServerState::new(2, tx.clone());
        let state = Arc::new(server_state);
        let state_clone = Arc::clone(&state);

        // 运行 rx_worker 线程
        tokio::spawn(async move { rx_worker(tx, rx, state_clone).await });

        // 创建示例任务
        state
            .push_task(Task {
                command: format!("exit 127"),
                log_path: Some(PathBuf::from(format!("/tmp/rtx/test_exit_code"))),
                current_dir: PathBuf::from_str("/")?,
                ..Default::default()
            })
            .await?;

        // 检查任务状态
        time::sleep(Duration::from_millis(50)).await;
        let tasks_now = get_tasks(&state).await;
        // 检查退出码
        assert_eq!(tasks_now.get(&0).unwrap().status, TaskStatus::Failed);
        assert_eq!(tasks_now.get(&0).unwrap().exit_code, Some(127));

        Ok(())
    }
}
