use super::state::{ChannelMessage, ServerState, Task, TaskAction, TaskStatus};
use crate::errors::ServerError;
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
    let _ = tx.send(ChannelMessage {
        task_id: Some(task_id),
        task_action,
    });
}

fn update_required_status(
    task_id: u32,
    task_status: TaskStatus,
    required: &[u32],
    tasks: &mut BTreeMap<u32, Task>,
) {
    for (required_id, required_t) in tasks.iter_mut().filter(|(id, _)| required.contains(*id)) {
        if let Some(t) = required_t.dependencies.get_mut(&task_id) {
            *t = task_status;
            if !required_t.not_safely_depends
                && matches!(
                    task_status,
                    TaskStatus::Failed | TaskStatus::Killed | TaskStatus::Skipped
                )
            {
                required_t.status = TaskStatus::Skipped;
            }
        } else {
            eprintln!(
                "Task {} is dependent on task {}, but task {} does not require task {}",
                task_id, required_id, required_id, task_id
            );
        }
    }
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
    if let Err(e) = fs::create_dir_all("/tmp/rtx") {
        return Err(Box::new(ServerError::InternalError(format!(
            "Cannot create dir /tmp/rtx : {}",
            e
        ))));
    };
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
                        TaskAction::Kill
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
        // 检查依赖状态
        if !task.not_safely_depends {
            let is_dependence_over = task
                .dependencies
                .iter()
                .all(|(_, s)| *s == TaskStatus::Completed);
            if !is_dependence_over {
                return;
            }
        }
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

        let mut tasks = state.tasks.lock().await;
        let num_slots = *state.num_slots.lock().await;
        let mut used_slots = state.used_slots.lock().await;
        match task_id {
            None => {
                // 尝试添加、运行新任务
                if task_action == TaskAction::Run {
                    try_create_tasks(used_slots, num_slots, tasks, &tx).await;
                }
            }
            Some(task_id) => {
                // 更新结束或失败任务的状态
                let Some(task) = tasks.get_mut(&task_id) else {
                    eprintln!("Cannot find task with ID {}", task_id);
                    continue;
                };
                match task_action {
                    TaskAction::Complete => {
                        task.status = TaskStatus::Completed;
                        task.exit_code = Some(0);
                    }
                    TaskAction::Fail(code) => {
                        task.status = TaskStatus::Failed;
                        task.exit_code = Some(code);
                    }
                    TaskAction::Kill => {
                        task.status = TaskStatus::Killed;
                        task.exit_code = Some(1);
                    }
                    TaskAction::Run => {
                        eprintln!("Cannot start given task {}", task_id);
                        continue;
                    }
                }
                task.end_time = Some(Local::now());
                let status = task.status;
                let required = task.required.clone();

                *used_slots -= 1;

                update_required_status(task_id, status, &required, &mut tasks);
                try_create_tasks(used_slots, num_slots, tasks, &tx).await;
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
    use tokio::time::{self, timeout};

    async fn get_tasks<'a>(state: &'a Arc<ServerState>) -> MutexGuard<'a, BTreeMap<u32, Task>> {
        state.tasks.lock().await
    }

    async fn wait_for_status(state: &Arc<ServerState>, task_id: u32, expected: TaskStatus) {
        timeout(Duration::from_secs(5), async {
            loop {
                let tasks = state.tasks.lock().await;
                if let Some(task) = tasks.get(&task_id) {
                    if task.status == expected {
                        return;
                    }
                }
                drop(tasks);
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "Timeout waiting for task {} to become {:?}",
                task_id, expected
            )
        });
    }

    async fn wait_for_end_time(state: &Arc<ServerState>, task_id: u32) {
        timeout(Duration::from_secs(5), async {
            loop {
                let tasks = state.tasks.lock().await;
                if let Some(task) = tasks.get(&task_id) {
                    if task.end_time.is_some() {
                        return;
                    }
                }
                drop(tasks);
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("Timeout waiting for task {} end_time", task_id));
    }

    fn init_worker(num_slots: u32) -> Arc<ServerState> {
        // 创建信道
        let (tx, rx) = watch::channel(ChannelMessage {
            task_id: None,
            task_action: TaskAction::Complete,
        });
        // 创建全局 state
        let server_state = ServerState::new(num_slots, tx.clone());
        let state = Arc::new(server_state);
        let state_clone = Arc::clone(&state);

        // 运行 rx_worker 线程
        tokio::spawn(async move { rx_worker(tx, rx, state_clone).await });
        state
    }

    #[tokio::test]
    async fn test_rx_work() -> Result<(), Box<dyn Error>> {
        let state = init_worker(2);

        // 创建示例任务
        for task_id in 0..3 {
            state
                .push_task(
                    Task {
                        command: format!("echo Hi task {task_id} && sleep 0.1"),
                        log_path: Some(PathBuf::from(format!("/tmp/rtx/test_worker_{task_id}"))),
                        current_dir: PathBuf::from_str("/")?,
                        ..Default::default()
                    },
                    &[],
                )
                .await?;
        }

        // Wait for tasks 0,1 to start running
        wait_for_status(&state, 0, TaskStatus::Running).await;
        wait_for_status(&state, 1, TaskStatus::Running).await;

        // Verify task 2 is still pending
        {
            let tasks_now = get_tasks(&state).await;
            assert_eq!(tasks_now.get(&2).unwrap().status, TaskStatus::Pending);
        }

        // 检查日志文件内容 (need a small extra wait for echo output)
        time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fs::read_to_string("/tmp/rtx/test_worker_0")?, "Hi task 0\n");
        assert_eq!(fs::read_to_string("/tmp/rtx/test_worker_1")?, "Hi task 1\n");

        // Wait for tasks to complete
        wait_for_end_time(&state, 0).await;
        wait_for_end_time(&state, 1).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_exit_code() -> Result<(), Box<dyn Error>> {
        let state = init_worker(2);

        // 创建示例任务
        state
            .push_task(
                Task {
                    command: format!("exit 127"),
                    log_path: Some(PathBuf::from(format!("/tmp/rtx/test_exit_code"))),
                    current_dir: PathBuf::from_str("/")?,
                    ..Default::default()
                },
                &[],
            )
            .await?;

        // 检查任务状态
        wait_for_status(&state, 0, TaskStatus::Failed).await;
        let tasks_now = get_tasks(&state).await;
        assert_eq!(tasks_now.get(&0).unwrap().exit_code, Some(127));

        Ok(())
    }

    #[tokio::test]
    async fn test_dependence() -> Result<(), Box<dyn Error>> {
        let state = init_worker(2);

        // 创建示例任务
        for task_id in 0..3 {
            state
                .push_task(
                    Task {
                        command: format!("sleep 0.1"),
                        log_path: Some(PathBuf::from(format!(
                            "/tmp/rtx/test_dependence_{task_id}"
                        ))),
                        current_dir: PathBuf::from_str("/")?,
                        ..Default::default()
                    },
                    &[],
                )
                .await?;
        }
        state
            .push_task(
                Task {
                    command: format!("sleep 0.1"),
                    log_path: Some(PathBuf::from(format!("/tmp/rtx/test_dependence_4"))),
                    current_dir: PathBuf::from_str("/")?,
                    ..Default::default()
                },
                &[0, 1, 2],
            )
            .await?;

        // Wait for tasks 0,1 to start running
        wait_for_status(&state, 0, TaskStatus::Running).await;
        wait_for_status(&state, 1, TaskStatus::Running).await;

        // Tasks 2,3 should be pending
        {
            let tasks_now = get_tasks(&state).await;
            assert_eq!(tasks_now.get(&2).unwrap().status, TaskStatus::Pending);
            assert_eq!(tasks_now.get(&3).unwrap().status, TaskStatus::Pending);
        }

        // Wait for tasks 0,1 to complete
        wait_for_status(&state, 0, TaskStatus::Completed).await;
        wait_for_status(&state, 1, TaskStatus::Completed).await;

        // Task 2 should now be running (dependency satisfied)
        wait_for_status(&state, 2, TaskStatus::Running).await;

        // Task 3 still pending (waiting for task 2)
        {
            let tasks_now = get_tasks(&state).await;
            assert_eq!(tasks_now.get(&3).unwrap().status, TaskStatus::Pending);
        }

        Ok(())
    }
}
