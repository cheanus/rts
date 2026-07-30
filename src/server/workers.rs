use super::gpu;
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
use tokio::time::{self, Duration};

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
    state: &Arc<ServerState>,
) {
    for (task_id, task) in tasks.iter_mut() {
        try_create_task(&mut used_slots, num_slots, *task_id, task, tx, state).await;
    }
}
async fn try_create_task(
    used_slots: &mut MutexGuard<'_, u32>,
    num_slots: u32,
    task_id: u32,
    task: &mut Task,
    tx: &Sender<ChannelMessage>,
    state: &Arc<ServerState>,
) {
    // 槽位满则 return
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
        // GPU 资源检查（共享分配模式）
        let mut assigned_gpus: Vec<u32> = Vec::new();
        if let Some(gpu_req) = &task.gpu_requirement {
            let Some(nvml) = &state.nvml else {
                return; // NVML 未初始化，GPU 任务无法运行
            };
            let pool = state.gpu_ids.lock().await.clone();
            let mut gpu_allocations = state.gpu_allocations.lock().await;
            let threshold = *state.gpu_mem_threshold.lock().await;
            // 查询空闲显存
            let free_mem = gpu::query_gpu_free_memory(nvml, &pool);
            // 找到满足共享条件的 GPU（允许同一 GPU 运行多个任务）
            for gpu_info in &state.gpu_infos {
                if !pool.contains(&gpu_info.index) {
                    continue;
                }
                // 累加此 GPU 上所有运行中任务的预留显存
                let mut reserved_on_gpu: u64 = 0;
                for (_tid, gpu_list) in gpu_allocations.iter() {
                    for (gpu_idx, reserved) in gpu_list.iter() {
                        if *gpu_idx == gpu_info.index {
                            reserved_on_gpu += *reserved;
                        }
                    }
                }
                // 查询 NVML 实际空闲
                let nvml_free = free_mem.get(&gpu_info.index).copied().unwrap_or(0);
                let nvml_used = gpu_info.total_memory_bytes.saturating_sub(nvml_free);
                // effective_used = max(NVML 实际已用, 所有任务预留之和)
                let effective_used = nvml_used.max(reserved_on_gpu);
                let available = gpu_info.total_memory_bytes.saturating_sub(effective_used);
                let required = match gpu_req.min_free_mem_bytes {
                    Some(bytes) => bytes,
                    None => (gpu_info.total_memory_bytes as f64 * threshold) as u64,
                };
                if available >= required {
                    assigned_gpus.push(gpu_info.index);
                }
            }
            if (assigned_gpus.len() as u32) < gpu_req.count {
                return; // GPU 资源不足，保持 Pending
            }
            // 只取需要的数量
            assigned_gpus.truncate(gpu_req.count as usize);
            // 计算预留值（每张 GPU 为该任务预留的内存量）
            let per_gpu_reserved = match gpu_req.min_free_mem_bytes {
                Some(bytes) => bytes,
                None => state
                    .gpu_infos
                    .iter()
                    .find(|g| g.index == assigned_gpus[0])
                    .map(|g| (g.total_memory_bytes as f64 * threshold) as u64)
                    .unwrap_or(0),
            };
            gpu_allocations.insert(
                task_id,
                assigned_gpus
                    .iter()
                    .map(|idx| (*idx, per_gpu_reserved))
                    .collect(),
            );
            drop(gpu_allocations);
            // 注入 CUDA_VISIBLE_DEVICES
            task.envs.insert(
                "CUDA_VISIBLE_DEVICES".to_string(),
                assigned_gpus
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            );
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
    let mut tick = time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            biased;
            result = rx.changed() => {
                if result.is_err() {
                    break;
                }
                let ChannelMessage {
                    task_id,
                    task_action,
                } = *rx.borrow();

                let mut tasks = state.tasks.lock().await;
                let num_slots = *state.num_slots.lock().await;
                let mut used_slots = state.used_slots.lock().await;
                match task_id {
                    None => {
                        if task_action == TaskAction::Run {
                            try_create_tasks(used_slots, num_slots, tasks, &tx, &state).await;
                        }
                    }
                    Some(task_id) => {
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

                        // 释放 GPU 分配
                        state.gpu_allocations.lock().await.remove(&task_id);

                        update_required_status(task_id, status, &required, &mut tasks);
                        try_create_tasks(used_slots, num_slots, tasks, &tx, &state).await;
                    }
                }
            }
            _ = tick.tick() => {
                if state.gpu_infos.is_empty() {
                    continue;
                }
                // 检查是否有 pending GPU 任务，有则触发调度
                let has_pending_gpu = {
                    let tasks = state.tasks.lock().await;
                    tasks.values().any(|t| {
                        t.status == TaskStatus::Pending && t.gpu_requirement.is_some()
                    })
                };
                if has_pending_gpu {
                    let _ = tx.send(ChannelMessage {
                        task_id: None,
                        task_action: TaskAction::Run,
                    });
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
        let server_state = ServerState::new(num_slots, None, vec![], tx.clone());
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
