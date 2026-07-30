use crate::errors::ServerError;
use crate::server::scheme::{
    ConfigureRequest, ListTaskResponse, PushTaskRequest, RemoveTaskRequest, TaskIdRequest,
};
use crate::server::state::{ChannelMessage, ServerState, Task, TaskAction, TaskStatus};
use axum::Json;
use axum::extract::{Query, State};
use chrono::Local;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use std::sync::Arc;

pub async fn push_task(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<PushTaskRequest>,
) -> Result<(), ServerError> {
    let log_path = request.log_path;
    // 验证 GPU 需求
    if let Some(gpu_req) = &request.gpu_requirement {
        if gpu_req.count == 0 {
            return Err(ServerError::InvalidParams("GPU count must be > 0".into()));
        }
        let gpu_infos_empty = state.gpu_infos.is_empty();
        if gpu_infos_empty {
            return Err(ServerError::InvalidParams(
                "No GPU available on this server".into(),
            ));
        }
        let pool = state.gpu_ids.lock().await;
        if gpu_req.count > pool.len() as u32 {
            return Err(ServerError::InvalidParams(format!(
                "Requested {} GPUs but only {} in pool",
                gpu_req.count,
                pool.len()
            )));
        }
    }
    let task = Task {
        label: request.label,
        status: TaskStatus::Pending,
        command: request.command,
        log_path,
        current_dir: request.current_dir,
        envs: request.envs,
        create_time: Local::now(),
        not_safely_depends: request.not_safely_depends,
        gpu_requirement: request.gpu_requirement,
        ..Default::default()
    };
    state.push_task(task, &request.dependencies).await
}

pub async fn remove_task(
    State(state): State<Arc<ServerState>>,
    Query(request): Query<RemoveTaskRequest>,
) -> Result<(), ServerError> {
    let mut tasks = state.tasks.lock().await;
    if request.is_all {
        let id_to_remove: Vec<u32> = tasks
            .iter()
            .filter(|(_, task)| !matches!(task.status, TaskStatus::Pending | TaskStatus::Running))
            .map(|(id, _)| *id)
            .collect();
        for id in id_to_remove {
            if tasks.remove(&id).is_none() {
                eprintln!("No task {} need to be remove", id);
            };
        }
        Ok(())
    } else {
        let task_id = request.task_id;
        match tasks.get(&task_id) {
            Some(task) if task.status == TaskStatus::Running => Err(ServerError::InvalidParams(
                format!("Task {task_id} is running!"),
            )),
            Some(_) => {
                tasks.remove(&task_id);
                Ok(())
            }
            None => Err(ServerError::InvalidParams(format!(
                "No task with ID {}",
                request.task_id
            ))),
        }
    }
}

pub async fn get_task_info(
    State(state): State<Arc<ServerState>>,
    Query(request): Query<TaskIdRequest>,
) -> Result<Json<Task>, ServerError> {
    let tasks = state.tasks.lock().await;
    match tasks.get(&request.task_id) {
        Some(task) => Ok(Json(task.clone())),
        None => Err(ServerError::InvalidParams(format!(
            "No task with ID {}",
            request.task_id
        ))),
    }
}

pub async fn kill_task(
    State(state): State<Arc<ServerState>>,
    Query(request): Query<TaskIdRequest>,
) -> Result<(), ServerError> {
    let tasks = state.tasks.lock().await;
    let Some(task) = tasks.get(&request.task_id) else {
        return Err(ServerError::InvalidParams(format!(
            "No task with ID {}",
            request.task_id
        )));
    };
    let Some(pid) = task.pid else {
        return Err(ServerError::InternalError(
            "The task may not has run".to_string(),
        ));
    };
    let Ok(pid) = i32::try_from(pid) else {
        return Err(ServerError::InternalError(
            "The task PID is too big!".to_string(),
        ));
    };
    let pid = Pid::from_raw(pid);
    kill(pid, Signal::SIGTERM).map_err(|e| ServerError::InternalError(e.to_string()))
}

pub async fn list_tasks(State(state): State<Arc<ServerState>>) -> Json<ListTaskResponse> {
    let tasks_snapshot = { state.tasks.lock().await.clone() };
    let num_slots = *state.num_slots.lock().await;
    let used_slots = *state.used_slots.lock().await;
    let gpu_ids = state.gpu_ids.lock().await.clone();
    let gpu_allocations = state.gpu_allocations.lock().await.clone();
    let list_tasks_json = ListTaskResponse {
        num_slots,
        used_slots,
        tasks: tasks_snapshot,
        gpu_ids,
        gpu_allocations,
    };
    Json(list_tasks_json)
}

pub async fn configure(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<ConfigureRequest>,
) -> Result<(), ServerError> {
    let mut sent = false;
    if let Some(num_slots) = request.num_slots {
        state.set_num_slots(num_slots).await?;
        sent = true;
    }
    if let Some(ref gpu_ids) = request.gpu_ids {
        let max_index = state.gpu_infos.last().map(|g| g.index).unwrap_or(0);
        if gpu_ids.iter().any(|id| *id > max_index) {
            return Err(ServerError::InvalidParams("GPU ID out of range".into()));
        }
        // 释放不再属于池的 GPU 上的分配
        let mut gpu_allocations = state.gpu_allocations.lock().await;
        gpu_allocations.retain(|_, assigned| assigned.iter().all(|idx| gpu_ids.contains(idx)));
        *state.gpu_ids.lock().await = gpu_ids.clone();
        sent = true;
    }
    if let Some(threshold) = request.gpu_threshold {
        if !(0.0..=1.0).contains(&threshold) {
            return Err(ServerError::InvalidParams(
                "GPU threshold must be between 0.0 and 1.0".into(),
            ));
        }
        *state.gpu_mem_threshold.lock().await = threshold;
        sent = true;
    }
    // 配置变更后触发调度
    if sent {
        state
            .tx
            .send(ChannelMessage {
                task_id: None,
                task_action: TaskAction::Run,
            })
            .map_err(|e| ServerError::InternalError(e.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    mod push_tests {
        use super::super::*;
        use crate::server::state::{ChannelMessage, GpuRequirement, TaskAction};
        use std::collections::HashMap;
        use std::error::Error;
        use std::path::PathBuf;
        use std::str::FromStr;
        use tokio::sync::watch;

        #[tokio::test]
        async fn test_push_task() -> Result<(), Box<dyn Error>> {
            let (tx, mut rx) = watch::channel(ChannelMessage {
                task_id: None,
                task_action: TaskAction::Complete,
            });
            let state = Arc::new(ServerState::new(1, None, vec![], tx));
            let request = PushTaskRequest {
                label: Some("test".to_string()),
                command: "echo hi".to_string(),
                log_path: Some(PathBuf::from_str("/tmp/rtx/test_push")?),
                current_dir: PathBuf::from_str("/")?,
                envs: HashMap::from([("PYTHONPATH".to_string(), "/".to_string())]),
                not_safely_depends: false,
                dependencies: Vec::new(),
                gpu_requirement: None,
            };
            push_task(State(Arc::clone(&state)), Json(request.clone())).await?;
            {
                let tasks = state.tasks.lock().await;
                let task0 = tasks.get(&0).unwrap();
                assert_eq!(task0.label, request.label);
                assert_eq!(task0.status, TaskStatus::Pending);
                assert_eq!(task0.command, request.command);
                assert_eq!(
                    task0.log_path,
                    Some(PathBuf::from_str("/tmp/rtx/test_push")?)
                );
                assert_eq!(task0.current_dir, request.current_dir);
                assert_eq!(task0.envs, request.envs);
            }
            rx.changed().await?;
            let message = *rx.borrow();
            assert_eq!(
                message,
                ChannelMessage {
                    task_id: None,
                    task_action: TaskAction::Run,
                }
            );
            Ok(())
        }

        #[tokio::test]
        async fn test_push_task_invalid_dependency() -> Result<(), Box<dyn Error>> {
            let (tx, _rx) = watch::channel(ChannelMessage {
                task_id: None,
                task_action: TaskAction::Complete,
            });
            let state = Arc::new(ServerState::new(1, None, vec![], tx));
            // Request with a dependency ID that doesn't exist (999)
            let request = PushTaskRequest {
                label: None,
                command: "echo hi".to_string(),
                log_path: None,
                current_dir: PathBuf::from_str("/")?,
                envs: HashMap::new(),
                not_safely_depends: false,
                dependencies: vec![999],
                gpu_requirement: None,
            };
            let result = push_task(State(Arc::clone(&state)), Json(request)).await;
            assert!(result.is_err());
            match result {
                Err(ServerError::InvalidParams(msg)) => {
                    assert_eq!(msg, "Invalid dependence task IDs");
                }
                _ => panic!("Expected ServerError::InvalidParams"),
            }
            Ok(())
        }

        #[tokio::test]
        async fn test_push_task_gpu_no_gpu_available() -> Result<(), Box<dyn Error>> {
            let (tx, _rx) = watch::channel(ChannelMessage {
                task_id: None,
                task_action: TaskAction::Complete,
            });
            let state = Arc::new(ServerState::new(1, None, vec![], tx));
            let request = PushTaskRequest {
                label: None,
                command: "echo hi".to_string(),
                log_path: None,
                current_dir: PathBuf::from_str("/")?,
                envs: HashMap::new(),
                not_safely_depends: false,
                dependencies: Vec::new(),
                gpu_requirement: Some(GpuRequirement {
                    count: 1,
                    min_free_mem_bytes: None,
                }),
            };
            let result = push_task(State(Arc::clone(&state)), Json(request)).await;
            assert!(result.is_err());
            match result {
                Err(ServerError::InvalidParams(msg)) => {
                    assert_eq!(msg, "No GPU available on this server");
                }
                _ => panic!("Expected ServerError::InvalidParams"),
            }
            Ok(())
        }
    }

    mod remove_tests {
        use super::super::*;
        use crate::server::state::{ChannelMessage, Task, TaskAction, TaskStatus};
        use std::error::Error;
        use tokio::sync::watch;

        #[tokio::test]
        async fn test_remove_running_task() -> Result<(), Box<dyn Error>> {
            let (tx, _rx) = watch::channel(ChannelMessage {
                task_id: None,
                task_action: TaskAction::Complete,
            });
            let state = Arc::new(ServerState::new(1, None, vec![], tx));
            {
                let task = Task {
                    status: TaskStatus::Running,
                    command: "echo hi".into(),
                    ..Default::default()
                };
                let mut tasks = state.tasks.lock().await;
                tasks.insert(0, task);
            }
            let result = remove_task(
                State(state),
                Query(RemoveTaskRequest {
                    task_id: 0,
                    is_all: false,
                }),
            )
            .await;
            assert!(result.is_err());
            Ok(())
        }

        #[tokio::test]
        async fn test_remove_unrun_task() -> Result<(), Box<dyn Error>> {
            let (tx, _rx) = watch::channel(ChannelMessage {
                task_id: None,
                task_action: TaskAction::Complete,
            });
            let state = Arc::new(ServerState::new(1, None, vec![], tx));
            {
                let task = Task {
                    status: TaskStatus::Completed,
                    command: "echo hi".into(),
                    ..Default::default()
                };
                let mut tasks = state.tasks.lock().await;
                tasks.insert(0, task);
            }
            let result = remove_task(
                State(state),
                Query(RemoveTaskRequest {
                    task_id: 0,
                    is_all: false,
                }),
            )
            .await;
            assert!(result.is_ok());
            Ok(())
        }

        #[tokio::test]
        async fn test_remove_all_task() -> Result<(), Box<dyn Error>> {
            let (tx, _rx) = watch::channel(ChannelMessage {
                task_id: None,
                task_action: TaskAction::Complete,
            });
            let state = Arc::new(ServerState::new(1, None, vec![], tx));
            {
                let mut tasks = state.tasks.lock().await;
                tasks.insert(
                    0,
                    Task {
                        status: TaskStatus::Pending,
                        command: "echo hi".into(),
                        ..Default::default()
                    },
                );
                tasks.insert(
                    1,
                    Task {
                        status: TaskStatus::Completed,
                        command: "echo hi".into(),
                        ..Default::default()
                    },
                );
            }
            let state_clone = Arc::clone(&state);
            let result = remove_task(
                State(state_clone),
                Query(RemoveTaskRequest {
                    task_id: 0,
                    is_all: true,
                }),
            )
            .await;
            assert!(result.is_ok());
            let tasks = state.tasks.lock().await;
            assert!(tasks.get(&0).is_some());
            assert!(tasks.get(&1).is_none());
            Ok(())
        }
    }

    mod info_tests {
        use super::super::*;
        use crate::server::state::{ChannelMessage, TaskAction, TaskStatus};
        use std::error::Error;
        use std::path::PathBuf;
        use tokio::sync::watch;

        #[tokio::test]
        async fn test_list_tasks() -> Result<(), Box<dyn Error>> {
            let (tx, _rx) = watch::channel(ChannelMessage {
                task_id: None,
                task_action: TaskAction::Complete,
            });
            let state = Arc::new(ServerState::new(1, None, vec![], tx));
            let task = Task {
                status: TaskStatus::Running,
                command: "echo hi".into(),
                log_path: Some(PathBuf::from("/tmp/a")),
                ..Default::default()
            };
            {
                let mut tasks = state.tasks.lock().await;
                tasks.insert(0, task.clone());
            }
            let Json(result) =
                get_task_info(State(state), Query(TaskIdRequest { task_id: 0 })).await?;
            assert_eq!(result, task);
            Ok(())
        }
    }

    mod kill_tests {
        use super::super::*;
        use crate::server::state::{ChannelMessage, Task, TaskAction, TaskStatus};
        use std::error::Error;
        use tokio::process;
        use tokio::sync::watch;

        #[tokio::test]
        async fn test_list_tasks() -> Result<(), Box<dyn Error>> {
            let (tx, _rx) = watch::channel(ChannelMessage {
                task_id: None,
                task_action: TaskAction::Complete,
            });
            let state = Arc::new(ServerState::new(1, None, vec![], tx));
            let mut child = process::Command::new("sh")
                .arg("-c")
                .arg("sleep 10")
                .spawn()?;
            let task = Task {
                status: TaskStatus::Running,
                command: "sleep 10".into(),
                pid: child.id(),
                ..Default::default()
            };
            {
                let mut tasks = state.tasks.lock().await;
                tasks.insert(0, task.clone());
            }
            kill_task(State(state), Query(TaskIdRequest { task_id: 0 })).await?;
            let status = child.wait().await?;
            assert_eq!(status.success(), false);
            assert!(status.code().is_none());
            Ok(())
        }
    }

    mod list_tests {
        use super::super::*;
        use crate::server::state::{ChannelMessage, Task, TaskAction, TaskStatus};
        use std::error::Error;
        use std::{collections::BTreeMap, path::PathBuf};
        use tokio::sync::watch;

        #[tokio::test]
        async fn test_list_tasks() -> Result<(), Box<dyn Error>> {
            let (tx, _rx) = watch::channel(ChannelMessage {
                task_id: None,
                task_action: TaskAction::Complete,
            });
            let state = Arc::new(ServerState::new(1, None, vec![], tx));
            let mut tasks = BTreeMap::new();
            tasks.insert(
                0,
                Task {
                    status: TaskStatus::Running,
                    command: "echo hi".into(),
                    log_path: Some(PathBuf::from("/tmp/a")),
                    ..Default::default()
                },
            );
            tasks.insert(
                1,
                Task {
                    label: Some("higher".to_string()),
                    status: TaskStatus::Pending,
                    command: "sleep 10".into(),
                    ..Default::default()
                },
            );
            *state.tasks.lock().await = tasks.clone();
            let Json(result) = list_tasks(State(state)).await;
            assert_eq!(result.num_slots, 1);
            assert_eq!(result.used_slots, 0);
            assert_eq!(result.tasks, tasks);
            Ok(())
        }
    }

    mod configure_tests {
        use super::super::*;
        use crate::server::state::{ChannelMessage, TaskAction};
        use std::error::Error;
        use tokio::sync::watch;

        #[tokio::test]
        async fn test_configure() -> Result<(), Box<dyn Error>> {
            let (tx, mut rx) = watch::channel(ChannelMessage {
                task_id: None,
                task_action: TaskAction::Complete,
            });
            let state = Arc::new(ServerState::new(1, None, vec![], tx));
            let request = ConfigureRequest {
                num_slots: Some(2),
                gpu_ids: None,
                gpu_threshold: None,
            };
            configure(State(Arc::clone(&state)), Json(request)).await?;
            assert_eq!(*state.num_slots.lock().await, 2);
            rx.changed().await?;
            let message = rx.borrow();
            assert_eq!(
                *message,
                ChannelMessage {
                    task_id: None,
                    task_action: TaskAction::Run,
                }
            );
            Ok(())
        }
    }
}
