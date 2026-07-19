use crate::errors::ServerError;
use chrono::{DateTime, Local};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use tokio::sync::{Mutex, watch::Sender};

pub struct ServerState {
    pub num_slots: Mutex<u32>,
    pub used_slots: Mutex<u32>,
    pub tasks: Mutex<BTreeMap<u32, Task>>,
    task_id_counter: Mutex<u32>,
    tx: Sender<ChannelMessage>,
}

impl ServerState {
    pub fn new(num_slots: u32, tx: Sender<ChannelMessage>) -> Self {
        ServerState {
            num_slots: Mutex::new(num_slots),
            used_slots: Mutex::new(0),
            task_id_counter: Mutex::new(0),
            tasks: Mutex::new(BTreeMap::new()),
            tx: tx,
        }
    }

    pub async fn set_num_slots(&self, num_slots: u32) -> Result<(), ServerError> {
        let mut old_num_slots = self.num_slots.lock().await;
        if *old_num_slots < num_slots {
            // 有新槽位则检查新任务
            *old_num_slots = num_slots;
            let tx = &self.tx;
            tx.send(ChannelMessage {
                task_id: Some(TaskId::New),
                task_action: TaskAction::Run,
            })
            .map_err(|e| ServerError::InternalError(e.to_string()))?;
        } else {
            *old_num_slots = num_slots;
        }
        Ok(())
    }

    pub async fn push_task(
        &self,
        mut task: Task,
        dependence_ids: &[u32],
    ) -> Result<(), ServerError> {
        // 由于 state.tasks 是 BTreeMap，所以各 task 默认是按创建时间排序的
        let mut tasks = self.tasks.lock().await;
        // 验证 dependence_ids 有效性
        if tasks.iter().any(|(id, _)| tasks.get(id).is_none()) {
            return Err(ServerError::InvalidParams(
                "Invalid dependence task IDs".into(),
            ));
        }

        let mut dependencies = HashMap::new();
        let mut task_id_counter = self.task_id_counter.lock().await;
        for (id, t) in tasks
            .iter_mut()
            .filter(|(id, _)| dependence_ids.contains(*id))
        {
            dependencies.insert(*id, t.status);
            t.required.push(*task_id_counter);
        }
        task.dependencies = dependencies;

        tasks.insert(*task_id_counter, task);
        *task_id_counter += 1;

        let tx = &self.tx;
        tx.send(ChannelMessage {
            task_id: Some(TaskId::New),
            task_action: TaskAction::Run,
        })
        .map_err(|e| ServerError::InternalError(e.to_string()))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Task {
    pub label: Option<String>,
    pub status: TaskStatus,
    pub command: String,
    pub log_path: Option<PathBuf>,
    pub current_dir: PathBuf,
    pub envs: HashMap<String, String>,
    pub create_time: DateTime<Local>,
    pub start_time: Option<DateTime<Local>>,
    pub end_time: Option<DateTime<Local>>,
    pub pid: Option<u32>,
    pub exit_code: Option<i32>,
    pub not_safely_depends: bool,
    pub dependencies: HashMap<u32, TaskStatus>,
    pub required: Vec<u32>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TaskStatus {
    #[default]
    Pending,
    Running,
    Completed,
    Failed,
    Killed,
    Skipped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskId {
    Old(u32),
    New,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskAction {
    Run,
    Complete,
    Fail(i32),
    Kill,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChannelMessage {
    pub task_id: Option<TaskId>,
    pub task_action: TaskAction,
}
