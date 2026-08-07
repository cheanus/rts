pub mod args;

use crate::cli::args::DoTaskMode;
use crate::errors::CliError;
use crate::server::scheme::{
    ConfigureRequest, ListTaskResponse, PushTaskRequest, RemoveTaskRequest, TaskIdRequest,
};
use crate::server::state::{Task, TaskStatus};
use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub fn get_server_host() -> String {
    let server_port = env::var("RTS_SERVER_PORT").unwrap_or_else(|_| "20110".to_string());
    format!("127.0.0.1:{}", server_port)
}

pub async fn is_server_alive() -> bool {
    let client = RtsClient::new();
    client.get_health().await.is_ok()
}

pub struct RtsClient {
    client: reqwest::Client,
    server_host: String,
}

impl Default for RtsClient {
    fn default() -> Self {
        Self::new()
    }
}

impl RtsClient {
    pub fn new() -> Self {
        RtsClient {
            client: reqwest::Client::new(),
            server_host: get_server_host(),
        }
    }

    async fn get_health(&self) -> Result<(), CliError> {
        let response = self
            .client
            .get(format!("http://{}/health", self.server_host))
            .send()
            .await?;
        if response.error_for_status_ref().is_err() {
            return Err(CliError::Http {
                status: response.status(),
                body: response.json::<crate::errors::ResponseError>().await?,
            });
        }
        Ok(())
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, CliError> {
        let response = self
            .client
            .get(format!("http://{}{}", self.server_host, path))
            .send()
            .await?;
        if response.error_for_status_ref().is_err() {
            return Err(CliError::Http {
                status: response.status(),
                body: response.json::<crate::errors::ResponseError>().await?,
            });
        }
        Ok(response.json::<T>().await?)
    }

    async fn get_success(&self, path: &str) -> Result<(), CliError> {
        let response = self
            .client
            .get(format!("http://{}{}", self.server_host, path))
            .send()
            .await?;
        if response.error_for_status_ref().is_err() {
            return Err(CliError::Http {
                status: response.status(),
                body: response.json::<crate::errors::ResponseError>().await?,
            });
        }
        Ok(())
    }

    async fn post_success<T: serde::Serialize>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<(), CliError> {
        let response = self
            .client
            .post(format!("http://{}{}", self.server_host, path))
            .json(body)
            .send()
            .await?;
        if response.error_for_status_ref().is_err() {
            return Err(CliError::Http {
                status: response.status(),
                body: response.json::<crate::errors::ResponseError>().await?,
            });
        }
        Ok(())
    }

    pub async fn list_tasks(&self) -> Result<(), CliError> {
        let ListTaskResponse {
            num_slots,
            used_slots,
            tasks,
            gpu_ids,
            gpu_allocations,
        } = self.get_json("/tasks/list").await?;
        if !gpu_ids.is_empty() {
            println!(
                "GPU pool: [{}]",
                gpu_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if !gpu_allocations.is_empty() {
            for (&task_id, gpus) in &gpu_allocations {
                println!(
                    "  task#{} → GPU [{}]",
                    task_id,
                    gpus.iter()
                        .map(|(idx, _)| idx.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }
        println!(
            "ID\tLabel\tOutput\tStatus\tCommand ({}/{})",
            used_slots, num_slots
        );
        for (task_id, task) in tasks {
            println!(
                "{}\t{}\t{}\t{:?}\t{}",
                task_id,
                task.label.as_deref().unwrap_or(""),
                task.log_path.unwrap_or(PathBuf::from("")).display(),
                task.status,
                task.command
            )
        }
        Ok(())
    }

    pub async fn get_task_info(&self, task_id: u32) -> Result<(), CliError> {
        let query = TaskIdRequest { task_id };
        let task: Task = self
            .get_json(&format!("/tasks/info?task_id={}", query.task_id))
            .await?;
        println!("Status: {:?}", task.status);
        if let Some(pid) = task.pid {
            println!("PID: {}", pid);
        }
        if let Some(exit_code) = task.exit_code {
            println!("Exit code: {}", exit_code);
        }
        println!("Command: {}", task.command);
        if let Some(label) = task.label {
            println!("Label: {}", label);
        }
        println!(
            "Log path: {}",
            task.log_path.unwrap_or(PathBuf::from("")).display()
        );
        if !task.dependencies.is_empty() {
            println!(
                "Dependence: {}",
                task.dependencies
                    .keys()
                    .map(|id| id.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            );
        }
        println!("Create time: {}", task.create_time);
        if let Some(start_time) = task.start_time {
            println!("Start time: {}", start_time);
        }
        if let Some(end_time) = task.end_time {
            println!("End time: {}", end_time);
        }
        if let (Some(start_time), Some(end_time)) = (task.start_time, task.end_time) {
            let elapse_time = end_time - start_time;
            println!("Elapse time: {}", elapse_time);
        }
        Ok(())
    }

    pub async fn get_task_log(&self, task_id: u32, is_tail: bool) -> Result<(), CliError> {
        let query = TaskIdRequest { task_id };
        let task: Task = self
            .get_json(&format!("/tasks/info?task_id={}", query.task_id))
            .await?;
        let is_finished = matches!(
            task.status,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Killed | TaskStatus::Skipped
        );
        if is_finished {
            // 任务已结束：保持原有行为
            self.print_log(&task, is_tail).await
        } else {
            // 任务仍在运行/等待：实时输出日志，任务结束后退出
            self.follow_log(task_id, is_tail).await
        }
    }

    /// 打印日志文件内容：非 tail 输出全部，tail 输出末尾 10 行
    async fn print_log(&self, task: &Task, is_tail: bool) -> Result<(), CliError> {
        if let Some(log_path) = &task.log_path {
            let content = self.read_log_bytes(log_path).await?;
            print_log_lines(&content, is_tail);
        } else {
            eprintln!("No log file");
        }
        Ok(())
    }

    /// 实时跟踪日志：初始输出已有内容（cat/tail），随后持续输出新增内容，任务结束时退出
    async fn follow_log(&self, task_id: u32, is_tail: bool) -> Result<(), CliError> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

        let mut started = false; // 日志文件是否已出现并完成初始输出
        let mut pos: u64 = 0;
        let mut remaining: Vec<u8> = Vec::new();

        loop {
            let task: Task = self
                .get_json(&format!("/tasks/info?task_id={}", task_id))
                .await?;
            let is_finished = matches!(
                task.status,
                TaskStatus::Completed
                    | TaskStatus::Failed
                    | TaskStatus::Killed
                    | TaskStatus::Skipped
            );

            // 读取新增日志（任务可能尚未创建日志文件，此时跳过）
            if let Some(log_path) = &task.log_path {
                if let Ok(mut file) = tokio::fs::File::open(log_path).await {
                    let len = file.metadata().await.map(|m| m.len()).unwrap_or(0);
                    if !started {
                        // 首次出现日志文件：cat 输出全部，tail 输出末尾 10 行
                        let content = self.read_log_bytes(log_path).await.unwrap_or_default();
                        print_log_lines(&content, is_tail);
                        pos = content.len() as u64;
                        started = true;
                    } else if len > pos {
                        let mut buf = vec![0u8; (len - pos) as usize];
                        file.seek(SeekFrom::Start(pos)).await?;
                        file.read_exact(&mut buf).await?;
                        pos = len;
                        emit_log_lines(&mut remaining, &buf, false);
                    }
                }
            }

            if is_finished {
                // 任务结束：冲刷末尾没有换行的残余内容后退出
                emit_log_lines(&mut remaining, &[], true);
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok(())
    }

    async fn read_log_bytes(&self, log_path: &Path) -> Result<Vec<u8>, CliError> {
        use tokio::io::AsyncReadExt;
        let mut file = tokio::fs::File::open(log_path).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;
        Ok(buf)
    }

    pub async fn push_task(
        &self,
        label: Option<String>,
        path: Option<String>,
        mode: Option<crate::cli::args::DependTaskMode>,
        command: String,
        gpu: Option<u32>,
        gpu_mem: Option<f64>,
    ) -> Result<(), CliError> {
        let mut not_safely_depends: bool = false;
        let mut dependencies: Vec<u32> = Vec::new();
        if let Some(depend_mode) = mode {
            if let Some(waits) = depend_mode.wait {
                dependencies = waits;
            } else if let Some(delays) = depend_mode.delay {
                not_safely_depends = true;
                dependencies = delays;
            }
        }
        // 验证 -m 必须配合 -G 使用
        if gpu_mem.is_some() && gpu.is_none() {
            return Err(CliError::InvalidParams(
                "[-m] must be used with [-G]".into(),
            ));
        }
        let gpu_requirement = gpu.map(|count| crate::server::state::GpuRequirement {
            count,
            min_free_mem_bytes: gpu_mem.map(|gb| (gb * 1_073_741_824.0) as u64),
        });
        let data = PushTaskRequest {
            label,
            command,
            log_path: path.map(PathBuf::from),
            current_dir: env::current_dir()?,
            envs: env::vars().collect(),
            not_safely_depends,
            dependencies,
            gpu_requirement,
        };
        self.post_success("/tasks/push", &data).await
    }

    pub async fn remove_task(&self, task_id: u32, is_all: bool) -> Result<(), CliError> {
        let data = RemoveTaskRequest { task_id, is_all };
        self.get_success(&format!(
            "/tasks/remove?task_id={}&is_all={}",
            data.task_id, data.is_all
        ))
        .await
    }

    pub async fn kill_task(&self, task_id: u32) -> Result<(), CliError> {
        self.get_success(&format!("/tasks/kill?task_id={}", task_id))
            .await
    }

    pub async fn configure(
        &self,
        num_slots: Option<u32>,
        gpu_ids: Option<Vec<u32>>,
        gpu_threshold: Option<f64>,
    ) -> Result<(), CliError> {
        let data = ConfigureRequest {
            num_slots,
            gpu_ids,
            gpu_threshold,
        };
        self.post_success("/configure", &data).await
    }
}

/// 打印日志内容：非 tail 输出全部，tail 输出末尾 10 行
fn print_log_lines(content: &[u8], is_tail: bool) {
    let text = String::from_utf8_lossy(content);
    let lines: Vec<&str> = text.lines().collect();
    if is_tail {
        for line in lines.iter().rev().take(10).rev() {
            println!("{}", line);
        }
    } else {
        for line in &lines {
            println!("{}", line);
        }
    }
}

/// 追加新增日志字节并输出完整行；`flush` 时冲刷末尾没有换行的残余内容
fn emit_log_lines(remaining: &mut Vec<u8>, new: &[u8], flush: bool) {
    remaining.extend_from_slice(new);
    loop {
        match remaining.iter().position(|&b| b == b'\n') {
            Some(i) => {
                let line: Vec<u8> = remaining.drain(..=i).collect();
                println!("{}", String::from_utf8_lossy(&line[..line.len() - 1]));
            }
            None => break,
        }
    }
    if flush && !remaining.is_empty() {
        println!("{}", String::from_utf8_lossy(remaining));
        remaining.clear();
    }
}

pub async fn handle_do_command(mode: &DoTaskMode, client: &RtsClient) -> Result<(), CliError> {
    if let Some(id) = mode.info {
        client.get_task_info(id).await
    } else if let Some(id) = mode.cat {
        client.get_task_log(id, false).await
    } else if let Some(id) = mode.tail {
        client.get_task_log(id, true).await
    } else if let Some(id) = mode.remove {
        client.remove_task(id, false).await
    } else if mode.clear {
        client.remove_task(0, true).await
    } else if let Some(id) = mode.kill {
        client.kill_task(id).await
    } else {
        Ok(())
    }
}
