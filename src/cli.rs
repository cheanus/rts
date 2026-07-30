pub mod args;

use crate::cli::args::DoTaskMode;
use crate::errors::CliError;
use crate::server::scheme::{
    ConfigureRequest, ListTaskResponse, PushTaskRequest, RemoveTaskRequest, TaskIdRequest,
};
use crate::server::state::Task;
use rev_buf_reader::RevBufReader;
use std::env;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

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
        if let Some(log_path) = task.log_path {
            let file = fs::File::open(log_path)?;
            if !is_tail {
                let reader = BufReader::new(file);
                for line in reader.lines() {
                    println!("{}", line?);
                }
            } else {
                let reader = RevBufReader::new(file);
                for line in
                    reader
                        .lines()
                        .take(10)
                        .collect::<Result<Vec<_>, _>>()
                        .map(|mut v| {
                            v.reverse();
                            v
                        })?
                {
                    println!("{}", line);
                }
            }
        } else {
            eprintln!("No log file");
        }
        Ok(())
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
