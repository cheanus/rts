pub mod args;

use super::cli::args::DependTaskMode;
use super::errors::CliError;
use super::server::scheme::{ListTaskResponse, PushTaskRequest, RemoveTaskRequest, TaskIdRequest};
use super::server::state::Task;
use crate::errors::ResponseError;
use rev_buf_reader::RevBufReader;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

pub fn get_server_host() -> String {
    let server_port = env::var("RTS_SERVER_PORT").unwrap_or_else(|_| "20110".to_string());
    format!("127.0.0.1:{}", server_port)
}

pub async fn list_tasks() -> Result<(), Box<dyn Error>> {
    let server_host = get_server_host();
    let response = reqwest::get(format!("http://{server_host}/tasks/list")).await?;
    if response.error_for_status_ref().is_err() {
        return Err(Box::new(CliError(response.json::<ResponseError>().await?)));
    }

    let ListTaskResponse {
        num_slots,
        used_slots,
        tasks,
    } = response.json::<ListTaskResponse>().await?;
    println!("Task list");
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

pub async fn get_task_info(task_id: u32) -> Result<(), Box<dyn Error>> {
    let server_host = get_server_host();
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{server_host}/tasks/info"))
        .query(&TaskIdRequest { task_id })
        .send()
        .await?;
    if response.error_for_status_ref().is_err() {
        return Err(Box::new(CliError(response.json::<ResponseError>().await?)));
    }
    let task = response.json::<Task>().await?;
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
                .iter()
                .map(|(id, _)| id.to_string())
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

pub async fn get_task_log(task_id: u32, is_tail: bool) -> Result<(), Box<dyn Error>> {
    let server_host = get_server_host();
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{server_host}/tasks/info"))
        .query(&TaskIdRequest { task_id })
        .send()
        .await?;
    if response.error_for_status_ref().is_err() {
        return Err(Box::new(CliError(response.json::<ResponseError>().await?)));
    }
    let task = response.json::<Task>().await?;
    if let Some(log_path) = task.log_path {
        let file = fs::File::open(log_path)?;
        if !is_tail {
            // 逐行读取
            let reader = BufReader::new(file);
            for line in reader.lines() {
                println!("{}", line?);
            }
        } else {
            let reader = RevBufReader::new(file);
            for line in reader
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
    label: Option<String>,
    path: Option<String>,
    mode: Option<DependTaskMode>,
    command: String,
) -> Result<(), Box<dyn Error>> {
    let server_host = get_server_host();
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
    let data = PushTaskRequest {
        label,
        command,
        log_path: path.map(|p| PathBuf::from(p)),
        current_dir: env::current_dir()?,
        envs: env::vars().collect(),
        not_safely_depends,
        dependencies,
    };
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{server_host}/tasks/push"))
        .json(&data)
        .send()
        .await?;
    if response.error_for_status_ref().is_err() {
        return Err(Box::new(CliError(response.json::<ResponseError>().await?)));
    }
    Ok(())
}

pub async fn remove_task(task_id: u32, is_all: bool) -> Result<(), Box<dyn Error>> {
    let server_host = get_server_host();
    let data = RemoveTaskRequest { task_id, is_all };
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{server_host}/tasks/remove"))
        .query(&data)
        .send()
        .await?;
    if response.error_for_status_ref().is_err() {
        return Err(Box::new(CliError(response.json::<ResponseError>().await?)));
    }
    Ok(())
}

pub async fn kill_task(task_id: u32) -> Result<(), Box<dyn Error>> {
    let server_host = get_server_host();
    let data = TaskIdRequest { task_id };
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{server_host}/tasks/kill"))
        .query(&data)
        .send()
        .await?;
    if response.error_for_status_ref().is_err() {
        return Err(Box::new(CliError(response.json::<ResponseError>().await?)));
    }
    Ok(())
}

pub async fn configure(num_slots: u32) -> Result<(), Box<dyn Error>> {
    let server_host = get_server_host();
    let mut data = HashMap::new();
    data.insert("num_slots", num_slots);
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{server_host}/configure"))
        .json(&data)
        .send()
        .await?;
    if response.error_for_status_ref().is_err() {
        return Err(Box::new(CliError(response.json::<ResponseError>().await?)));
    }
    Ok(())
}
