pub mod args;

use super::server::scheme::{ListTaskResponse, PushTaskRequest, RemoveTaskRequest, TaskIdRequest};
use super::server::state::Task;
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

    let ListTaskResponse {
        num_slots,
        used_slots,
        tasks,
    } = reqwest::get(format!("http://{server_host}/tasks/list"))
        .await?
        .json::<ListTaskResponse>()
        .await?;
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
    let task = client
        .get(format!("http://{server_host}/tasks/info"))
        .query(&TaskIdRequest { task_id })
        .send()
        .await?
        .json::<Task>()
        .await?;
    println!("Status: {:?}", task.status);
    println!("Command: {}", task.command);
    println!("Label: {}", task.label.as_deref().unwrap_or(""));
    println!(
        "Log path: {}",
        task.log_path.unwrap_or(PathBuf::from("")).display()
    );
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
    let task = client
        .get(format!("http://{server_host}/tasks/info"))
        .query(&TaskIdRequest { task_id })
        .send()
        .await?
        .json::<Task>()
        .await?;
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
    command: String,
) -> Result<(), Box<dyn Error>> {
    let server_host = get_server_host();
    let data = PushTaskRequest {
        label,
        command,
        log_path: path.map(|p| PathBuf::from(p)),
        current_dir: env::current_dir()?,
        envs: env::vars().collect(),
    };
    let client = reqwest::Client::new();
    client
        .post(format!("http://{server_host}/tasks/push"))
        .json(&data)
        .send()
        .await?;
    Ok(())
}

pub async fn remove_task(task_id: u32, is_all: bool) -> Result<(), Box<dyn Error>> {
    let server_host = get_server_host();
    let data = RemoveTaskRequest { task_id, is_all };
    let client = reqwest::Client::new();
    client
        .get(format!("http://{server_host}/tasks/remove"))
        .query(&data)
        .send()
        .await?;
    Ok(())
}

pub async fn configure(num_slots: u32) -> Result<(), Box<dyn Error>> {
    let server_host = get_server_host();
    let mut data = HashMap::new();
    data.insert("num_slots", num_slots);
    let client = reqwest::Client::new();
    client
        .post(format!("http://{server_host}/configure"))
        .json(&data)
        .send()
        .await?;
    Ok(())
}
