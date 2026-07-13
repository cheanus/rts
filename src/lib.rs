pub mod cli;
pub mod server;

use server::scheme::ListTaskResponse;
use std::collections::HashMap;
use std::env;
use std::error::Error;
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
            task.path.unwrap_or(PathBuf::from("")).display(),
            task.status,
            task.command
        )
    }
    Ok(())
}

pub async fn push_task(
    label: Option<String>,
    path: Option<String>,
    command: String,
) -> Result<(), Box<dyn Error>> {
    let server_host = get_server_host();
    let mut data = HashMap::new();
    data.insert("command", command);
    if let Some(p) = path {
        data.insert("path", p);
    }
    if let Some(l) = label {
        data.insert("label", l);
    }
    let client = reqwest::Client::new();
    client
        .post(format!("http://{server_host}/tasks/push"))
        .json(&data)
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
