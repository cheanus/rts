pub mod cli;
pub mod server;

use server::scheme::ListTaskResponse;
use std::collections::HashMap;
use std::env;
use std::error::Error;

pub fn get_server_host() -> String {
    let server_port = env::var("RTS_SERVER_PORT").unwrap_or_else(|_| "20110".to_string());
    format!("127.0.0.1:{}", server_port)
}

pub async fn list_tasks() -> Result<(), Box<dyn Error>> {
    let server_host = get_server_host();
    println!("Task list");
    println!("ID\tlabel\tstatus\tcommand");
    let task_list = reqwest::get(format!("http://{server_host}/tasks/list"))
        .await?
        .json::<Vec<ListTaskResponse>>()
        .await?;
    for task in task_list {
        println!(
            "{}\t{}\t{:?}\t{}",
            task.id,
            task.label.as_deref().unwrap_or(""),
            task.status,
            task.command
        )
    }
    Ok(())
}

pub async fn push_task(command: String, label: Option<String>) -> Result<(), Box<dyn Error>> {
    let server_host = get_server_host();
    let mut data = HashMap::new();
    data.insert("command", command);
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
