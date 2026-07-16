use clap::Parser;
use rts::cli;
use rts::server;

#[tokio::main]
async fn main() {
    let args = cli::args::Args::parse();

    match args.command {
        cli::args::Commands::Server => {
            server::server(cli::get_server_host()).await;
        }
        cli::args::Commands::Run {
            label,
            path,
            command,
        } => cli::push_task(label, path, command.join(" "))
            .await
            .unwrap_or_else(|e| eprintln!("Cannot push task: {}", e)),
        cli::args::Commands::List => cli::list_tasks()
            .await
            .unwrap_or_else(|e| eprintln!("Cannot list tasks: {}", e)),
        cli::args::Commands::Do { mode } => {
            if let Some(id) = mode.info {
                cli::get_task_info(id).await.unwrap_or_else(|e| {
                    eprintln!("Cannot get task information with ID {}: {}", id, e)
                })
            } else if let Some(id) = mode.cat {
                cli::get_task_log(id, false)
                    .await
                    .unwrap_or_else(|e| eprintln!("Cannot get log of task with ID {}: {}", id, e))
            } else if let Some(id) = mode.tail {
                cli::get_task_log(id, true)
                    .await
                    .unwrap_or_else(|e| eprintln!("Cannot get log of task with ID {}: {}", id, e))
            } else if let Some(id) = mode.remove {
                cli::remove_task(id, false)
                    .await
                    .unwrap_or_else(|e| eprintln!("Cannot remove task with ID {}: {}", id, e))
            } else if mode.clear {
                cli::remove_task(0, true)
                    .await
                    .unwrap_or_else(|e| eprintln!("Cannot clear all tasks: {}", e))
            }
        }
        cli::args::Commands::Config { num_slots } => cli::configure(num_slots)
            .await
            .unwrap_or_else(|e| eprintln!("Cannot configure: {}", e)),
    }
}
