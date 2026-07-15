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
            .unwrap_or_else(|e| eprintln!("Error pushing task: {}", e)),
        cli::args::Commands::List => cli::list_tasks()
            .await
            .unwrap_or_else(|e| eprintln!("Error listing tasks: {}", e)),
        cli::args::Commands::Get { id } => cli::get_task_info(id)
            .await
            .unwrap_or_else(|e| eprintln!("Error get task with id {}: {}", id, e)),
        cli::args::Commands::Config { num_slots } => cli::configure(num_slots)
            .await
            .unwrap_or_else(|e| eprintln!("Error configure: {}", e)),
    }
}
