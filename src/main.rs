use clap::Parser;
use rts::cli;
use rts::server;

#[tokio::main]
async fn main() {
    let args = cli::args::Args::parse();

    match args.command {
        cli::args::Commands::Server => {
            server::server(rts::get_server_host()).await;
        }
        cli::args::Commands::Run {
            label,
            path,
            command,
        } => rts::push_task(label, path, command.join(" "))
            .await
            .unwrap_or_else(|e| eprintln!("Error pushing task: {}", e)),
        cli::args::Commands::List => rts::list_tasks()
            .await
            .unwrap_or_else(|e| eprintln!("Error listing tasks: {}", e)),
        cli::args::Commands::Config { num_slots } => rts::configure(num_slots)
            .await
            .unwrap_or_else(|e| eprintln!("Error configure: {}", e)),
    }
}
