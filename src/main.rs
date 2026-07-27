use clap::Parser;
use rts::cli;
use rts::cli::RtsClient;
use rts::server;

#[tokio::main]
async fn main() {
    let args = cli::args::Args::parse();

    let command = match args.command {
        Some(cmd) => cmd,
        None => {
            if cli::is_server_alive().await {
                cli::args::Commands::List
            } else {
                cli::args::Commands::Server
            }
        }
    };

    match command {
        cli::args::Commands::Server => {
            server::server(cli::get_server_host()).await;
        }
        cli::args::Commands::Run {
            label,
            path,
            mode,
            command,
        } => {
            let client = RtsClient::new();
            client
                .push_task(label, path, mode, command.join(" "))
                .await
                .unwrap_or_else(|e| eprintln!("Cannot push task: {}", e))
        }
        cli::args::Commands::List => {
            let client = RtsClient::new();
            client
                .list_tasks()
                .await
                .unwrap_or_else(|e| eprintln!("Cannot list tasks: {}", e))
        }
        cli::args::Commands::Do { mode } => {
            let client = RtsClient::new();
            cli::handle_do_command(&mode, &client)
                .await
                .unwrap_or_else(|e| eprintln!("Command failed: {}", e))
        }
        cli::args::Commands::Config { num_slots } => {
            let client = RtsClient::new();
            client
                .configure(num_slots)
                .await
                .unwrap_or_else(|e| eprintln!("Cannot configure: {}", e))
        }
    }
}
