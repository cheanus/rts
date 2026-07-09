mod cli;
mod server;

use clap::Parser;

fn main() {
    let args = cli::args::Args::parse();

    match args.command {
        cli::args::Commands::Server => {
            println!("Starting RTS server");
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(server::server());
        }
        cli::args::Commands::Run { command } => {
            println!("Executing command: {}", command.join(" "));
        }
        cli::args::Commands::Config { num_slot } => {
            println!("Configuring RTS server with {} simultaneous jobs", num_slot);
        }
    }
}
