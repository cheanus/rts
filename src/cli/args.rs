use clap::{Parser, Subcommand};

/// Task orchestration
#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start the RTS server
    Server,

    /// Execute a command (trailing args)
    #[command(trailing_var_arg = true)]
    Run {
        /// Optional label for the command
        #[arg(short, long)]
        label: Option<String>,
        /// Optional log path
        #[arg(short, long)]
        path: Option<String>,
        /// The command to execute
        #[arg(required = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },

    /// List tasks
    List,

    /// Get a task
    Get {
        /// The id of task to get
        #[arg(short, long)]
        id: u32,
    },

    /// Configure the RTS server
    Config {
        /// Get/set the number of max simultaneous jobs
        #[arg(short = 'S', default_value_t = 1)]
        num_slots: u32,
    },
}
