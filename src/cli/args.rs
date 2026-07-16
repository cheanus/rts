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

    /// Operate tasks
    Do {
        // Choose mode to get task
        #[command(flatten)]
        mode: TaskMode,
    },

    /// Configure the RTS server
    Config {
        /// Get/set the number of max simultaneous jobs
        #[arg(short = 'S', default_value_t = 1)]
        num_slots: u32,
    },
}

#[derive(Debug, Parser)]
#[group(multiple = false)]
pub struct TaskMode {
    /// Get information of task with ID
    #[arg(short)]
    pub info: Option<u32>,
    /// Cat log of task with ID
    #[arg(short)]
    pub cat: Option<u32>,
    /// Tail log of task with ID
    #[arg(short)]
    pub tail: Option<u32>,
    /// Remove a task with ID
    #[arg(short)]
    pub remove: Option<u32>,
    /// Clear all tasks
    #[arg(short = 'C')]
    pub clear: bool,
}
