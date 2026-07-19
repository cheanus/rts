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
        /// Optional dependence mode
        #[command(flatten)]
        mode: Option<DependTaskMode>,
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
        mode: DoTaskMode,
    },

    /// Configure the RTS server
    Config {
        /// Get/set the number of max simultaneous jobs
        #[arg(short = 'S')]
        num_slots: u32,
    },
}

#[derive(Debug, Parser)]
#[group(multiple = false)]
pub struct DoTaskMode {
    /// Get information of task
    #[arg(short, value_name = "ID")]
    pub info: Option<u32>,
    /// Cat log of task
    #[arg(short, value_name = "ID")]
    pub cat: Option<u32>,
    /// Tail log of task
    #[arg(short, value_name = "ID")]
    pub tail: Option<u32>,
    /// Remove a task
    #[arg(short, value_name = "ID")]
    pub remove: Option<u32>,
    /// Clear all tasks
    #[arg(short = 'C')]
    pub clear: bool,
    /// Kill a task
    #[arg(short, value_name = "ID")]
    pub kill: Option<u32>,
}

#[derive(Debug, Parser)]
#[group(multiple = false)]
pub struct DependTaskMode {
    /// The job will be run after the job of given IDs ends well (exit code 0).
    #[arg(short, value_name = "ID,...", value_delimiter = ',')]
    pub wait: Option<Vec<u32>>,
    /// The job will be run after the job of given IDs ends.
    #[arg(short, value_name = "ID,...", value_delimiter = ',')]
    pub delay: Option<Vec<u32>>,
}
