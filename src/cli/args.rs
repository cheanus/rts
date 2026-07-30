use clap::{Parser, Subcommand};

/// Task orchestration
#[derive(Parser)]
#[command(
    version,
    about,
    long_about = "A task queue tool for deep learning written in Rust ",
    subcommand_required = false
)]
pub struct Args {
    #[command(subcommand)]
    pub command: Option<Commands>,
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
        /// 请求 N 张 GPU
        #[arg(short = 'G', value_name = "N")]
        gpu: Option<u32>,
        /// 每张 GPU 所需最低空闲显存（GB），须配合 -G 使用
        #[arg(short = 'm', value_name = "GB")]
        gpu_mem: Option<f64>,
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
        /// 设置最大并行任务数
        #[arg(short = 'S')]
        num_slots: Option<u32>,
        /// 设置可管理的 GPU 索引（逗号分隔），如 -G 0,1
        #[arg(short = 'G', value_delimiter = ',', value_name = "IDS")]
        gpu_ids: Option<Vec<u32>>,
        /// 设置 GPU 空闲显存阈值（0.0 ~ 1.0），默认 0.98
        #[arg(short = 'T', value_name = "THRESHOLD")]
        gpu_threshold: Option<f64>,
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
