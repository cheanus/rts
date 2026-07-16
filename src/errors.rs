pub mod cli;
pub mod server;

pub use cli::CliError;
pub use server::ServerError;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ResponseError {
    code: String,
    message: String,
}
