use crate::errors::ResponseError;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum CliError {
    Http {
        status: reqwest::StatusCode,
        body: ResponseError,
    },
    Request(reqwest::Error),
    Io(std::io::Error),
    Json(serde_json::Error),
    Env(std::env::VarError),
    InvalidParams(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::Http { status, body } => {
                write!(f, "HTTP {}: {}", status.as_u16(), body.message)
            }
            CliError::Request(e) => write!(f, "Request error: {}", e),
            CliError::Io(e) => write!(f, "IO error: {}", e),
            CliError::Json(e) => write!(f, "JSON error: {}", e),
            CliError::Env(e) => write!(f, "Env error: {}", e),
            CliError::InvalidParams(msg) => write!(f, "Invalid params: {}", msg),
        }
    }
}

impl Error for CliError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CliError::Request(e) => Some(e),
            CliError::Io(e) => Some(e),
            CliError::Json(e) => Some(e),
            CliError::Env(e) => Some(e),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for CliError {
    fn from(e: reqwest::Error) -> Self {
        CliError::Request(e)
    }
}

impl From<std::io::Error> for CliError {
    fn from(e: std::io::Error) -> Self {
        CliError::Io(e)
    }
}

impl From<serde_json::Error> for CliError {
    fn from(e: serde_json::Error) -> Self {
        CliError::Json(e)
    }
}

impl From<std::env::VarError> for CliError {
    fn from(e: std::env::VarError) -> Self {
        CliError::Env(e)
    }
}
