use crate::errors::ResponseError;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum ServerError {
    InvalidJson(String),
    InternalError(String),
    InvalidParams(String),
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            ServerError::InvalidJson(ref msg) => {
                (StatusCode::BAD_REQUEST, "INVALID_JSON", msg.as_str())
            }
            ServerError::InternalError(ref msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                msg.as_str(),
            ),
            ServerError::InvalidParams(ref msg) => {
                (StatusCode::BAD_REQUEST, "INVALID_PARAMS", msg.as_str())
            }
        };

        let body = json!(ResponseError {
            code: code.to_string(),
            message: message.to_string()
        });

        (status, Json(body)).into_response()
    }
}

impl From<serde_json::Error> for ServerError {
    fn from(e: serde_json::Error) -> Self {
        ServerError::InvalidJson(e.to_string())
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerError::InvalidJson(msg) => write!(f, "ServerError: Invalid JSON: {}", msg),
            ServerError::InternalError(msg) => write!(f, "ServerError: Internal error: {}", msg),
            ServerError::InvalidParams(msg) => write!(f, "ServerError: Invalid params: {}", msg),
        }
    }
}

impl Error for ServerError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_no_panic() {
        // Regression test: ensure Display doesn't recurse infinitely
        let err = ServerError::InternalError("test error".into());
        let msg = format!("{}", err);
        assert_eq!(msg, "ServerError: Internal error: test error");

        let err = ServerError::InvalidJson("bad json".into());
        let msg = format!("{}", err);
        assert_eq!(msg, "ServerError: Invalid JSON: bad json");

        let err = ServerError::InvalidParams("invalid".into());
        let msg = format!("{}", err);
        assert_eq!(msg, "ServerError: Invalid params: invalid");
    }
}
