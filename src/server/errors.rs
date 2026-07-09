use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(Debug)]
pub enum ServerError {
    InvalidJson(String),
    MissingFields(String),
    InternalError(String),
    InvalidParams(String),
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            ServerError::InvalidJson(ref msg) => {
                (StatusCode::BAD_REQUEST, "INVALID_JSON", msg.as_str())
            }
            ServerError::MissingFields(ref msg) => {
                (StatusCode::BAD_REQUEST, "MISSING_FIELDS", msg.as_str())
            }
            ServerError::InternalError(ref msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", msg.as_str())
            }
            ServerError::InvalidParams(ref msg) => {
                (StatusCode::BAD_REQUEST, "INVALID_PARAMS", msg.as_str())
            }
        };

        let body = json!({
            "error": message,
            "code": code,
        });

        (status, Json(body)).into_response()
    }
}

impl From<serde_json::Error> for ServerError {
    fn from(e: serde_json::Error) -> Self {
        ServerError::InvalidJson(e.to_string())
    }
}
