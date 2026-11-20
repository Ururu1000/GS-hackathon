//! `ApiError` bodies: `{"error": {"code", "message", "current"?}}`.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub current: Option<Value>,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        ApiError {
            status,
            code,
            message: message.into(),
            current: None,
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    pub fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", "alert not found")
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", message)
    }

    pub fn conflict(code: &'static str, message: impl Into<String>, current: Value) -> Self {
        ApiError {
            status: StatusCode::CONFLICT,
            code,
            message: message.into(),
            current: Some(current),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        tracing::error!(error = format!("{e:#}"), "api error");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", "internal error")
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        anyhow::Error::from(e).into()
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = json!({"error": {"code": self.code, "message": self.message}});
        if let Some(c) = self.current {
            body["error"]["current"] = c;
        }
        (self.status, Json(body)).into_response()
    }
}
