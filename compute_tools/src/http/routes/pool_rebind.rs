use crate::compute::ComputeNode;
use crate::http::{JsonResponse, extract::Json};
use crate::pool_rebind::{self, PoolRebindRequest};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use std::sync::Arc;

static OPERATIONS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn authorized(headers: &HeaderMap) -> bool {
    let Ok(token) = std::env::var("COMPUTE_CTL_POOL_API_TOKEN") else {
        return false;
    };
    bearer_matches(headers, &token)
}

fn bearer_matches(headers: &HeaderMap, token: &str) -> bool {
    if token.is_empty() {
        return false;
    }
    let Some(received) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
    else {
        return false;
    };
    if received.len() != token.len() {
        return false;
    }
    received
        .bytes()
        .zip(token.bytes())
        .fold(0u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}

pub(in crate::http) async fn binding_context(
    State(compute): State<Arc<ComputeNode>>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&headers) {
        return JsonResponse::error(StatusCode::UNAUTHORIZED, "invalid pool API token");
    }
    match pool_rebind::context(&compute) {
        Ok(body) => JsonResponse::success(StatusCode::OK, body),
        Err(error) => JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}

pub(in crate::http) async fn pool_rebind(
    State(compute): State<Arc<ComputeNode>>,
    headers: HeaderMap,
    request: Json<PoolRebindRequest>,
) -> Response {
    if !authorized(&headers) {
        return JsonResponse::error(StatusCode::UNAUTHORIZED, "invalid pool API token");
    }
    // A second request receives a conflict rather than waiting and accidentally
    // rebinding an instance already changed by another operation.
    let Ok(guard) = OPERATIONS.try_lock() else {
        return JsonResponse::error(StatusCode::CONFLICT, "pool operation already in progress");
    };
    // Move the lock into the blocking worker so client cancellation cannot unlock
    // the operation while that worker continues.
    match tokio::task::spawn_blocking(move || {
        let _guard = guard;
        pool_rebind::rebind(&compute, request.0)
    })
    .await
    {
        Ok(Ok(body)) => JsonResponse::success(StatusCode::OK, body),
        Ok(Err((status, error))) => JsonResponse::error(status, error),
        Err(error) => JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_auth_is_fail_closed_and_requires_exact_bearer() {
        let mut headers = HeaderMap::new();
        assert!(!bearer_matches(&headers, "test-token"));
        headers.insert("authorization", "Bearer test-token".parse().unwrap());
        assert!(!bearer_matches(&headers, ""));
        assert!(!bearer_matches(&headers, "test-tokeo"));
        assert!(!bearer_matches(&headers, "other"));
        assert!(bearer_matches(&headers, "test-token"));
        headers.insert("authorization", "Basic test-token".parse().unwrap());
        assert!(!bearer_matches(&headers, "test-token"));
    }
}
