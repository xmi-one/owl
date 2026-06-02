//! 内置 HTTP API + WebSocket（Phase 3-1）。
//!
//! - 默认仅绑定 `127.0.0.1:8757`
//! - 可通过 `OWL_API_ADDR` 覆盖监听地址
//! - 可通过 `OWL_API_TOKEN` 开启 Bearer 鉴权

use std::net::SocketAddr;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::ipc::message::StartOptions;
use crate::process::manager::ManagerHandle;

#[derive(Clone)]
struct ApiState {
    mgr: ManagerHandle,
    token: Option<String>,
}

pub fn api_addr() -> SocketAddr {
    std::env::var("OWL_API_ADDR")
        .ok()
        .and_then(|s| s.parse::<SocketAddr>().ok())
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 8757)))
}

/// 启动 HTTP API 服务（持续运行直到被外部 abort）。
pub async fn serve(mgr: ManagerHandle) -> Result<(), String> {
    let addr = api_addr();
    let token = std::env::var("OWL_API_TOKEN").ok().filter(|s| !s.is_empty());
    let state = ApiState { mgr, token };

    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/processes", get(processes))
        .route("/api/processes/{target}", get(process_detail))
        .route("/api/processes/{target}/stop", post(stop))
        .route("/api/processes/{target}/restart", post(restart))
        .route("/api/processes/{target}/delete", post(delete))
        .route("/api/start", post(start))
        .route("/api/ws", get(ws))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("绑定 HTTP API 失败({addr}): {e}"))?;
    owl_logger::info!("HTTP API 已启动: http://{addr}");
    if let Err(e) = axum::serve(listener, app).await {
        return Err(format!("HTTP API 异常退出: {e}"));
    }
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "name": "owl",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

async fn processes(State(st): State<ApiState>, headers: HeaderMap) -> Response {
    if let Some(resp) = auth_failed(&headers, st.token.as_deref()) {
        return resp;
    }
    match st.mgr.list().await {
        Ok(list) => Json(json!({ "data": list })).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

async fn process_detail(
    State(st): State<ApiState>,
    Path(target): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(resp) = auth_failed(&headers, st.token.as_deref()) {
        return resp;
    }
    match st.mgr.info(target).await {
        Ok(info) => Json(json!({ "data": info })).into_response(),
        Err(e) => err(StatusCode::NOT_FOUND, &e.to_string()),
    }
}

async fn stop(
    State(st): State<ApiState>,
    Path(target): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(resp) = auth_failed(&headers, st.token.as_deref()) {
        return resp;
    }
    match st.mgr.stop(target).await {
        Ok(msg) => Json(json!({ "ok": true, "message": msg })).into_response(),
        Err(e) => err(StatusCode::BAD_REQUEST, &e.to_string()),
    }
}

async fn restart(
    State(st): State<ApiState>,
    Path(target): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(resp) = auth_failed(&headers, st.token.as_deref()) {
        return resp;
    }
    match st.mgr.restart(target).await {
        Ok(msg) => Json(json!({ "ok": true, "message": msg })).into_response(),
        Err(e) => err(StatusCode::BAD_REQUEST, &e.to_string()),
    }
}

async fn delete(
    State(st): State<ApiState>,
    Path(target): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(resp) = auth_failed(&headers, st.token.as_deref()) {
        return resp;
    }
    match st.mgr.delete(target).await {
        Ok(msg) => Json(json!({ "ok": true, "message": msg })).into_response(),
        Err(e) => err(StatusCode::BAD_REQUEST, &e.to_string()),
    }
}

async fn start(
    State(st): State<ApiState>,
    headers: HeaderMap,
    Json(opts): Json<StartOptions>,
) -> Response {
    if let Some(resp) = auth_failed(&headers, st.token.as_deref()) {
        return resp;
    }
    match st.mgr.start(opts).await {
        Ok(info) => Json(json!({ "ok": true, "data": info })).into_response(),
        Err(e) => err(StatusCode::BAD_REQUEST, &e.to_string()),
    }
}

async fn ws(
    State(st): State<ApiState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if let Some(resp) = auth_failed(&headers, st.token.as_deref()) {
        return resp;
    }
    ws.on_upgrade(move |socket| ws_loop(socket, st.mgr))
}

async fn ws_loop(mut socket: WebSocket, mgr: ManagerHandle) {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    loop {
        ticker.tick().await;
        let payload = match mgr.list().await {
            Ok(list) => json!({ "type": "process_list", "data": list }),
            Err(e) => json!({ "type": "error", "error": e.to_string() }),
        };
        if socket
            .send(Message::Text(payload.to_string().into()))
            .await
            .is_err()
        {
            break;
        }
    }
}

fn auth_failed(headers: &HeaderMap, token: Option<&str>) -> Option<Response> {
    let token = token?;
    let expected = format!("Bearer {token}");
    let got = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if got == expected {
        None
    } else {
        Some(err(StatusCode::UNAUTHORIZED, "unauthorized"))
    }
}

fn err(code: StatusCode, msg: &str) -> Response {
    (code, Json(json!({ "ok": false, "error": msg }))).into_response()
}
