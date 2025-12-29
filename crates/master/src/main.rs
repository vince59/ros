use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use core::{LookupResp, RegisterResp};
use dashmap::DashMap;
use serde::Deserialize;
use std::{net::SocketAddr, sync::Arc};

#[derive(Clone)]
struct AppState {
    topics: Arc<DashMap<String, String>>, // topic -> addr
}

#[derive(Deserialize)]
struct RegisterQ {
    topic: String,
    addr: String,
}

#[derive(Deserialize)]
struct LookupQ {
    topic: String,
}

async fn register(State(st): State<AppState>, Query(q): Query<RegisterQ>) -> Json<RegisterResp> {
    st.topics.insert(q.topic.clone(), q.addr.clone());
    println!("[master] registered topic {}", q.topic);
    Json(RegisterResp { ok: true })
}

async fn lookup(State(st): State<AppState>, Query(q): Query<LookupQ>) -> Json<LookupResp> {
    let addr = st.topics.get(&q.topic).map(|v| v.value().clone());
    println!("[master] lookup topic {}", q.topic);
    Json(LookupResp { topic: q.topic, addr })
}

#[tokio::main]
async fn main() {
    let st = AppState {
        topics: Arc::new(DashMap::new()),
    };

    let app = Router::new()
        .route("/register", get(register))
        .route("/lookup", get(lookup))
        .with_state(st);

    let addr: SocketAddr = "0.0.0.0:8080".parse().unwrap();
    println!("master listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
