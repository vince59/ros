use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use core::{LookupResp, RegisterResp, TopicName};
use dashmap::DashMap;
use serde::Deserialize;
use std::{net::SocketAddr, sync::Arc};
use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    port: String, // listening port for master ex: 8080
}

#[derive(Clone)]
struct AppState {
    topics: Arc<DashMap<String, String>>, // topic -> addr
}

#[derive(Deserialize)]
struct RegisterQ {
    topic: TopicName,
    addr: String,
}

#[derive(Deserialize)]
struct LookupQ {
    topic: TopicName,
}

async fn register(State(st): State<AppState>, Query(q): Query<RegisterQ>) -> Json<RegisterResp> {
    st.topics.insert(q.topic.to_string(), q.addr.clone());
    println!("[master] registered topic {}", q.topic.to_string());
    Json(RegisterResp { ok: true })
}

async fn lookup(State(st): State<AppState>, Query(q): Query<LookupQ>) -> Json<LookupResp> {
    let addr = st.topics.get(&q.topic.to_string()).map(|v| v.value().clone());
    println!("[master] lookup topic {}", q.topic.to_string());
    Json(LookupResp { name: q.topic, addr })
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let st = AppState {
        topics: Arc::new(DashMap::new()),
    };

    let app = Router::new()
        .route("/register", get(register))
        .route("/lookup", get(lookup))
        .with_state(st);

    let addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse().unwrap();
    println!("master listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
