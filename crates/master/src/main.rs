use axum::{
    Json, Router,
    extract::{Query, State, rejection::QueryRejection},
    http::{StatusCode, Uri},
    response::IntoResponse,
    routing::get,
};
use clap::Parser;
use core::{LookupResp, RegisterResp, TopicName};
use dashmap::DashMap;
use serde::Deserialize;
use std::{net::SocketAddr, sync::Arc};

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

async fn register(
    State(st): State<AppState>,
    uri: Uri,
    query: Result<Query<RegisterQ>, QueryRejection>,
) -> impl IntoResponse {
    match query {
        Ok(Query(q)) => {
            if let Some(existing) = st.topics.get(&q.topic.to_string()) {
                eprintln!(
                    "[master] warning: topic {} already registered",
                    existing.key()
                );
            } else {
                st.topics.insert(q.topic.to_string(), q.addr.clone());
                println!("[master] registered topic {}, addr={}", q.topic, q.addr);
            }
            (StatusCode::OK, Json(RegisterResp { ok: true }))
        }
        Err(err) => {
            eprintln!(
                "[master] register query deserialization error uri={} error={}",
                uri, err
            );
            (StatusCode::BAD_REQUEST, Json(RegisterResp { ok: false }))
        }
    }
}

async fn lookup(
    State(st): State<AppState>,
    uri: Uri,
    query: Result<Query<LookupQ>, QueryRejection>,
) -> impl IntoResponse {
    match query {
        Ok(Query(q)) => {
            let addr = st
                .topics
                .get(&q.topic.to_string())
                .map(|v| v.value().clone());
            println!(
                "[master] lookup uri={} topic={} addr={:?}",
                uri, q.topic, addr
            );
            (
                StatusCode::OK,
                Json(LookupResp {
                    name: q.topic,
                    addr,
                }),
            )
        }
        Err(err) => {
            eprintln!("[master] lookup invalid query uri={} error={}", uri, err);

            (
                StatusCode::BAD_REQUEST,
                Json(LookupResp {
                    name: TopicName::Undefined,
                    addr: None,
                }),
            )
        }
    }
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
    println!("[master] listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
