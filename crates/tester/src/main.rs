use anyhow::Context;
use bytes::Bytes;
use clap::Parser;
use core::{decode, encode, WireMsg};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::{net::{TcpListener, TcpStream}, sync::broadcast};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Parser, Debug)]
struct Args {
    /// ex: http://127.0.0.1:8080
    #[arg(long)]
    master: String,

    /// adresse d'écoute TCP de ce node, ex 0.0.0.0:9001
    #[arg(long)]
    listen: String,

    /// topics à ouvrir au démarrage (tu peux répéter)
    #[arg(long)]
    open: Vec<String>,

    /// topics à souscrire au démarrage (tu peux répéter)
    #[arg(long)]
    sub: Vec<String>,
}

#[derive(Clone)]
struct NodeState {
    master: String,
    listen_addr: String, // "0.0.0.0:9001"
    topics: Arc<DashMap<String, broadcast::Sender<Vec<u8>>>>,
    http: reqwest::Client,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let st = NodeState {
        master: args.master.clone(),
        listen_addr: args.listen.clone(),
        topics: Arc::new(DashMap::new()),
        http: reqwest::Client::new(),
    };

    // Ouvrir des topics
    for t in &args.open {
        open_topic(&st, t).await?;
    }

    // Lancer serveur TCP du node
    let st_srv = st.clone();
    tokio::spawn(async move {
        if let Err(e) = run_tcp_server(st_srv).await {
            eprintln!("[node] tcp server error: {e:#}");
        }
    });

    // Souscrire à des topics
    for t in &args.sub {
        let st_sub = st.clone();
        let topic = t.clone();
        tokio::spawn(async move {
            if let Err(e) = subscribe_loop(st_sub, topic).await {
                eprintln!("[node] subscribe error: {e:#}");
            }
        });
    }

    println!("[node] running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn open_topic(st: &NodeState, topic: &str) -> anyhow::Result<()> {
    // broadcast channel
    let (tx, _rx) = broadcast::channel::<Vec<u8>>(1024);
    st.topics.insert(topic.to_string(), tx);

    // register auprès du master (GET)
    let url = format!(
        "{}/register?topic={}&addr={}",
        st.master,
        urlencoding::encode(topic),
        urlencoding::encode(&st.listen_addr)
    );

    let resp = st.http.get(url).send().await?.error_for_status()?;
    let _json: serde_json::Value = resp.json().await?;
    println!("[node] opened & registered topic '{topic}' at {}", st.listen_addr);
    Ok(())
}

async fn lookup_topic(st: &NodeState, topic: &str) -> anyhow::Result<Option<String>> {
    let url = format!(
        "{}/lookup?topic={}",
        st.master,
        urlencoding::encode(topic)
    );
    let resp = st.http.get(url).send().await?.error_for_status()?;
    let v: core::LookupResp = resp.json().await?;
    Ok(v.addr)
}

async fn run_tcp_server(st: NodeState) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&st.listen_addr)
        .await
        .with_context(|| format!("bind {}", st.listen_addr))?;

    println!("[node] tcp listening on {}", st.listen_addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        let st2 = st.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_peer(st2, stream).await {
                eprintln!("[node] peer {addr} error: {e:#}");
            }
        });
    }
}

async fn handle_peer(st: NodeState, stream: TcpStream) -> anyhow::Result<()> {
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

    // 1) Attend Subscribe
    let first = framed
        .next()
        .await
        .context("expected first frame")?
        .context("read first frame")?;

    let topic = match decode(&first) {
        WireMsg::Subscribe { topic } => topic,
        _ => anyhow::bail!("first msg must be Subscribe"),
    };

    let tx = st
        .topics
        .get(&topic)
        .map(|e| e.value().clone())
        .context(format!("unknown topic '{topic}'"))?;

    let mut rx = tx.subscribe();
    println!("[node] peer subscribed to '{topic}'");

    loop {
        tokio::select! {
            // a) message reçu du peer (Publish)
            maybe = framed.next() => {
                match maybe {
                    Some(Ok(bytes)) => {
                        match decode(&bytes) {
                            WireMsg::Publish{ payload } => {
                                // broadcast sur le topic
                                let _ = tx.send(payload);
                            }
                            WireMsg::Subscribe{..} => {
                                // on ignore pour rester simple
                            }
                        }
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                }
            }

            // b) message à pousser au peer
            msg = rx.recv() => {
                match msg {
                    Ok(payload) => {
                        let out = encode(&WireMsg::Publish{ payload });
                        framed.send(Bytes::from(out)).await?;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // le client n'a pas suivi -> on continue
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    Ok(())
}

/// Boucle de souscription: lookup -> connect -> subscribe -> print messages
async fn subscribe_loop(st: NodeState, topic: String) -> anyhow::Result<()> {
    loop {
        let Some(addr) = lookup_topic(&st, &topic).await? else {
            eprintln!("[node] topic '{topic}' not found, retry in 1s");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            continue;
        };

        println!("[node] subscribing to '{topic}' at {addr}");
        match TcpStream::connect(&addr).await {
            Ok(stream) => {
                if let Err(e) = run_subscription(stream, &topic).await {
                    eprintln!("[node] subscription ended: {e:#}");
                }
            }
            Err(e) => eprintln!("[node] connect failed: {e}"),
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

async fn run_subscription(stream: TcpStream, topic: &str) -> anyhow::Result<()> {
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

    // envoie Subscribe
    framed.send(Bytes::from(encode(&WireMsg::Subscribe{ topic: topic.to_string() }))).await?;

    // exemple: publie un message toutes les 200ms (tu peux remplacer par ton code)
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
    let mut n: u64 = 0;

    loop {
        tokio::select! {
            // lecture push
            maybe = framed.next() => {
                match maybe {
                    Some(Ok(bytes)) => {
                        if let WireMsg::Publish{ payload } = decode(&bytes) {
                            println!("[sub:{topic}] recv {} bytes", payload.len());
                        }
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => return Ok(()),
                }
            }

            // exemple d'écriture (publish)
            _ = interval.tick() => {
                n += 1;
                let payload = format!("hello #{n} from subscriber on '{topic}'").into_bytes();
                framed.send(Bytes::from(encode(&WireMsg::Publish{ payload }))).await?;
            }
        }
    }
}
