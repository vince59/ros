use anyhow::{Context, anyhow};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex, broadcast, mpsc},
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Copy)]
pub enum TopicName {
    Logger,
    Tester,
    Undefined,
}

impl fmt::Display for TopicName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TopicName::Logger => write!(f, "Logger"),
            TopicName::Tester => write!(f, "Tester"),
            TopicName::Undefined => write!(f, "Undefined"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Copy)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogMode {
    Console,               // log to console
    File { name: String }, // log to file
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Log {
        level: LogLevel,
        content: String,
        mode: LogMode,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicMode {
    ReadOnly,  // owner -> clients (broadcast), clients ne publient pas
    WriteOnly, // clients -> owner (callback), pas de broadcast
    ReadWrite, // owner <-> clients
}

pub type AsyncCallback =
    Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;

#[derive(Clone)]
pub struct TopicParam {
    pub master: String, // ex http://127.0.0.1:8080
    pub name: TopicName,
    pub listen_addr: String, // ex "0.0.0.0:9001"
    pub public_addr: String, // ex "127.0.0.1:9001" ou "192.168.1.10:9001"
    pub mode: TopicMode,
    pub on_incoming: Option<AsyncCallback>, // message reçu d'un client (Publish)
}

impl TopicParam {
    pub fn new(
        master: String,
        name: TopicName,
        port: String,
        mode: TopicMode,
        on_incoming: Option<AsyncCallback>,
    ) -> Self {
        Self {
            master,
            name,
            listen_addr: format!("0.0.0.0:{}", port),
            public_addr: format!("127.0.0.1:{}", port),
            mode,
            on_incoming,
        }
    }
}

#[derive(Clone)]
pub struct TopicSubscribe {
    pub master: String,
    pub name: TopicName,
    pub on_message: Option<AsyncCallback>, // message reçu du broadcast
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LookupResp {
    pub name: TopicName,
    pub addr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResp {
    pub ok: bool,
}

// Protocole wire (1 connexion = 1 topic)
#[derive(Debug, Serialize, Deserialize)]
enum WireMsg {
    Subscribe { topic: TopicName },
    Publish { payload: Vec<u8> },
    // Optionnel: tu peux ajouter Ack / Error si tu veux
}

fn encode(msg: &WireMsg) -> Vec<u8> {
    // bincode v1 (stable)
    bincode::serialize(msg).expect("bincode serialize")
}
fn decode(bytes: &[u8]) -> WireMsg {
    bincode::deserialize(bytes).expect("bincode deserialize")
}

pub fn encode_msg(msg: &Message) -> Vec<u8> {
    bincode::serialize(msg).expect("bincode serialize Message")
}

pub fn decode_msg(bytes: &[u8]) -> Message {
    bincode::deserialize(bytes).expect("bincode deserialize Message")
}

#[derive(Clone)]
pub struct Topic {
    inner: Arc<Inner>,
}

struct Inner {
    kind: Kind,
    mode: TopicMode,
    name: TopicName,
    http: reqwest::Client,

    // broadcast (sert pour owner -> clients, et aussi pour subscriber -> callbacks)
    tx: broadcast::Sender<Vec<u8>>,

    // pour publish réseau (subscriber ou client-connection), on passe par un mpsc -> writer task
    writer: Mutex<Option<mpsc::Sender<Vec<u8>>>>,

    // callbacks
    on_incoming_owner: Option<AsyncCallback>, // client->owner
    on_message_sub: Option<AsyncCallback>,    // broadcast reçu côté subscriber
}

enum Kind {
    Owner { listen_addr: String, master: String },
    Subscriber { master: String },
}

#[derive(thiserror::Error, Debug)]
pub enum TopicError {
    #[error("publish not allowed by mode")]
    PublishForbidden,
    #[error("topic not found on master")]
    TopicNotFound,
}

impl Topic {
    /// Ouvre un topic côté owner: démarre le serveur TCP et register sur le master.
    pub async fn open(cfg: TopicParam) -> anyhow::Result<Self> {
        let http = reqwest::Client::new();
        let (tx, _rx) = broadcast::channel::<Vec<u8>>(1024);

        // register master (GET)
        let reg_url = format!(
            "{}/register?topic={}&addr={}",
            cfg.master,
            urlencoding::encode(&cfg.name.to_string()),
            urlencoding::encode(&cfg.public_addr)
        );

        println!("[{}] registering topic at master...", cfg.name);
        http.get(&reg_url)
            .send()
            .await
            .context(format!("[{}] failed to send register request", cfg.name))?
            .error_for_status()
            .context(format!("[{}] master rejected topic registration", cfg.name))?;

        let inner = Arc::new(Inner {
            kind: Kind::Owner {
                listen_addr: cfg.listen_addr.clone(),
                master: cfg.master.clone(),
            },
            mode: cfg.mode,
            name: cfg.name,
            http,
            tx,
            writer: Mutex::new(None),
            on_incoming_owner: cfg.on_incoming,
            on_message_sub: None,
        });

        // démarrage serveur TCP du topic
        println!("[{}] starting topic server...", cfg.name);
        let inner2 = inner.clone();
        let name = inner.name.to_string();
        tokio::spawn(async move {
            if let Err(e) = run_owner_server(inner2).await {
                eprintln!("[{}] server error: {e:#}", name);
            }
        });

        Ok(Self { inner })
    }

    /// Souscrit à un topic: lookup master -> connect -> démarre reader/writer tasks.
    pub async fn subscribe(cfg: TopicSubscribe) -> anyhow::Result<Self> {
        let http = reqwest::Client::new();
        let (tx, _rx) = broadcast::channel::<Vec<u8>>(1024);

        let inner = Arc::new(Inner {
            kind: Kind::Subscriber {
                master: cfg.master.clone(),
            },
            mode: TopicMode::ReadWrite,
            name: cfg.name,
            http,
            tx,
            writer: Mutex::new(None),
            on_incoming_owner: None,
            on_message_sub: cfg.on_message,
        });

        connect_subscriber(inner.clone()).await?;
        Ok(Self { inner })
    }

    pub async fn subscribe_with_retry(
        cfg: TopicSubscribe,
        initial: Duration,
        max: Duration,
    ) -> anyhow::Result<Self> {
        let http = reqwest::Client::new();
        let (tx, _rx) = tokio::sync::broadcast::channel::<Vec<u8>>(1024);

        let inner = Arc::new(Inner {
            kind: Kind::Subscriber {
                master: cfg.master.clone(),
            },
            mode: TopicMode::ReadWrite,
            name: cfg.name,
            http,
            tx,
            writer: tokio::sync::Mutex::new(None),
            on_incoming_owner: None,
            on_message_sub: cfg.on_message,
        });

        let mut delay = initial;

        loop {
            println!("[topic-sub] trying to connect to topic '{}'...", inner.name);
            match connect_subscriber(inner.clone()).await {
                Ok(()) => {
                    println!("test");
                    return Ok(Self { inner });
                }
                Err(e) => {
                    eprintln!("{}", e);
                    let is_not_found = e
                        .downcast_ref::<TopicError>()
                        .is_some_and(|te| matches!(te, TopicError::TopicNotFound));

                    if !is_not_found {
                        return Err(e);
                    }

                    tokio::time::sleep(delay).await;
                    delay = std::cmp::min(delay * 2, max);
                }
            }
        }
    }

    /// Publie un message (owner ou subscriber). Les permissions sont appliquées côté local
    /// (et le serveur fera foi dans tous les cas).
    pub async fn publish(&self, payload: Vec<u8>) -> Result<(), TopicError> {
        // Owner publish local -> broadcast vers clients (si mode le permet)
        if matches!(self.inner.kind, Kind::Owner { .. }) {
            if !matches!(self.inner.mode, TopicMode::ReadOnly | TopicMode::ReadWrite) {
                return Err(TopicError::PublishForbidden);
            }
            let _ = self.inner.tx.send(payload);
            return Ok(());
        }

        // Subscriber publish réseau (si on a un writer)
        let writer = self.inner.writer.lock().await.clone();
        let Some(txw) = writer else {
            // pas connecté (ou reconnexion à faire)
            return Err(TopicError::TopicNotFound);
        };

        // On ne bloque pas : on pousse dans la queue du writer task
        txw.send(payload)
            .await
            .map_err(|_| TopicError::TopicNotFound)?;
        Ok(())
    }
}

// =============== impl owner server ===============

async fn run_owner_server(inner: Arc<Inner>) -> anyhow::Result<()> {
    let listen_addr = match &inner.kind {
        Kind::Owner { listen_addr, .. } => listen_addr.clone(),
        _ => unreachable!(),
    };

    let listener = TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("bind {listen_addr}"))?;

    println!("[{}] listening on {}", inner.name, listen_addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        let inner2 = inner.clone();
        println!("test2");
        tokio::spawn(async move {
            println!("test3");
            if let Err(e) = handle_owner_peer(inner2, stream).await {
                eprintln!("[topic-owner] peer {addr} error: {e:#}");
            }
        });
    }
}

async fn handle_owner_peer(inner: Arc<Inner>, stream: TcpStream) -> anyhow::Result<()> {
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

    // Attend Subscribe
    let first = framed
        .next()
        .await
        .ok_or_else(|| anyhow!("expected first frame"))??;

    let requested = match decode(&first) {
        WireMsg::Subscribe { topic } => topic,
        _ => anyhow::bail!("first msg must be Subscribe"),
    };

    if requested != inner.name {
        anyhow::bail!(
            "unknown topic '{requested}' (this server serves '{}')",
            inner.name
        );
    }

    // WriteOnly
    if inner.mode == TopicMode::WriteOnly {
        while let Some(item) = framed.next().await {
            let bytes = item?;
            match decode(&bytes) {
                WireMsg::Publish { payload } => {
                    // callback owner
                    if let Some(cb) = &inner.on_incoming_owner {
                        (cb)(payload).await;
                    }
                }
                WireMsg::Subscribe { .. } => {}
            }
        }
        return Ok(());
    }

    // ReadOnly / ReadWrite
    let tx = inner.tx.clone();
    let mut rx = tx.subscribe();

    loop {
        tokio::select! {
            maybe = framed.next() => {
                match maybe {
                    Some(Ok(bytes)) => {
                        if let WireMsg::Publish { payload } = decode(&bytes) {
                            // ReadWrite: callback + broadcast
                            if let Some(cb) = &inner.on_incoming_owner {
                                (cb)(payload.clone()).await;
                            }
                            if inner.mode == TopicMode::ReadWrite {
                                let _ = inner.tx.send(payload);
                            }
                        }
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                }
            }

            msg = rx.recv() => {
                match msg {
                    Ok(payload) => {
                        let out = encode(&WireMsg::Publish { payload });
                        framed.send(Bytes::from(out)).await?;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    Ok(())
}

// =============== impl subscriber ===============

async fn connect_subscriber(inner: Arc<Inner>) -> anyhow::Result<()> {
    let master = match &inner.kind {
        Kind::Subscriber { master } => master.clone(),
        _ => unreachable!(),
    };

    // lookup
    let url = format!(
        "{}/lookup?topic={}",
        master,
        urlencoding::encode(&inner.name.to_string())
    );
    let resp = inner.http.get(url).send().await?.error_for_status()?;
    let info: LookupResp = resp.json().await?;

    let Some(addr) = info.addr else {
        return Err(TopicError::TopicNotFound.into());
    };

    let stream = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("connect to {addr}"))?;
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

    // send Subscribe
    framed
        .send(Bytes::from(encode(&WireMsg::Subscribe {
            topic: inner.name,
        })))
        .await?;

    // Writer task via mpsc
    let (txw, mut rxw) = mpsc::channel::<Vec<u8>>(1024);
    *inner.writer.lock().await = Some(txw);

    // Split: on garde framed entier mais on fait 2 tasks avec Mutex serait lourd.
    // Ici on fait simple: on spawn un task dédié qui possède framed.
    let inner2 = inner.clone();
    tokio::spawn(async move {
        let mut framed = framed;

        loop {
            tokio::select! {
                // lecture serveur->subscriber
                maybe = framed.next() => {
                    match maybe {
                        Some(Ok(bytes)) => {
                            if let WireMsg::Publish { payload } = decode(&bytes) {
                                // broadcast local
                                let _ = inner2.tx.send(payload.clone());

                                // callback subscriber
                                if let Some(cb) = &inner2.on_message_sub {
                                    (cb)(payload).await;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            eprintln!("[topic-sub] read error: {e}");
                            break;
                        }
                        None => break,
                    }
                }

                // écriture subscriber->serveur
                some = rxw.recv() => {
                    match some {
                        Some(payload) => {
                            let out = encode(&WireMsg::Publish { payload });
                            if framed.send(Bytes::from(out)).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        // plus de writer dispo
        *inner2.writer.lock().await = None;
        eprintln!("[topic-sub] disconnected");
    });

    Ok(())
}
