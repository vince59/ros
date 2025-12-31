use anyhow::{anyhow, Context};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, Mutex},
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicMode {
    ReadOnly,   // owner -> clients (broadcast), clients ne publient pas
    WriteOnly,  // clients -> owner (callback), pas de broadcast
    ReadWrite,  // owner <-> clients
}

pub type AsyncCallback =
    Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;

#[derive(Clone)]
pub struct TopicOpen {
    pub master: String,      // ex http://127.0.0.1:8080
    pub topic: String,       // ex "chat"
    pub listen_addr: String, // ex "0.0.0.0:9001"
    pub mode: TopicMode,
    pub on_incoming: Option<AsyncCallback>, // message reçu d'un client (Publish)
}

#[derive(Clone)]
pub struct TopicSubscribe {
    pub master: String,
    pub topic: String,
    pub on_message: Option<AsyncCallback>, // message reçu du broadcast
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LookupResp {
    pub topic: String,
    pub addr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResp {
    pub ok: bool,
}

// Protocole wire (1 connexion = 1 topic)
#[derive(Debug, Serialize, Deserialize)]
enum WireMsg {
    Subscribe { topic: String },
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

#[derive(Clone)]
pub struct Topic {
    inner: Arc<Inner>,
}

struct Inner {
    kind: Kind,
    mode: TopicMode,
    topic: String,
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
    pub async fn open(cfg: TopicOpen) -> anyhow::Result<Self> {
        let http = reqwest::Client::new();
        let (tx, _rx) = broadcast::channel::<Vec<u8>>(1024);

        // register master (GET)
        let reg_url = format!(
            "{}/register?topic={}&addr={}",
            cfg.master,
            urlencoding::encode(&cfg.topic),
            urlencoding::encode(&cfg.listen_addr)
        );
        http.get(reg_url).send().await?.error_for_status()?;

        let inner = Arc::new(Inner {
            kind: Kind::Owner {
                listen_addr: cfg.listen_addr.clone(),
                master: cfg.master.clone(),
            },
            mode: cfg.mode,
            topic: cfg.topic.clone(),
            http,
            tx,
            writer: Mutex::new(None),
            on_incoming_owner: cfg.on_incoming,
            on_message_sub: None,
        });

        // démarrage serveur TCP du topic
        let inner2 = inner.clone();
        tokio::spawn(async move {
            if let Err(e) = run_owner_server(inner2).await {
                eprintln!("[topic-owner] server error: {e:#}");
            }
        });

        Ok(Self { inner })
    }

    /// Souscrit à un topic: lookup master -> connect -> démarre reader/writer tasks.
    pub async fn subscribe(cfg: TopicSubscribe) -> anyhow::Result<Self> {
        let http = reqwest::Client::new();
        let (tx, _rx) = broadcast::channel::<Vec<u8>>(1024);

        let inner = Arc::new(Inner {
            kind: Kind::Subscriber { master: cfg.master.clone() },
            // le mode exact (RO/WO/RW) n'est pas connu si master ne le fournit pas.
            // On applique une politique simple: subscriber peut tenter publish (serveur décidera),
            // et recevra broadcast si le serveur en envoie.
            mode: TopicMode::ReadWrite,
            topic: cfg.topic.clone(),
            http,
            tx,
            writer: Mutex::new(None),
            on_incoming_owner: None,
            on_message_sub: cfg.on_message,
        });

        connect_subscriber(inner.clone()).await?;
        Ok(Self { inner })
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
        txw.send(payload).await.map_err(|_| TopicError::TopicNotFound)?;
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

    println!("[topic-owner] listening on {listen_addr} for topic '{}'", inner.topic);

    loop {
        let (stream, addr) = listener.accept().await?;
        let inner2 = inner.clone();
        tokio::spawn(async move {
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

    if requested != inner.topic {
        anyhow::bail!("unknown topic '{requested}' (this server serves '{}')", inner.topic);
    }

    // Si les clients peuvent lire -> on s'abonne au broadcast
    let mut rx_opt = if matches!(inner.mode, TopicMode::ReadOnly | TopicMode::ReadWrite) {
        Some(inner.tx.subscribe())
    } else {
        None
    };

    loop {
        tokio::select! {
            // A) lecture client->owner
            maybe = framed.next() => {
                match maybe {
                    Some(Ok(bytes)) => {
                        match decode(&bytes) {
                            WireMsg::Publish { payload } => {
                                // client a-t-il le droit d'écrire ?
                                if !matches!(inner.mode, TopicMode::WriteOnly | TopicMode::ReadWrite) {
                                    // interdit: ignore (ou tu peux couper la connexion)
                                    continue;
                                }

                                // callback owner
                                if let Some(cb) = &inner.on_incoming_owner {
                                    (cb)(payload.clone()).await;
                                }

                                // broadcast seulement si clients peuvent lire (RW)
                                if matches!(inner.mode, TopicMode::ReadWrite) {
                                    let _ = inner.tx.send(payload);
                                }
                            }
                            WireMsg::Subscribe { .. } => {}
                        }
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                }
            }

            // B) push owner->client (broadcast)
            msg = async {
                match &mut rx_opt {
                    Some(rx) => Some(rx.recv().await),
                    None => None,
                }
            } => {
                match msg {
                    Some(Ok(payload)) => {
                        let out = encode(&WireMsg::Publish { payload });
                        framed.send(Bytes::from(out)).await?;
                    }
                    Some(Err(broadcast::error::RecvError::Lagged(_))) => {}
                    Some(Err(broadcast::error::RecvError::Closed)) => break,
                    None => { /* WriteOnly => pas de push */ }
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
    let url = format!("{}/lookup?topic={}", master, urlencoding::encode(&inner.topic));
    let resp = inner.http.get(url).send().await?.error_for_status()?;
    let info: LookupResp = resp.json().await?;

    let Some(addr) = info.addr else {
        return Err(TopicError::TopicNotFound.into());
    };

    let stream = TcpStream::connect(&addr).await
        .with_context(|| format!("connect to {addr}"))?;
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

    // send Subscribe
    framed.send(Bytes::from(encode(&WireMsg::Subscribe { topic: inner.topic.clone() }))).await?;

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
