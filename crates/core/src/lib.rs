use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast;
use dashmap::DashMap;   
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireMsg {
    /// 1er message envoyé par un pair après connexion TCP
    Subscribe { topic: String },

    /// Publier sur le topic de cette connexion
    Publish { payload: Vec<u8> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Topic {
    Logs,
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Topic::Logs => write!(f, "Logs"),
        }
    }
}

pub fn encode(msg: &WireMsg) -> Vec<u8> {
    bincode::serialize(msg).expect("bincode serialize")
}

pub fn decode(bytes: &[u8]) -> WireMsg {
    bincode::deserialize(bytes).expect("bincode deserialize")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupResp {
    pub topic: String,
    pub addr: Option<String>, // ex "127.0.0.1:9001"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResp {
    pub ok: bool,
}

#[derive(Clone)]
pub struct NodeState {
    master: String,
    listen_addr: String, 
    topics: Arc<DashMap<String, broadcast::Sender<Vec<u8>>>>,
    http: reqwest::Client,
}

impl NodeState {
    pub fn new(master: String, port: String) -> Self {
        Self {
            master,
            listen_addr: format!("0.0.0.0:{}", port),
            topics: Arc::new(DashMap::new()),
            http: reqwest::Client::new(),
        }
    }
}

pub async fn open_topic(st: &NodeState, topic: Topic) -> anyhow::Result<()> {
    // broadcast channel
    let (tx, _rx) = broadcast::channel::<Vec<u8>>(1024);
    st.topics.insert(topic.to_string(), tx);

    // register auprès du master (GET)
    let url = format!(
        "{}/register?topic={}&addr={}",
        st.master,
        urlencoding::encode(topic.to_string().as_str()),
        urlencoding::encode(&st.listen_addr)
    );

    let resp = st.http.get(url).send().await?.error_for_status()?;
    let _json: serde_json::Value = resp.json().await?;
    println!("[master] new topic '{topic}' at {}", st.listen_addr);
    Ok(())
}