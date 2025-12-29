use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireMsg {
    /// 1er message envoyé par un pair après connexion TCP
    Subscribe { topic: String },

    /// Publier sur le topic de cette connexion
    Publish { payload: Vec<u8> },
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
