use clap::Parser;
use std::sync::Arc;
use core::{AsyncCallback, Topic, TopicMode, TopicOpen, TopicName};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    master: String, // master ip and port ex: http://127.0.0.1:8080

    #[arg(long)]
    port: String, // listening port ex: 9001

}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("[logger] V1");
    let args = Args::parse();
     let master = "http://127.0.0.1:8080".to_string();
    let topic_name = "chat".to_string();
    let listen_addr = "0.0.0.0:9001".to_string();

    // Callback appelée quand un client envoie un message (Publish) au topic
    let on_incoming: AsyncCallback = Arc::new(|payload: Vec<u8>| {
        Box::pin(async move {
            println!("[owner] incoming {} bytes: {:?}", payload.len(), payload);
        })
    });

    // Ouvre le topic et démarre le serveur TCP automatiquement
    let topic = Topic::open(TopicOpen {
        master,
        name: TopicName::Logs,
        listen_addr,
        mode: TopicMode::ReadWrite, // ReadOnly / WriteOnly / ReadWrite
        on_incoming: Some(on_incoming),
    })
    .await?;

    // Exemple : publier périodiquement depuis l’owner (si mode le permet)
    let mut tick: u64 = 0;
    loop {
        tick += 1;
        let msg = format!("hello from owner #{tick}").into_bytes();
        // En ReadWrite/ReadOnly -> broadcast, en WriteOnly -> PublishForbidden
        let _ = topic.publish(msg).await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    println!("[logger] running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    Ok(())
}
