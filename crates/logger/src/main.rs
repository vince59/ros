use clap::Parser;
use std::sync::Arc;
use core::{AsyncCallback, Topic, TopicMode, TopicParam, TopicName};

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

    // Callback appelée quand un client envoie un message (Publish) au topic
    let on_incoming: AsyncCallback = Arc::new(|payload: Vec<u8>| {
        Box::pin(async move {
            println!("[owner] incoming {} bytes: {:?}", payload.len(), payload);
        })
    });

    // Ouvre le topic et démarre le serveur TCP automatiquement
    let cfg= TopicParam::new(
        args.master,
        TopicName::Logs,
        args.port,
        TopicMode::WriteOnly, 
        Some(on_incoming),
    );

    let _logger = Topic::open(cfg).await?;
    println!("[logger] running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    Ok(())
}
