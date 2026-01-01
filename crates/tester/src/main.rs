use core::{Message, Topic, TopicName, TopicSubscribe, LogLevel, LogMode, encode_msg};
use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    master: String, // master ip and port ex: http://127.0.0.1:8080

    #[arg(long)]
    port: String, // listening port ex: 9001
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("[tester] V1");
    let args = Args::parse();

    let sub = Topic::subscribe_with_retry(TopicSubscribe {
        master: args.master,
        name: TopicName::Logger,
        on_message: None,
    }, std::time::Duration::from_millis(250), std::time::Duration::from_secs(5)).await?;

    // 3) Publie périodiquement (tu recevras ces messages via la souscription)
    let mut n: u64 = 0;
    loop {
        n += 1;
        print!("[tester] publishing ping #{n}\n");
        let msg = Message::Log { level: LogLevel::Info, content: format!("ping #{n}"), mode: LogMode::Console };
        let _ = sub.publish(encode_msg(&msg)).await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}
/* Ancienne version pour référence
async fn main() -> anyhow::Result<()> {
    // Master déjà lancé ailleurs
    let master = "http://127.0.0.1:8080".to_string();

    // Exemple fixe
    let listen_addr = "127.0.0.1:9001".to_string();

    // Callback côté owner: s'active quand un client publie vers le topic
    let on_incoming: AsyncCallback = Arc::new(|payload: Vec<u8>| {
        Box::pin(async move {
            println!(
                "[owner] incoming: {}",
                String::from_utf8_lossy(&payload)
            );
        })
    });

    // 1) Ouvre le topic (démarre le serveur TCP + register auprès du master)
    let owner = Topic::open(TopicParam {
        master: master.clone(),
        name: TopicName::Test,
        listen_addr,
        mode: TopicMode::ReadWrite,
        on_incoming: Some(on_incoming),
    })
    .await?;

    // Callback côté subscriber: s'active quand on reçoit un broadcast du topic
    let on_message: AsyncCallback = Arc::new(|payload: Vec<u8>| {
        Box::pin(async move {
            println!(
                "[sub] broadcast: {}",
                String::from_utf8_lossy(&payload)
            );
        })
    });

    // 2) Souscrit au même topic (lookup master + connect TCP)
    let sub = Topic::subscribe(TopicSubscribe {
        master,
        name: TopicName::Test,
        on_message: Some(on_message),
    })
    .await?;

    // 3) Publie périodiquement (tu recevras ces messages via la souscription)
    let mut n: u64 = 0;
    loop {
        n += 1;

        // Publish côté "client"
        let _ = sub.publish(format!("ping #{n}").into_bytes()).await;

        // Publish côté "owner" (optionnel: montre l'autre sens)
        let _ = owner.publish(format!("owner says #{n}").into_bytes()).await;

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}
*/