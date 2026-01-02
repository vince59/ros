use core::{Message, Topic, TopicName, TopicSubscribe, LogLevel, LogMode};
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
    println!("[Tester] V1");
    let args = Args::parse();

    let sub = Topic::subscribe_with_retry(TopicSubscribe {
        master: args.master,
        name: TopicName::Logger,
        on_message: None,
    }, std::time::Duration::from_millis(250), std::time::Duration::from_secs(5)).await?;

    let mut n: u64 = 0;
    loop {
        n += 1;
        println!("[Tester] publishing ping #{n}");
        let msg = Message::Log { who: "Tester".to_string(), level: LogLevel::Info, content: format!("ping #{n}"), mode: LogMode::Console };
        let _ = sub.publish(msg).await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}
