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
    println!("[logger] V1");
    let args = Args::parse();
    let st = core::NodeState::new(args.master, args.port);
    core::open_topic(&st, core::Topic::Logs).await?;
    println!("[logger] running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    Ok(())
}
