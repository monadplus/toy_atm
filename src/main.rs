//! Toy ATM: emulates a simple transaction engine with the following semantics
//!
//! - `Deposit`: increases the funds of an account.
//! - `Withdrawal`: decreases the funds of an account. Ignored if there are not enough funds in the account.
//! - `Dispute`: holds a deposit transaction until it is resolved or chargedback. Ignored if the transaction doesn't exist.
//! - `Resolve`: unlocks a dispute retuning the held funds to the account. Ignored if the transaction doesn't exist or it is not in dispute.
//! - `Chargeback`: unlocks a dispute decreasing the funds from the account and freezes the account. Ignored if the transaction doesn't exist or it is not in dispute.
//!
//! ## Usage
//!
//! ```ignore
//! $ RUST_LOG=info RUST_BACKTRACE=1 cargo run -- <file.csv>
//! ```
use anyhow as any;
use anyhow::anyhow;
use env_logger::Env;
use std::env;
use toy_atm::engine::Engine;
use toy_atm::tx_provider::TxProvider;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> any::Result<()> {
    // Set up the logging
    env_logger::Builder::from_env(Env::default().default_filter_or("error")).init();

    // Parse the CSV
    let csv_file_path = match env::args().nth(1) {
        None => Err(anyhow!("Expecting one argument")),
        Some(file_path) => Ok(file_path),
    }?;

    // Start the engine to process asynchronous transactions on the background.
    let mut engine = Engine::run(4 /*arbitrary*/).await;

    let tx_provider = TxProvider::new();
    // Streamly process all transactions from the CSV
    tx_provider
        .with_csv(&csv_file_path, |trans| engine.send_trans(trans))
        .await?;

    // Await until all transactions have been processed
    engine.finish().await;

    let report = engine.report().await;
    print!("{}", report.pretty_csv());

    Ok(())
}
