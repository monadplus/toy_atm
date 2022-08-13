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

fn main() -> any::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("error")).init();

    let csv_file_path = match env::args().nth(1) {
        None => Err(anyhow!("Expecting one argument")),
        Some(file_path) => Ok(file_path),
    }?;

    let (engine_tx, mut engine) = Engine::new();

    let tx_provider = TxProvider::new();
    tx_provider.with_csv(&csv_file_path, |tx| engine_tx.send_tx(tx))?;
    engine_tx.finish();

    let report = engine.start()?;
    print!("{}", report.pretty_csv());

    Ok(())
}
