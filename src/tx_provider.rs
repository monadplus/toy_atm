//! This module includes a generic provider for the transaction engine.
//!
//! Right now, there is only a CSV provider, but in the future this
//! module could include a provider from Kafka, RabbitMQ, Postgres, etc.

#![allow(clippy::new_without_default)]
use any::Context;
use anyhow as any;
use anyhow::anyhow;
use rust_decimal::Decimal;
use std::fmt::Debug;
use std::future::Future;
use std::path::Path;

use crate::tx::{ClientID, Tx, TxID};

pub struct TxProvider;

impl TxProvider {
    /// Instantiates a new 'TxProvider'.
    pub fn new() -> Self {
        Self
    }

    /// Given the filepath of a CSV, executes the given function for each line.
    pub async fn with_csv<P, F, Fut>(&self, path: P, process_tx: F) -> any::Result<()>
    where
        P: AsRef<Path> + Debug,
        F: Fn(Tx) -> Fut,
        Fut: Future<Output = any::Result<()>>,
    {
        use std::io::BufRead;

        let file = std::fs::File::open(path)?;
        let mut lines = std::io::BufReader::new(file).lines();
        lines.next(); // header: type,client,tx,amount
        for line in lines {
            let line: String = line?;
            let mut iter = line.split(',');
            let ty = iter.next().context("Missing `type` column")?;
            let client_id: ClientID = {
                let str = iter.next().context("Missing `client` column")?;
                ClientID(str.trim().parse::<u16>()?)
            };
            let tx_id: TxID = {
                let str = iter.next().context("Missing `tx` column")?;
                TxID(str.trim().parse::<u32>()?)
            };
            let amount = iter
                .next()
                .filter(|str| !str.is_empty())
                .map(|str| str.trim().parse::<Decimal>())
                .transpose()?;
            let trans: Tx = match ty.trim() {
                "deposit" => Tx::Deposit {
                    client_id,
                    tx_id,
                    amount: amount.context("Deposit missing `amount` column")?,
                    in_dispute: false,
                },
                "withdrawal" => Tx::Withdrawal {
                    client_id,
                    tx_id,
                    amount: amount.context("Withdrawal missing `amount` column")?,
                },
                "dispute" => Tx::Dispute {
                    client_id,
                    tx_id_reference: tx_id,
                },
                "resolve" => Tx::Resolve {
                    client_id,
                    tx_id_reference: tx_id,
                },
                "chargeback" => Tx::Chargeback {
                    client_id,
                    tx_id_reference: tx_id,
                },
                ty => anyhow::bail!("Not expected type {}", ty),
            };

            process_tx(trans).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::tx;

    use super::*;
    use std::{
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    fn get_resource(file_name: impl AsRef<Path>) -> PathBuf {
        let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        root.push("resources/test");
        root.push(file_name);
        root
    }

    async fn generic_csv_test(file_name: impl AsRef<Path>, expected: Vec<Tx>) {
        let csv_path = get_resource(file_name);
        let tx_provider = TxProvider::new();
        let accum: Arc<Mutex<Vec<Tx>>> = Arc::new(Mutex::new(vec![]));
        let accum_clone = Arc::clone(&accum);
        tx_provider
            .with_csv(csv_path, {
                |trans| {
                    let accum = Arc::clone(&accum_clone);
                    async move {
                        accum.lock().unwrap().push(trans);
                        Ok(())
                    }
                }
            })
            .await
            .unwrap();
        assert_eq!(*accum.lock().unwrap(), expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn from_csv_small_test() {
        let expected = vec![
            tx!(+, 1, 1, 100),
            tx!(-, 1, 2, 20),
            tx!(!, 1, 1),
            tx!(ok, 1, 1),
            tx!(+, 2, 3, 100),
            tx!(-, 2, 4, 20),
            tx!(!, 2, 3),
            tx!(ko, 2, 3),
        ];
        generic_csv_test("small.csv", expected).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn from_csv_small_trailing_test() {
        let expected = vec![
            tx!(+, 1, 1, 100),
            tx!(-, 1, 2, 20),
            tx!(!, 1, 1),
            tx!(ok, 1, 1),
            tx!(+, 2, 3, 100),
            tx!(-, 2, 4, 20),
            tx!(!, 2, 3),
            tx!(ko, 2, 3),
        ];
        generic_csv_test("small_trailing_commas.csv", expected).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn from_csv_small_ws_test() {
        let expected = vec![
            tx!(+, 1, 1, 100),
            tx!(-, 1, 2, 20),
            tx!(!, 1, 1),
            tx!(ok, 1, 1),
            tx!(+, 2, 3, 100),
            tx!(-, 2, 4, 20),
            tx!(!, 2, 3),
            tx!(ko, 2, 3),
        ];
        generic_csv_test("small_ws.csv", expected).await;
    }
}
