//! This module contains the transaction engine.

#![allow(clippy::new_without_default)]
use crate::{
    report::{AccountSummary, Report},
    tx::{ClientID, Tx, TxID},
};
use anyhow as any;
use anyhow::anyhow;
use futures::future::join_all;
use log::{error, info, trace, warn};
use rust_decimal::Decimal;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tokio::{
    sync::{mpsc, oneshot, RwLock},
    task,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Account {
    pub available: Decimal,
    pub held: Decimal,
    pub deposits: HashMap<TxID, Tx>,
    pub locked: bool,
}

impl Account {
    pub fn new() -> Self {
        Account {
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            deposits: HashMap::new(),
            locked: false,
        }
    }

    pub fn process_tx(&mut self, tx: Tx) {
        if self.locked {
            // Locked accounts ignore transactions
            warn!("Locked account ignored transaction {tx:?}");
        } else {
            let to_insert: Option<TxID> = match tx {
                Tx::Deposit { tx_id, amount, .. } => {
                    self.available += amount;
                    Some(tx_id)
                }
                Tx::Withdrawal { tx_id, amount, .. } => {
                    if self.available < amount {
                        warn!("Withdrawal {tx_id:?} not enough funds (amount: {amount}, available: {})", self.available);
                    } else {
                        self.available -= amount;
                    }
                    None
                }
                Tx::Dispute {
                    tx_id_reference, ..
                } => {
                    match self.deposits.get_mut(&tx_id_reference) {
                        None => {
                            warn!("Disputing {tx_id_reference:?} which does not exist");
                        }
                        Some(tx) => match tx {
                            Tx::Deposit {
                                amount, in_dispute, ..
                            } => {
                                if *in_dispute {
                                    warn!("Can't dispute {tx_id_reference:?} more than once");
                                } else {
                                    *in_dispute = true;
                                    self.available -= *amount;
                                    self.held += *amount;
                                }
                            }
                            _ => {
                                warn!("Disputing {tx_id_reference:?} which is not a deposit");
                            }
                        },
                    }
                    None
                }
                Tx::Resolve {
                    tx_id_reference, ..
                } => {
                    match self.deposits.get_mut(&tx_id_reference) {
                        None => {
                            warn!("Resolving {tx_id_reference:?} which does not exist");
                        }
                        Some(tx) => match tx {
                            Tx::Deposit {
                                amount, in_dispute, ..
                            } => {
                                if *in_dispute {
                                    *in_dispute = false;
                                    self.available += *amount;
                                    self.held -= *amount;
                                } else {
                                    warn!("Resolving {tx_id_reference:?}, but not in dispute");
                                }
                            }
                            _ => {
                                warn!("Resolving {tx_id_reference:?} which is not a deposit");
                            }
                        },
                    }
                    None
                }
                Tx::Chargeback {
                    tx_id_reference, ..
                } => {
                    match self.deposits.get_mut(&tx_id_reference) {
                        None => {
                            warn!("Chargingback {tx_id_reference:?} which does not exist");
                        }
                        Some(tx) => match tx {
                            Tx::Deposit {
                                amount, in_dispute, ..
                            } => {
                                if *in_dispute {
                                    *in_dispute = false;
                                    self.locked = true;
                                    self.held -= *amount;
                                } else {
                                    warn!("Chargingback {tx_id_reference:?}, but not in dispute");
                                }
                            }
                            _ => {
                                warn!("Chargingback {tx_id_reference:?} which is not a deposit");
                            }
                        },
                    }
                    None
                }
            };

            if let Some(tx_id) = to_insert {
                trace!("Inserting new deposit {tx_id:?}");
                if let Some(_old_tx) = self.deposits.insert(tx_id, tx) {
                    // We have assumed that all transactions' id are global unique.
                    // We do not crash when this invariant is broken because it seems
                    // from the requirements that the sources may contain invalid inputs.
                    error!("Unexpected repeated transaction id {tx_id:?}");
                }
            }
        }
    }

    /// Generates a summary of the account.
    ///
    /// Amounts are rounded up to 4 decimals using Bankers rounding.
    pub fn summary(&self, client_id: ClientID) -> AccountSummary {
        AccountSummary {
            client_id,
            available: self.available.round_dp(4),
            held: self.held.round_dp(4),
            total: self.available.round_dp(4) + self.held.round_dp(4),
            locked: self.locked,
        }
    }
}

// TODO
// We pay a little overhead using a sync primitive when we only need
// access to an exclusive reference, but `Cell` cannot be used because
// it is not `Send` (although we know it is safe to be sent to another thread)

type Accounts = Arc<RwLock<HashMap<WorkerID, AccountsShard>>>;

// TODO Use https://docs.rs/dashmap/5.3.4/dashmap/ instead of Mutex<HashMap<..>>
type AccountsShard = Arc<Mutex<HashMap<ClientID, Account>>>;

/// The transaction engine.
pub struct Engine {
    accounts: Accounts,
    trans_tx: Option<mpsc::Sender<Tx>>,
    shut_rx: Option<oneshot::Receiver<()>>,
}

impl Engine {
    pub async fn run(n_workers: u8) -> Engine {
        let (trans_tx, trans_rx) = mpsc::channel(1000);
        let (shut_tx, shut_rx) = oneshot::channel();
        let accounts: Accounts = Arc::new(RwLock::new(HashMap::new()));

        // Spawn master
        // Master will spawn workers
        let accounts_ptr = Arc::clone(&accounts);
        task::spawn(async move {
            Self::master_daemon(n_workers, accounts_ptr, trans_rx, shut_tx).await
        });

        Engine {
            accounts,
            trans_tx: Some(trans_tx),
            shut_rx: Some(shut_rx),
        }
    }

    pub async fn send_trans(&self, trans: Tx) -> any::Result<()> {
        match self.trans_tx {
            None => Err(anyhow!("Sender dropped")),
            Some(ref tx) => Ok(tx.send(trans).await?),
        }
    }

    /// Stops the engine.
    /// This task will wait that all workers have finished processing transactions.
    pub async fn finish(mut self) -> Report {
        // Drops the sender to finish the master
        self.trans_tx.take();

        // Wait until all transactions are processed
        if let Some(shut_tx) = self.shut_rx.take() {
            if shut_tx.await.is_err() {
                error!("shut_rx dropped before receiving the stop signal");
            }
        }

        self.report().await
    }

    /// Generates a [Report] from the current state of the accounts.
    async fn report(&self) -> Report {
        let accounts: HashMap<ClientID, Account> = {
            let mut accounts = HashMap::new();
            // Locking here is fine since the processing has already been finished.
            let accounts_rlock = self.accounts.read().await;
            for (_worker_id, shard) in accounts_rlock.iter() {
                let shard_lock = shard.lock().await;
                for (client_id, account) in shard_lock.iter() {
                    accounts.insert(*client_id, account.clone());
                }
            }
            accounts
        };

        Report::new(&accounts)
    }

    async fn master_daemon(
        n_workers: u8,
        accounts: Accounts,
        mut mailbox: mpsc::Receiver<Tx>,
        shutdown_tx: oneshot::Sender<()>,
    ) {
        let mut workers: Vec<Worker> = vec![];
        let mut clients_map: HashMap<ClientID, WorkerID> = HashMap::new();
        let mut workers_tx: HashMap<WorkerID, mpsc::UnboundedSender<Tx>> = HashMap::new();

        // Spawn workers
        for i in 0..n_workers {
            let worker_id = WorkerID(i);
            let (trans_tx, trans_rx) = mpsc::unbounded_channel();
            let account_shard = Arc::new(Mutex::new(HashMap::new()));
            let worker = Worker::run(worker_id, account_shard.clone(), trans_rx).await;

            // Create the shard
            accounts.write().await.insert(worker_id, account_shard);
            // Fill auxiliary data structures
            workers_tx.insert(worker_id, trans_tx);
            workers.push(worker);
        }

        // Forward incoming transactions until the producer is dropped.
        let mut n = 0u64;
        while let Some(tx) = mailbox.recv().await {
            trace!("Master received {tx:?}");
            let client_id = tx.client_id();
            let worker_id = clients_map.get(&client_id).cloned().unwrap_or_else(|| {
                let worker_id = WorkerID((n % (n_workers as u64)) as u8);
                clients_map.insert(client_id, worker_id);
                info!("Client {client_id:?} assigned to worker {worker_id:?}");
                n += 1;
                worker_id
            });
            if workers_tx.get(&worker_id).unwrap().send(tx).is_err() {
                error!("Worker {worker_id:?} receiver has been dropped before finishing");
            }
        }

        // Stop all workers and wait for them to finish
        drop(workers_tx);
        join_all(workers.into_iter().map(Worker::finish).collect::<Vec<_>>()).await;

        // Send shutdown signal
        if shutdown_tx.send(()).is_err() {
            error!("shut_rx dropped before sending the stop signal")
        };
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerID(u8);

pub struct Worker {
    id: WorkerID,
    shutdown_rx: oneshot::Receiver<()>,
}

impl Worker {
    /// Starts a new worker processing in the background.
    pub async fn run(
        id: WorkerID,
        accounts: AccountsShard,
        trans_rx: mpsc::UnboundedReceiver<Tx>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        task::spawn(async move { Self::worker_daemon(id, accounts, trans_rx, shutdown_tx).await });

        Worker { id, shutdown_rx }
    }

    /// Process all pending transactions before returning.
    pub async fn finish(self) {
        if self.shutdown_rx.await.is_err() {
            error!("Worker shutdown_tx dropped before used")
        };
        info!("Worker {:?} finished", self.id)
    }

    /// Background task that:
    /// - Picks a client with pending transactions to process
    /// - and processes all transactions from that client
    async fn worker_daemon(
        id: WorkerID,
        accounts: AccountsShard,
        mut trans_rx: mpsc::UnboundedReceiver<Tx>,
        shutdown_tx: oneshot::Sender<()>,
    ) {
        while let Some(tx) = trans_rx.recv().await {
            let client_id = tx.client_id();
            // This is locking the mutex for a long time but it is totally OK because
            // we are the only ones accessing it.
            let mut accounts_lock = accounts.lock().await;
            match accounts_lock.get_mut(&client_id) {
                None => {
                    let mut account = Account::new();
                    account.process_tx(tx);
                    accounts_lock.insert(client_id, account);
                }
                Some(account) => {
                    account.process_tx(tx);
                }
            }
        }

        if shutdown_tx.send(()).is_err() {
            error!("Worker {id:?} shutdown receiver dropped before a message was sent");
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use crate::{hash_map, tx};

    use super::*;

    #[test]
    fn account_process_tx_test() {
        let mut account = Account::new();

        // Deposit
        account.process_tx(Tx::Deposit {
            client_id: ClientID(1),
            tx_id: TxID(1),
            amount: dec!(100.1),
            in_dispute: false,
        });
        let expected = Account {
            available: dec!(100.1),
            held: dec!(0.0),
            deposits: hash_map! {
                TxID(1) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(1),
                    amount: dec!(100.1),
                    in_dispute: false,
                },
            },
            locked: false,
        };
        assert_eq!(expected, account);

        // Dispute Tx 1
        account.process_tx(Tx::Dispute {
            client_id: ClientID(1),
            tx_id_reference: TxID(1),
        });
        let expected = Account {
            available: dec!(0.0),
            held: dec!(100.1),
            deposits: hash_map! {
                TxID(1) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(1),
                    amount: dec!(100.1),
                    in_dispute: true,
                },
            },
            locked: false,
        };
        assert_eq!(expected, account);

        // Dispute Tx 1 again, nothing should happen
        account.process_tx(Tx::Dispute {
            client_id: ClientID(1),
            tx_id_reference: TxID(1),
        });
        let expected = Account {
            available: dec!(0.0),
            held: dec!(100.1),
            deposits: hash_map! {
                TxID(1) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(1),
                    amount: dec!(100.1),
                    in_dispute: true,
                },
            },
            locked: false,
        };
        assert_eq!(expected, account);

        // Resolve invalid reference
        account.process_tx(Tx::Resolve {
            client_id: ClientID(1),
            tx_id_reference: TxID(2),
        });
        let expected = Account {
            available: dec!(0.0),
            held: dec!(100.1),
            deposits: hash_map! {
                TxID(1) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(1),
                    amount: dec!(100.1),
                    in_dispute: true,
                },
            },
            locked: false,
        };
        assert_eq!(expected, account);

        // Resolve Tx 1
        account.process_tx(Tx::Resolve {
            client_id: ClientID(1),
            tx_id_reference: TxID(1),
        });
        let expected = Account {
            available: dec!(100.1),
            held: dec!(0.0),
            deposits: hash_map! {
                TxID(1) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(1),
                    amount: dec!(100.1),
                    in_dispute: false,
                },
            },
            locked: false,
        };
        assert_eq!(expected, account);

        // Another deposit
        account.process_tx(Tx::Deposit {
            client_id: ClientID(1),
            tx_id: TxID(2),
            amount: dec!(50.0),
            in_dispute: false,
        });
        let expected = Account {
            available: dec!(150.1),
            held: dec!(0.0),
            deposits: hash_map! {
                TxID(1) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(1),
                    amount: dec!(100.1),
                    in_dispute: false,
                },
                TxID(2) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(2),
                    amount: dec!(50.0),
                    in_dispute: false,
                },
            },
            locked: false,
        };
        assert_eq!(expected, account);

        // Withdrawal
        account.process_tx(Tx::Withdrawal {
            client_id: ClientID(1),
            tx_id: TxID(3),
            amount: dec!(20.0),
        });
        let expected = Account {
            available: dec!(130.1),
            held: dec!(0.0),
            deposits: hash_map! {
                TxID(1) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(1),
                    amount: dec!(100.1),
                    in_dispute: false,
                },
                TxID(2) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(2),
                    amount: dec!(50.0),
                    in_dispute: false,
                },
            },
            locked: false,
        };
        assert_eq!(expected, account);

        // Dispute Tx 2
        account.process_tx(Tx::Dispute {
            client_id: ClientID(1),
            tx_id_reference: TxID(2),
        });
        let expected = Account {
            available: dec!(80.1),
            held: dec!(50.0),
            deposits: hash_map! {
                TxID(1) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(1),
                    amount: dec!(100.1),
                    in_dispute: false,
                },
                TxID(2) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(2),
                    amount: dec!(50.0),
                    in_dispute: true,
                },
            },
            locked: false,
        };
        assert_eq!(expected, account);

        // Chargeback Tx 2
        account.process_tx(Tx::Chargeback {
            client_id: ClientID(1),
            tx_id_reference: TxID(2),
        });
        let expected = Account {
            available: dec!(80.1),
            held: dec!(0.0),
            deposits: hash_map! {
                TxID(1) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(1),
                    amount: dec!(100.1),
                    in_dispute: false,
                },
                TxID(2) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(2),
                    amount: dec!(50.0),
                    in_dispute: false,
                },
            },
            locked: true,
        };
        assert_eq!(expected, account);

        // Deposit on locked account should be ignored
        account.process_tx(Tx::Deposit {
            client_id: ClientID(1),
            tx_id: TxID(3),
            amount: dec!(30.0),
            in_dispute: false,
        });
        let expected = Account {
            available: dec!(80.1),
            held: dec!(0.0),
            deposits: hash_map! {
                TxID(1) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(1),
                    amount: dec!(100.1),
                    in_dispute: false,
                },
                TxID(2) => Tx::Deposit {
                    client_id: ClientID(1),
                    tx_id: TxID(2),
                    amount: dec!(50.0),
                    in_dispute: false,
                },
            },
            locked: true,
        };
        assert_eq!(expected, account);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn engine_test() {
        // Run: `$ cargo test engine_test -- --nocapture`
        //
        // Uncomment for debugging:
        // env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();

        let engine = Engine::run(4).await;
        // Client 1
        engine.send_trans(tx!(+,1,1,100)).await.unwrap();
        engine.send_trans(tx!(+,1,2,50)).await.unwrap();
        engine.send_trans(tx!(-,1,3,30)).await.unwrap();
        engine.send_trans(tx!(!, 1, 1)).await.unwrap();
        engine.send_trans(tx!(!, 1, 2)).await.unwrap();
        engine.send_trans(tx!(!, 1, 2)).await.unwrap();
        engine.send_trans(tx!(ok, 1, 1)).await.unwrap();
        engine.send_trans(tx!(ko, 1, 2)).await.unwrap();
        engine.send_trans(tx!(+,1,4,100)).await.unwrap();
        // Client 2
        engine.send_trans(tx!(+,2,5,100)).await.unwrap();
        engine.send_trans(tx!(+,2,6,100)).await.unwrap();
        engine.send_trans(tx!(-,2,7,30)).await.unwrap();
        engine.send_trans(tx!(!, 2, 5)).await.unwrap();

        let report = engine.finish().await;

        let expected = Report(vec![
            AccountSummary {
                client_id: ClientID(1),
                available: dec!(70),
                held: dec!(0.0000),
                total: dec!(70),
                locked: true,
            },
            AccountSummary {
                client_id: ClientID(2),
                available: dec!(70),
                held: dec!(100),
                total: dec!(170),
                locked: false,
            },
        ]);
        assert_eq!(report, expected)
    }
}
