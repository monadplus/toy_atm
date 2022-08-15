//! This module contains the transaction engine.

#![allow(clippy::new_without_default)]
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use crate::{
    report::{AccountSummary, Report},
    tx::{ClientID, Tx, TxID},
};
use anyhow as any;
use anyhow::anyhow;
use log::{error, info, trace, warn};
use rust_decimal::Decimal;
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

pub struct AccountState {
    pub account: RwLock<Account>,
    pub pending_queue: RwLock<Vec<Tx>>,
}

impl AccountState {
    pub fn new() -> Self {
        Self {
            account: RwLock::new(Account::new()),
            pending_queue: RwLock::new(vec![]),
        }
    }
}

// TODO Use dashmap
type Accounts = Arc<RwLock<HashMap<ClientID, AccountState>>>;

type PendingClients = Arc<RwLock<BTreeSet<ClientID>>>;

/// The transaction engine.
pub struct Engine {
    accounts: Accounts,
    trans_tx: Option<mpsc::Sender<Tx>>,
    shut_rx: Option<oneshot::Receiver<()>>,
}

impl Engine {
    pub async fn run(n_workers: u8) -> Engine {
        let (trans_tx, trans_rx) = mpsc::channel(10);
        let (shut_tx, shut_rx) = oneshot::channel();
        let accounts: Accounts = Arc::new(RwLock::new(HashMap::new()));
        let pending_queue: PendingClients = Arc::new(RwLock::new(BTreeSet::new()));

        // Spawn broker
        let accounts_ptr = Arc::clone(&accounts);
        let pending_queue_ptr = Arc::clone(&pending_queue);
        task::spawn(async move {
            engine_logic::master_daemon(
                n_workers,
                accounts_ptr,
                pending_queue_ptr,
                trans_rx,
                shut_tx,
            )
            .await
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

    pub async fn finish(&mut self) {
        // Drops the sender to finish the `engine_logic::main`
        if let None = self.trans_tx.take() {
            error!("finish called multiple times");
        }

        // Wait until messages are processed
        if let Some(shut_tx) = self.shut_rx.take() {
            if let Err(_) = shut_tx.await {
                error!("shut_rx dropped before receiving the stop signal")
            }
        }
    }

    pub async fn report(&self) -> Report {
        let accounts: HashMap<ClientID, Account> = {
            // Locking here is fine since the processing has already been finished.
            let rlock = self.accounts.read().await;
            let mut accounts_clone = HashMap::new();
            for (client_id, account_state) in rlock.iter() {
                let account = account_state.account.read().await.clone();
                accounts_clone.insert(*client_id, account);
            }
            accounts_clone
        };

        Report::new(&accounts)
    }
}

mod engine_logic {
    use std::time::Duration;

    use futures::future::try_join_all;
    use tokio::time::sleep;

    use super::*;

    pub async fn master_daemon(
        n_workers: u8,
        accounts: Accounts,
        pending_queue: PendingClients,
        mut mailbox: mpsc::Receiver<Tx>,
        shutdown_tx: oneshot::Sender<()>,
    ) {
        let mut in_txs: Vec<oneshot::Sender<()>> = vec![];
        let mut out_rxs: Vec<oneshot::Receiver<()>> = vec![];

        // Spawn workers
        for _ in 0..n_workers {
            let (in_tx, in_rx) = oneshot::channel();
            let (out_tx, out_rx) = oneshot::channel();
            in_txs.push(in_tx);
            out_rxs.push(out_rx);
            let accounts_ptr = Arc::clone(&accounts);
            let pending_queue_ptr = Arc::clone(&pending_queue);
            task::spawn(async move {
                engine_logic::worker_daemon(accounts_ptr, pending_queue_ptr, in_rx, out_tx).await
            });
        }

        // Process incoming transactions until the producer is dropped.
        while let Some(tx) = mailbox.recv().await {
            trace!("Broker received {tx:?}");
            let client_id = tx.client_id();

            // 1. Place the transaction in the Account State
            {
                let mut wlock = accounts.write().await;
                match wlock.get_mut(&client_id) {
                    Some(state) => {
                        state.pending_queue.write().await.push(tx);
                    }
                    None => {
                        let state = AccountState::new();
                        state.pending_queue.write().await.push(tx);
                        wlock.insert(client_id, state);
                        info!("New account created for client {client_id:?}");
                    }
                }
            }

            // 2. Place the transaction in the pending queue
            {
                let mut wlock = pending_queue.write().await;
                wlock.insert(client_id);
            }
        }

        // Wait for all workers to finish
        for tx in in_txs.into_iter() {
            if let Err(_) = tx.send(()) {
                error!("Sender stop signal for worker failed")
            }
        }
        if let Err(_) = try_join_all(out_rxs).await {
            error!("Receiver shutdown for worker failed")
        }

        // Send shutdown signal
        if let Err(_) = shutdown_tx.send(()) {
            error!("shut_rx dropped before sending the stop signal")
        };
    }

    /// Background task that:
    /// - Picks a client with pending transactions to process
    /// - and processes all transactions from that client
    pub async fn worker_daemon(
        accounts: Accounts,
        pending_clients: PendingClients,
        stop_signal_rx: oneshot::Receiver<()>,
        shutdown_tx: oneshot::Sender<()>,
    ) {
        tokio::pin!(stop_signal_rx);
        loop {
            tokio::select! {
                mut p = pending_clients.write() => {
                  let head = p.pop_first();
                  drop(p);
                  match head {
                      None => {
                          // Don't waste CPU
                          sleep(Duration::from_millis(100)).await;
                      }
                      Some(client_id) => {
                          // TODO: is it possible to get a reference to an element
                          // of the hashmap after releasing the lock ?
                          let accounts_rlock = accounts.read().await;
                          let state = accounts_rlock.get(&client_id);
                          match state {
                              None => warn!("Retrieving state for {:?} not found", client_id),
                              Some(state) => {
                                  // Race condition:
                                  //
                                  // We want to keep both pending_queue and accounts locked
                                  // to prevent another worker from picking the account before us.
                                  //
                                  // TODO Arnau: avoid locking
                                  let mut pending_lock = state.pending_queue.write().await;
                                  let mut account_lock = state.account.write().await;
                                  let transs: Vec<Tx> = pending_lock.drain(..).collect();
                                  for trans in transs {
                                      account_lock.process_tx(trans);
                                  }
                              }
                          }
                      }
                  }
                }

                _ = &mut stop_signal_rx => {
                    info!("Stop signal received");
                    break;
                }
            }
        }
        if let Err(_) = shutdown_tx.send(()) {
            error!("Worker shutdown receiver dropped before a message was sent");
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

        let mut engine = Engine::run(2).await;
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
        engine.finish().await;

        let report = engine.report().await;
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
