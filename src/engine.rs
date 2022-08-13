//! This module contains the transaction engine.

#![allow(clippy::new_without_default)]
use std::{
    collections::HashMap,
    sync::mpsc::{self, Receiver, Sender},
};

use crate::{
    report::{AccountSummary, Report},
    tx::{ClientID, Tx, TxID},
};
use anyhow as any;
use log::{error, info, trace, warn};
use rust_decimal::Decimal;

/* TODO
accounts: RwLock<ClientID, RwLock<(Account, pending: Vec<Tx>)>>
pending: RwLock<HashSet<ClientID>

1. Spawn the main daemon:
  1.1. Read a Tx from the channel
  1.2. Place the transaction in the pending map

2. Spawn n daemon that:
  2.1. clientID <- Pop the first pending
  2.2. RwLock<(account, pending)> <- Retrieves the account(clientID)
  2.3. account.process_tx(pending)
*/

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

/// Wrapper for [Sender<Tx>].
/// Use this to communicate with the [Engine].
/// Don't forget to call [EngineTx::finish] when you wish to stop the engine.
pub struct EngineTx(Sender<Tx>);

impl EngineTx {
    pub fn send_tx(&self, tx: Tx) -> any::Result<()> {
        Ok(self.0.send(tx)?)
    }

    /// Drops the sender when the processing is finished.
    pub fn finish(self) {}
}

/// The transaction engine.
pub struct Engine {
    rx: Receiver<Tx>,
    accounts: HashMap<ClientID, Account>,
}

impl Engine {
    pub fn new() -> (EngineTx, Engine) {
        let (tx, rx) = mpsc::channel();
        let sink = EngineTx(tx);
        let engine = Engine {
            rx,
            accounts: HashMap::new(),
        };
        (sink, engine)
    }

    pub fn start(&mut self) -> any::Result<Report> {
        while let Ok(unprocessed_tx) = self.rx.recv() {
            trace!("Processing transaction {unprocessed_tx:?}");
            self.process_tx(unprocessed_tx)
        }

        Ok(Report::new(&self.accounts))
    }

    pub fn process_tx(&mut self, tx: Tx) {
        let client_id = tx.client_id();
        match self.accounts.get_mut(&client_id) {
            None => {
                let mut account = Account::new();
                account.process_tx(tx);
                self.accounts.insert(client_id, account);
                info!("New account created for client {client_id:?}");
            }
            Some(account) => {
                account.process_tx(tx);
            }
        };
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

    #[test]
    fn engine_test() {
        let (engine_tx, mut engine) = Engine::new();
        // Client 1
        engine_tx.send_tx(tx!(+,1,1,100)).unwrap();
        engine_tx.send_tx(tx!(+,1,2,50)).unwrap();
        engine_tx.send_tx(tx!(-,1,3,30)).unwrap();
        engine_tx.send_tx(tx!(!, 1, 1)).unwrap();
        engine_tx.send_tx(tx!(!, 1, 2)).unwrap();
        engine_tx.send_tx(tx!(!, 1, 2)).unwrap();
        engine_tx.send_tx(tx!(ok, 1, 1)).unwrap();
        engine_tx.send_tx(tx!(ko, 1, 2)).unwrap();
        engine_tx.send_tx(tx!(+,1,4,100)).unwrap();
        // Client 2
        engine_tx.send_tx(tx!(+,2,5,100)).unwrap();
        engine_tx.send_tx(tx!(+,2,6,100)).unwrap();
        engine_tx.send_tx(tx!(-,2,7,30)).unwrap();
        engine_tx.send_tx(tx!(!, 2, 5)).unwrap();
        engine_tx.send_tx(tx!(!, 2, 7)).unwrap();
        engine_tx.finish();

        let report = engine.start().unwrap();
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
