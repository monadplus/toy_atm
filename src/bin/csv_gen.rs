use rand::prelude::{IteratorRandom, ThreadRng};
use rand::{random, thread_rng, Rng};
use std::collections::HashMap;
use std::env;

use anyhow as any;
use anyhow::anyhow;
use rust_decimal::Decimal;
use toy_atm::tx::{ClientID, RawTx, Tx, TxID};

trait Arbitrary {
    fn arbitrary() -> Self;
}

impl Arbitrary for bool {
    fn arbitrary() -> Self {
        random::<bool>()
    }
}

impl Arbitrary for Decimal {
    fn arbitrary() -> Self {
        Decimal::from_f64_retain(random::<f64>())
            .unwrap()
            .round_dp(4)
    }
}

impl Arbitrary for ClientID {
    fn arbitrary() -> Self {
        let mut rng = thread_rng();
        let n: u16 = rng.gen_range(0..10);
        ClientID(n)
    }
}

struct TxGen {
    n_trans: u32,
    rng: ThreadRng,
    deposits: HashMap<ClientID, Vec<TxID>>,
    in_dispute: HashMap<ClientID, Vec<TxID>>,
}

impl TxGen {
    pub fn new() -> Self {
        TxGen {
            n_trans: 1,
            rng: thread_rng(),
            deposits: HashMap::new(),
            in_dispute: HashMap::new(),
        }
    }

    fn arbitrary(&mut self) -> Tx {
        let n: u8 = self.rng.gen_range(0..=10);
        if n <= 5 {
            let tx_id = TxID(self.n_trans);
            let tx = Tx::Deposit {
                client_id: <ClientID as Arbitrary>::arbitrary(),
                tx_id,
                amount: <Decimal as Arbitrary>::arbitrary(),
                in_dispute: false,
            };
            match self.deposits.get_mut(&tx.client_id()) {
                Some(deposits) => {
                    deposits.push(tx_id);
                }
                None => {
                    self.deposits.insert(tx.client_id(), vec![tx_id]);
                }
            }
            self.n_trans += 1;
            tx
        } else if n <= 7 {
            let tx = Tx::Withdrawal {
                client_id: <ClientID as Arbitrary>::arbitrary(),
                tx_id: TxID(self.n_trans),
                amount: <Decimal as Arbitrary>::arbitrary(),
            };
            self.n_trans += 1;
            tx
        } else if n <= 8 {
            match self.deposits.keys().cloned().choose(&mut self.rng) {
                Some(client_id) => {
                    let txs = self.deposits.get(&client_id).unwrap().clone();
                    let tx_id = txs.into_iter().choose(&mut self.rng).unwrap();
                    match self.in_dispute.get_mut(&client_id) {
                        Some(disputes) => {
                            disputes.push(tx_id);
                        }
                        None => {
                            self.deposits.insert(client_id, vec![tx_id]);
                        }
                    }
                    Tx::Dispute {
                        client_id,
                        tx_id_reference: tx_id,
                    }
                }
                None => self.arbitrary(),
            }
        } else if n <= 9 {
            match self.in_dispute.keys().cloned().choose(&mut self.rng) {
                Some(client_id) => {
                    let txs = self.in_dispute.get(&client_id).unwrap().clone();
                    let tx = txs.into_iter().choose(&mut self.rng).unwrap();
                    self.in_dispute.remove(&client_id).unwrap();
                    Tx::Resolve {
                        client_id,
                        tx_id_reference: tx,
                    }
                }
                None => self.arbitrary(),
            }
        } else {
            match self.in_dispute.keys().cloned().choose(&mut self.rng) {
                Some(client_id) => {
                    let txs = self.in_dispute.get(&client_id).unwrap().clone();
                    let tx = txs.into_iter().choose(&mut self.rng).unwrap();
                    self.in_dispute.remove(&client_id).unwrap();
                    Tx::Chargeback {
                        client_id,
                        tx_id_reference: tx,
                    }
                }
                None => self.arbitrary(),
            }
        }
    }
}

fn main() -> any::Result<()> {
    let csv_path = match env::args().nth(1) {
        None => Err(anyhow!("Usage: <csv> <n_rows>")),
        Some(file_path) => Ok(file_path),
    }?;

    let n_rows: u32 = match env::args().nth(2) {
        None => Err(anyhow!("Usage: <csv> <n_rows>")),
        Some(str) => Ok(str.parse()?),
    }?;

    let mut csv_writer = csv::Writer::from_path(&csv_path)?;
    let mut tx_gen = TxGen::new();
    for _ in 0..n_rows {
        csv_writer.serialize(RawTx::from(tx_gen.arbitrary()))?;
    }
    csv_writer.flush()?;

    Ok(())
}
