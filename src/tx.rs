//! This module includes the definitions e.g. [Tx], [ClientID], [TxID], etc.

use rust_decimal::Decimal;
use serde::{self, Deserialize, Serialize};

// We assume:
// * ClientID are global unique.
// * Each client has at most one account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct ClientID(pub u16);

// We assume:
// * TxIDs are global unique.
// * The order of the tx in the system is the same as the order of the tx in the CSV.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct TxID(pub u32);

/// Raw transaction from CSV
#[derive(Debug, Serialize, Deserialize)]
pub struct RawTx {
    #[serde(rename = "type")]
    ty: String,
    #[serde(rename = "client")]
    client_id: ClientID,
    #[serde(rename = "tx")]
    tx_id: TxID,
    amount: Option<Decimal>,
}

// Tagged enums do not work with csv.
// See https://github.com/BurntSushi/rust-csv/issues/211
// We decided to go with serde(try_from).

/// There are 5 types of transactions in the system:
///
/// * Deposit: increases the funds of an account.
/// * Withdrawal: decreases the funds of an account.
/// * Dispute: holds a deposit transaction until it is resolved or chargedback.
/// * Resolve: unlocks a dispute retuning the held funds to the account.
/// * Chargeback: unlocks a dispute decreasing the funds from the account
///               and freezes the account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawTx")]
pub enum Tx {
    Deposit {
        #[serde(rename = "client")]
        client_id: ClientID,
        #[serde(rename = "tx")]
        tx_id: TxID,
        amount: Decimal,
        #[serde(default)]
        in_dispute: bool,
    },
    Withdrawal {
        #[serde(rename = "client")]
        client_id: ClientID,
        #[serde(rename = "tx")]
        tx_id: TxID,
        amount: Decimal,
    },
    Dispute {
        #[serde(rename = "client")]
        client_id: ClientID,
        #[serde(rename = "tx")]
        tx_id_reference: TxID,
    },
    Resolve {
        #[serde(rename = "client")]
        client_id: ClientID,
        #[serde(rename = "tx")]
        tx_id_reference: TxID,
    },
    Chargeback {
        #[serde(rename = "client")]
        client_id: ClientID,
        #[serde(rename = "tx")]
        tx_id_reference: TxID,
    },
}

impl Tx {
    pub fn client_id(&self) -> ClientID {
        match self {
            Tx::Deposit { client_id, .. } => *client_id,
            Tx::Withdrawal { client_id, .. } => *client_id,
            Tx::Dispute { client_id, .. } => *client_id,
            Tx::Resolve { client_id, .. } => *client_id,
            Tx::Chargeback { client_id, .. } => *client_id,
        }
    }
}

/// Macro to create transactions by hand quickly while testing.
#[cfg(test)]
#[macro_export]
macro_rules! tx {
    (+, $c:expr, $t:expr, $a:expr) => {
        $crate::tx::Tx::Deposit {
            client_id: $crate::tx::ClientID($c),
            tx_id: $crate::tx::TxID($t),
            amount: rust_decimal::Decimal::new($a, 0),
            in_dispute: false,
        }
    };
    (-, $c:expr, $t:expr, $a:expr) => {
        $crate::tx::Tx::Withdrawal {
            client_id: $crate::tx::ClientID($c),
            tx_id: $crate::tx::TxID($t),
            amount: rust_decimal::Decimal::new($a, 0),
        }
    };
    (!, $c:expr, $t:expr) => {
        $crate::tx::Tx::Dispute {
            client_id: $crate::tx::ClientID($c),
            tx_id_reference: $crate::tx::TxID($t),
        }
    };
    (ok, $c:expr, $t:expr) => {
        $crate::tx::Tx::Resolve {
            client_id: $crate::tx::ClientID($c),
            tx_id_reference: $crate::tx::TxID($t),
        }
    };
    (ko, $c:expr, $t:expr) => {
        $crate::tx::Tx::Chargeback {
            client_id: $crate::tx::ClientID($c),
            tx_id_reference: $crate::tx::TxID($t),
        }
    };
}

impl TryFrom<RawTx> for Tx {
    type Error = String;

    fn try_from(raw: RawTx) -> Result<Self, Self::Error> {
        let unwrap_amount = |err_msg| raw.amount.map(Ok).unwrap_or(Err(err_msg));
        match &raw.ty as &str {
            "deposit" => Ok(Tx::Deposit {
                client_id: raw.client_id,
                tx_id: raw.tx_id,
                amount: unwrap_amount("Deposit expects amount field")?,
                in_dispute: false,
            }),
            "withdrawal" => Ok(Tx::Withdrawal {
                client_id: raw.client_id,
                tx_id: raw.tx_id,
                amount: unwrap_amount("Withdrawal expects amount field")?,
            }),
            "dispute" => Ok(Tx::Dispute {
                client_id: raw.client_id,
                tx_id_reference: raw.tx_id,
            }),
            "resolve" => Ok(Tx::Resolve {
                client_id: raw.client_id,
                tx_id_reference: raw.tx_id,
            }),
            "chargeback" => Ok(Tx::Chargeback {
                client_id: raw.client_id,
                tx_id_reference: raw.tx_id,
            }),
            _ => Err(format!("Not expected type {}", raw.ty)),
        }
    }
}

impl From<Tx> for RawTx {
    fn from(tx: Tx) -> Self {
        match tx {
            Tx::Deposit {
                client_id,
                tx_id,
                amount,
                ..
            } => RawTx {
                ty: "deposit".to_string(),
                client_id,
                tx_id,
                amount: Some(amount),
            },
            Tx::Withdrawal {
                client_id,
                tx_id,
                amount,
            } => RawTx {
                ty: "withdrawal".to_string(),
                client_id,
                tx_id,
                amount: Some(amount),
            },
            Tx::Dispute {
                client_id,
                tx_id_reference,
            } => RawTx {
                ty: "dispute".to_string(),
                client_id,
                tx_id: tx_id_reference,
                amount: None,
            },
            Tx::Resolve {
                client_id,
                tx_id_reference,
            } => RawTx {
                ty: "resolve".to_string(),
                client_id,
                tx_id: tx_id_reference,
                amount: None,
            },
            Tx::Chargeback {
                client_id,
                tx_id_reference,
            } => RawTx {
                ty: "chargeback".to_string(),
                client_id,
                tx_id: tx_id_reference,
                amount: None,
            },
        }
    }
}
