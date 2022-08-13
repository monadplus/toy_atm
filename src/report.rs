//! This module includes reporting capabilities for the client's accounts.
//! Including a pretty printer to CSV.

use std::collections::HashMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{engine::Account, tx::ClientID};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report(pub Vec<AccountSummary>);

impl Report {
    pub fn new(accounts: &HashMap<ClientID, Account>) -> Self {
        let mut summaries = accounts
            .iter()
            .map(|(client_id, account)| account.summary(*client_id))
            .collect::<Vec<_>>();
        // Sorting to have consistent reports
        summaries.sort_by(|a, b| a.client_id.cmp(&b.client_id));
        Report(summaries)
    }

    /// The report can be formatted to CSV.
    ///
    /// ```ignore
    /// client,available,held,total,locked
    /// 1,100.0,20.0,120.0,false
    /// ```
    pub fn pretty_csv(&self) -> String {
        let mut wrt = csv::Writer::from_writer(vec![]);
        for r in self.0.iter() {
            wrt.serialize(r).unwrap();
        }
        String::from_utf8(wrt.into_inner().unwrap()).unwrap()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSummary {
    #[serde(rename = "client")]
    pub client_id: ClientID,
    #[serde(with = "rust_decimal::serde::str")]
    pub available: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub held: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub total: Decimal,
    pub locked: bool,
}

impl AccountSummary {
    #[allow(dead_code)]
    fn new(client_id: ClientID) -> Self {
        Self {
            client_id,
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            total: Decimal::ZERO,
            locked: false,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(unused_mut)]
    use rust_decimal_macros::dec;

    use crate::hash_map;

    use super::*;

    #[test]
    fn pretty_csv_test() {
        let accounts: HashMap<ClientID, Account> = hash_map! {
          ClientID(1) => Account {
              available: dec!(125.1234),
              held: dec!(20.0),
              deposits: hash_map!(),
              locked: false,
          },
          ClientID(2) => Account {
              available: dec!(100.0),
              held: dec!(0.0),
              deposits: hash_map!(),
              locked: true,
          },
        };
        let report = Report::new(&accounts);
        let expected = {
            let header = "client,available,held,total,locked";
            let body = concat!(
                "1,125.1234,20.0,145.1234,false\n",
                "2,100.0,0.0000,100.0,true\n",
            );
            format!("{header}\n{body}")
        };
        assert_eq!(expected, report.pretty_csv())
    }
}
