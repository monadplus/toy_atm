# Toy Transaction Engine

CLI application that emulates a transaction engine. There are 5 types of transactions:

* **Deposit**: increases the funds of an account.
* **Withdrawal**: decreases the funds of an account. Ignored if there are not enough funds in the account.
* **Dispute**: holds a deposit transaction until it is resolved or chargedback. Ignored if the transaction doesn't exist.
* **Resolve**: unlocks a dispute retuning the held funds to the account. Ignored if the transaction doesn't exist or it is not in dispute.
* **Chargeback**: unlocks a dispute decreasing the funds from the account and freezes the account. Ignored if the transaction doesn't exist or it is not in dispute.

The app expects a CSV file where each line represent a transaction.
The format of the input CSV is the following:

```
type,client,tx,amount
deposit,1,1,200.0
withdrawal,1,2,200.0
dispute,1,1
resolve,1,1
cargeback,1,1
```

The output consist of a CSV list of the state of all the accounts after all transactions have been processed.
The format is the following:

```
client,available,held,total,locked
1,100.0,20.0,120.0,false
```

## Compile

```sh
# Compile app
cargo build

# Compile tests
cargo test --no-run
```

## Run 

```sh
cargo run -- <file.csv>
```

## Execute Tests

```sh
cargo test
```

## Docs

```sh
cargo doc --open
```

## Assumptions

* Transactions IDs are global and unique.
* Withdrawals cannot be disputed, only deposits.
* Transactions to a locked account are ignored.
* The balance of an account can be negative (e.g. deposit > withdrawal > deposit disputed).

## Implementation choices

This current implementation includes
- multithreading with `tokio` using a master-slave architecture,
- proper error handling through `Result` using [anyhow](https://docs.rs/anyhow/latest/anyhow/) and [thiserror](https://docs.rs/thiserror/latest/thiserror/),
- logging using `env_logger`,
- a battery of tests to check the correctness of the code,
- rustdocs

The CSV is processed line by line without loading the whole file into memory.

## TODOs

- [ ] Change architecture
  - The broker will have a map of worker <-> List(ClientID).
  - The broker will forward each tx to the assigned worker.
  - The Accounts will be sharded by worker to only lock the shard of the worker
  - Inside each worker, the processing of the transactions will be sequential.

- [ ] Profile, benchmark and improve performance
