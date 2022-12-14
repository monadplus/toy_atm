# Transaction Engine

CLI application that emulates a transaction engine. 

It includes 5 types of transactions:

* **Deposit**: increases the funds of an account.
* **Withdrawal**: decreases the funds of an account. Ignored if there are not enough funds in the account.
* **Dispute**: holds a deposit transaction until it is resolved or chargedback. Ignored if the transaction doesn't exist.
* **Resolve**: unlocks a dispute retuning the held funds to the account. Ignored if the transaction doesn't exist or it is not in dispute.
* **Chargeback**: unlocks a dispute decreasing the funds from the account and freezes the account. Ignored if the transaction doesn't exist or it is not in dispute.

The app expects a CSV file where each line represent a transaction.
The format of the input CSV is the following:

```csv
type,client,tx,amount
deposit,1,1,200.0
withdrawal,1,2,200.0
dispute,1,1
resolve,1,1
cargeback,1,1
```

The output consist of a CSV list of the state of all the accounts after all transactions have been processed.
The format is the following:

```csv
client,available,held,total,locked
1,100.0,20.0,120.0,false
```

### Assumptions

* Transactions IDs are global and unique.
* Withdrawals cannot be disputed, only deposits.
* Transactions to a locked account are ignored.
* The balance of an account can be negative (e.g. deposit > withdrawal > deposit disputed).

### Performance

We measured the performance of the application on a [large CSV](./resources/test/large.csv) (1M transactions) on a `Thinkpad p14s on a Ryzen 7 PRO 5850U`.

```
$ time cargo run --release -- resources/test/large.csv  
1.86s user 0.62s system 270% cpu 0.920 total
```

The transaction engine can process more than `1,000,000 tx/s`!

The bad news is that the sequential version performs as fast as the parallel one.

My hypothesis is that the bottleneck is the parsing because it is done sequentially 
and the engine logic is so simple that can be done faster that CSV processing.

We confirmed this hypothesis by adding an artificial delay (100ms) on the processing of each transaction.
We generated a smaller CSV (1000 lines) and run several runs on different numbers of workers.

```bash
# 1 worker
cargo run --release -- large.csv  0.12s user 0.05s system 0% cpu 1:41.23 total

# 2 workers
cargo run --release -- large.csv  0.10s user 0.05s system 0% cpu 51.541 total

# 4 workers
cargo run --release -- large.csv  0.11s user 0.02s system 0% cpu 30.006 total

# 8 workers
cargo run --release -- large.csv  0.08s user 0.02s system 0% cpu 19.375 total

# 16 workers
cargo run --release -- large.csv  0.07s user 0.02s system 0% cpu 11.184 total
```

We peaked at 16 workers with a 10x speed up.

## Versions

This project has been progressively built from simple to complex:

- [Version 0.1](https://github.com/monadplus/toy_atm/tree/v0.1/sequential): single process
- [Version 0.2](https://github.com/monadplus/toy_atm/tree/v0.2/multithreading): multi-threading
  - This version has a race condition. It can (randomly) be reproduced on the test `engine_test`
- [Version 1.0 (current)](https://github.com/monadplus/toy_atm): multi-threading with master-slaves architecture

## Implementation choices

This current implementation includes
- multi-threading with `tokio` using a master-slave architecture,
- proper error handling through `Result` using [anyhow](https://docs.rs/anyhow/latest/anyhow/) and [thiserror](https://docs.rs/thiserror/latest/thiserror/),
- logging using `env_logger`,
- a battery of tests to check the correctness of the code,
- rustdocs

The CSV is processed line by line without loading the whole file into memory.

## Compile

```sh
cargo build
cargo build --release
cargo test --no-run
```

## Run 

```sh
cargo run -- <file.csv>

# <log_level> = trace/info/warn/error
RUST_LOG=<log_level> cargo run -- <file.csv>
```

### Generate inputs

The project contains a subproject that allows to generate arbitrary large input CSV files.

```sh
cargo run --bin csv_gen -- large.csv 100000
```

## Tests

```sh
cargo test
```

## Docs

```sh
cargo doc --open
```

## TODOs

- [ ] Replace `RwLock<HashMap<..>>` for `DashMap`
- [ ] Profile, benchmark and improve performance
