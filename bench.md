# Benchmarking

Hyperfine:

```bash
$ cargo build --release
$ hyperfine './target/release/toy_atm resources/test/large.csv'
```

Flamegraph:

```bash
# Don't forget to `profile.release.debug` = true on `Cargo.toml`
$ cargo flamegraph -- ./resources/test/large.cs
$ firefox flamegraph.svg
```

Perf:

```bash
$ perf report --symbol-filter closure
```

## Optimizations

1. Cargo.toml opt flags
    - Before: 516.6 ms ±  16.0 ms
    - After:  464.1 ms ±   8.2 ms
2. Replace csv::ReaderBuilder by manual parsing
    - Before: 464.1 ms ±   8.2 ms
    - After: 321.9 ms ±  15.1 ms
3. Mutex<HashMap> by DashMap
    - Before: 321.9 ms ±  15.1 ms
    - After:
