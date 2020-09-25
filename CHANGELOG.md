# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed
- [`tikv`] Added ability to customize batch time threshold by using `batch_time_threshold=<value>` query parameter in dsn (accepts `time.Duration` string formats).
- [`tikv`] Added ability to customize batch ops threshold by using `batch_ops_threshold=<value>` query parameter in dsn (accepts positive numbers).
- [`tikv`] Added ability to customize batch size threshold by using `batch_size_threshold=<value>` query parameter in dsn (accepts positive numbers).
- [`tikv`] Added ability to customize compression size threshold by using `compression_size_threshold=<value>` query parameter in dsn (accepts positive numbers).
- [`tikv`] Re-added support for compression to overcome issue where single element are bigger than 8MB. This is now an opt-in feature, to activate, use `compression=zstd` query parameter in dsn.
- [`tikv`] Default batch size threshold ifs now 8MiB, which is the default max raft entry size in TiKV.
- [`core`] **BREAKING** Renamed `store.BachOp` to `store.BatchOp` (fixed typo in `Bach`).
- [`core`] Added `Close` method on `store.KVStore`.

## [v0.0.1]

### Changed
- [`core`] Added `BatchPrefix` method on `store.KVStore`. It's now possible to perform multiple prefix scan on a single batch. Driver not supporting natively the functionality degrades to a sequential call to standard prefix capabilities.
- [`core`] **BREAKING** Added `limit` options to `store.KVStore#Prefix`.
- [`core`] **BREAKING** Function `store.Iterator#Next` return's type is now `store.KV`, previously `*store.KV`.
- [`core`] **BREAKING** Function `store.Iterator#PushItem` `item` argument's type is now `store.KV`, previously `*store.KV`.
- [`tikv`] Added 'OptionEmptyValueEnable' to let the driver simulate empty values by adding a little byte to every row
- **BREAKING** Removed explicit compression (just use built-in from each db), only kept within badger for backward-compatibility

### Improved
- [`core`] Reduced memory allocations when receiving items from iterators.
- [`badger`] Improved performance of `Scan` in presence of small limit value.
- [`bigkv`] Improved performance of `BatchGet` which was sequential instead of using BigTable `ReadRows` call which is batched.

### Fixed

- [`badger`] Fixed returning proper store.NotFoundErrors on 'BatchGet'

## [pre-open-sourcing]

### Changed

* The `GetTransactionEvents` family of functions that take a transaction ID prefix, NOW REQUIRE a prefix that is an even number of hex characters (as they are converted to bytes and compared on bytes). They cannot be compared on half a byte (which a single hex character represents).  Sanitization must happen before calling this library, or those calls will panic.

* License changed to Apache 2.0
