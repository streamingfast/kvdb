# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- [`core`] **BREAKING** Added `limit` options to `store.KVStore#Prefix`.
- [`core`] **BREAKING** Function `store.Iterator#Next` return's type is now `store.KV`, previously `*store.KV`.
- [`core`] **BREAKING** Function `store.Iterator#PushItem` `item` argument's type is now `store.KV`, previously `*store.KV`.

### Improved
- [`core`] Reduced memory allocations when receiving items from iterators.
- [`badger`] Improved performance of `Scan` in presence of small limit value.
- [`bigkv`] Improved performance of `BatchGet` which was sequential instead of using BigTable `ReadRows` call which is batched.

### Changed

* The `GetTransactionEvents` family of functions that take a transaction ID prefix, NOW REQUIRE a prefix that is an even number of hex characters (as they are converted to bytes and compared on bytes). They cannot be compared on half a byte (which a single hex character represents).  Sanitization must happen before calling this library, or those calls will panic.

* License changed to Apache 2.0
