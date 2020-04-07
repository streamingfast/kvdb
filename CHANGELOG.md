# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

* The `GetTransactionEvents` family of functions that take a transaction ID prefix, NOW REQUIRE a prefix that is an even number of hex characters (as they are converted to bytes and compared on bytes). They cannot be compared on half a byte (which a single hex character represents).  Sanitization must happen before calling this library, or those calls will panic.

* License changed to Apache 2.0
