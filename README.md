# dfuse Key/Value Store

[![reference](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://pkg.go.dev/github.com/dfuse-io/kvdb)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


This library contains different implementations for storing blocks and transactions in a key/value database.
It is used by **[dfuse](https://github.com/dfuse-io/dfuse)**.

## Usage

```go
db, err := store.New(dsn)
if err != nil {
    return fmt.Errorf("failed setting up db: %s", err)
}
```

The following DSNs are provided by this package:
* Badger: `badger:///home/user/dfuse-data/component/my-badger.db`
* TiKV:   `tikv://pd0,pd1,pd2:2379?prefix=namespace_prefix`
* Google Cloud BigTable: `bigkv://project.instance/namespace-prefix?createTables=true`

## Contributing

**Issues and PR in this repo related strictly to the kvdb library.**

Report any protocol-specific issues in their
[respective repositories](https://github.com/dfuse-io/dfuse#protocols)

**Please first refer to the general
[dfuse contribution guide](https://github.com/dfuse-io/dfuse/blob/master/CONTRIBUTING.md)**,
if you wish to contribute to this code base.


## License

[Apache 2.0](LICENSE)
