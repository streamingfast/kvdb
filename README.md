# dfuse Key/Value Store

[![reference](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://pkg.go.dev/github.com/dfuse-io/kvdb)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


This library contains different implementations for storing blocks and transactions in a key/value database.
It is used by **[dfuse](https://github.com/dfuse-io/dfuse)**.

## Usage

```go
db, err := eosdb.New(dsn)
if err != nil {
    return fmt.Errorf("failed setting up db: %s", err)
}
```

The following DSNs are provided by this pakcage:

* SQLite: `sqlite3:///tmp/mydb.db?cache=shared&mode=memory&createTables=true`
* MySQL: `mysql://root:@(127.0.0.1:4000)/mydb?createTables=true`
* Bigtable: `bigtable://project.instance/tbl-prefix?createTables=true`

## Contributing

**Issues and PR in this repo related strictly to the kvdb library.**

Report any protocol-specific issues in their
[respective repositories](https://github.com/dfuse-io/dfuse#protocols)

**Please first refer to the general
[dfuse contribution guide](https://github.com/dfuse-io/dfuse/blob/master/CONTRIBUTING.md)**,
if you wish to contribute to this code base.


## License

[Apache 2.0](LICENSE)
