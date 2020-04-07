Blocks and transactions storage layer for the dfuse Stack
=========================================================

Usage:

```go
db, err := eosdb.New(dsn)
if err != nil {
    return fmt.Errorf("failed setting up db: %s", err)
}
```

### Sample DSNs

* SQLite: `sqlite3:///tmp/mydb.db?cache=shared&mode=memory&createTables=true`
* MySQL: `mysql://root:@(127.0.0.1:4000)/mydb?createTables=true`
* Bigtable: `bigtable://project.instance/tbl-prefix?createTables=true`
