# solana-postgres-rpc-server
A RPC server serving major RPC requests from PostgreSQL database streamed by the [solana-geyser-plugin-postgres](https://github.com/solana-labs/solana-accountsdb-plugin-postgres) plugin.

## Build the RPC Server
Do the following to build

```
cargo build [--release]
```

## Run the RPC Server

### Prepare the Database
The RPC server requires a PostgreSQL database has already been setup. Please consult with
[solana-geyser-plugin-postgres](https://github.com/solana-labs/solana-accountsdb-plugin-postgres) on
setting up the plugin and database. In addition, run the following script to create some of the stored
functions and procedures in the database which are used for serving RPC queries.

```
psql -U solana -p 5433 -h 10.138.0.9 -w -d solana -f sql/query_account.sql
```

### Start the RPC Server

Execute the command similar to the following to run the RPC server listening on port 8888.

```
solana-postgres-rpc-server --db-config ~/postgres-db-config.json --rpc-port 8888 --rpc-threads 100 -o -
```

Use the following to see the detailed arguments of the command line.

```
solana-postgres-rpc-server --help
```

### The Database Configuration File Format

The `postgres-db-config.json` file specifies the connection information to the PostgreSQL database in JSON format.

For example,

```
{
	"host": "postgres-server",
	"user": "solana",
	"port": 5433,
}
```

The `host`, `user`, and `port` control the PostgreSQL configuration
information. For more advanced connection options such as passwords, please use the
`connection_str` field. Please see [Rust Postgres Configuration](https://docs.rs/postgres/0.19.2/postgres/config/struct.Config.html).


## Running RPC Queries Against the Server

The Server currently supports the following RPC calls.

- getAccountInfo
- getMultipleAccounts
- getProgramAccounts

There is plan to add support for other RPC calls related to blocks, accounts and transactions.

Please see [JSON RPC API](https://docs.solana.com/developing/clients/jsonrpc-api) for details on these APIs.

For example to get the account info for an account, do the following

```
curl http://localhost:8888 -X POST -H "Content-Type: application/json" -d '
  {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getAccountInfo",
    "params": [
      "vines1vzrYbzLMRdu58ou5XTby4qAqVRLmqo36NKPTg",
      {
        "encoding": "base58"
      }
    ]
  }
'
```

Response

```
{
    "id": 1,
    "jsonrpc": "2.0",
    "result": {
        "context": {
            "slot": 126599223
        },
        "value": {
            "data": [
                "",
                "base58"
            ],
            "executable": false,
            "lamports": 1974219920,
            "owner": "11111111111111111111111111111111",
            "rentEpoch": 293
        }
    }
}
```