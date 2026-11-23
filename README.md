# Replicated auction
This program represents an auction service that has multiple servers to make the auction continue if a server was to crash (up to n-1 crashes)

The auction itself lasts 30 seconds, where in the clients can bid. The auction time can be changed in the auction.go file, if you wish to change the duration of it.

## Servers
To run the servers, you must run the following commands in different shells
```bash
$ go run server/*.go --id=0 --port=3000 --auction_port=3001
$ go run server/*.go --id=1 --port=3002 --auction_port=3003 --try_connect=3000
```
You can run more servers if you wish, you just have to update the data given in the line (+1 to id, and make sure that the ports do not interfere with any ports in use)

## Client
To run the client, you must use the following command in your shell
```bash
$ go run client/*.go
```

## Crashing
To test the crash resistance of our program, use `ctrl`+ `c` in the shell of a running server, and then continue using the client.
