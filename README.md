Auction time is 30 seconds, can be manually set by changing the AUCTION_TIME attribute in the auction.go file.

Example setup, run each command in different terminals:
```bash
$ go run server/*.go --id=0 --port=3000 --auction_port=3001
$ go run server/*.go --id=1 --port=3002 --auction_port=3003 --try_connect=3000
$ go run client/*.go
```
To test crash resistance, use `ctrl`+ `c` to close a running server and continue using the client.
