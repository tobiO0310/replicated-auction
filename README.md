Example setup, run each command in different terminals:

```bash
$ go run server/*.go --id=0 --port=3000 --auction_port=3001
$ go run server/*.go --id=1 --port=3002 --auction_port=3003 --try_connect=3000
$ go run client/*.go
```
