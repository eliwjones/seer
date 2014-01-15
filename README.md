# Seer

Service discovery a.k.a. directory services a.k.a LDAP-y.

# Local Test

Go Get:
```
	$ cd ~
	$ go get github.com/eliwjones/seer
	$ go get code.google.com/p/go.net/ipv4
```

Start initial node:

```
	$ go run go/src/github.com/eliwjones/seer/seer.go -ip=127.0.0.1 -tcp=10000 -udp=9000 -listenbroadcast=false
```

Start peers:

```
	$ go run go/src/github.com/eliwjones/seer/seer.go -ip=127.0.0.1 -tcp=10001 -udp=9001 -listenbroadcast=false -bootstrap=127.0.0.1:9000
	$ go run go/src/github.com/eliwjones/seer/seer.go -ip=127.0.0.1 -tcp=10002 -udp=9002 -listenbroadcast=false -bootstrap=127.0.0.1:9000
	$ go run go/src/github.com/eliwjones/seer/seer.go -ip=127.0.0.1 -tcp=10003 -udp=9003 -listenbroadcast=false -bootstrap=127.0.0.1:9000
	$ go run go/src/github.com/eliwjones/seer/seer.go -ip=127.0.0.1 -tcp=10004 -udp=9004 -listenbroadcast=false -bootstrap=127.0.0.1:9000
```

Verify peers are aware (result of each of these commands should be "5"):
```
	$ for i in {0..4}; do curl -s http://127.0.0.1:1000$i/service/Seer | awk -F'},{' '{print NF}'; done;
```

Add a new service:
```
	$ curl -X PUT -H "Content-Type: application/json" -d '{"ServiceName":"catpics","ServiceAddr":"127.0.0.1:12345"}' http://127.0.0.1:10004/service
```

Ask a node about this new service:
```
	$ curl http://127.0.0.1:10001/service/catpics
```
