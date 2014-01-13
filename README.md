# Seer

Service discovery a.k.a. directory services a.k.a LDAP-y.

# Local Test

Go Get:
```
  cd ~
  go get github.com/eliwjones/seer
```

Start initial node:

```
	go run go/src/github.com/eliwjones/seer/seer.go -ip=127.0.0.1 -tcp=10000 -udp=9000 -listenbroadcast=false
```

Start peers:

```
	go run go/src/github.com/eliwjones/seer/seer.go -ip=127.0.0.1 -tcp=10001 -udp=9001 -listenbroadcast=false -bootstrap=127.0.0.1:9000
	go run go/src/github.com/eliwjones/seer/seer.go -ip=127.0.0.1 -tcp=10002 -udp=9002 -listenbroadcast=false -bootstrap=127.0.0.1:9000
	go run go/src/github.com/eliwjones/seer/seer.go -ip=127.0.0.1 -tcp=10003 -udp=9003 -listenbroadcast=false -bootstrap=127.0.0.1:9000
	go run go/src/github.com/eliwjones/seer/seer.go -ip=127.0.0.1 -tcp=10004 -udp=9004 -listenbroadcast=false -bootstrap=127.0.0.1:9000
```

Verify peers are aware (result of each of these commands should be "5"):
```
  curl -s http://127.0.0.1:10000/service/Seer | awk -F'},{' '{print NF}'
  curl -s http://127.0.0.1:10001/service/Seer | awk -F'},{' '{print NF}'
  curl -s http://127.0.0.1:10002/service/Seer | awk -F'},{' '{print NF}'
  curl -s http://127.0.0.1:10003/service/Seer | awk -F'},{' '{print NF}'
  curl -s http://127.0.0.1:10004/service/Seer | awk -F'},{' '{print NF}'
```

