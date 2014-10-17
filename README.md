seer
========

Simplified gossip based key-value store.


Quick Start
===========

To Test:
```
$ git clone git@github.com:eliwjones/seer.git
$ cd seer
$ go test -v
=== RUN Test_auth
--- PASS: Test_auth (0.00 seconds)
=== RUN Test_messageKey
--- PASS: Test_messageKey (0.00 seconds)
=== RUN Test_joinMessage
--- PASS: Test_joinMessage (0.00 seconds)
=== RUN Test_gossip
--- PASS: Test_gossip (0.00 seconds)
=== RUN Test_tombstoneReaper
--- PASS: Test_tombstoneReaper (0.00 seconds)
PASS
ok  	github.com/eliwjones/seer	0.004s
```

To Run:
```
$ go run cmd/sd/sd.go -ip=127.0.0.1 -udp=9999 -tcp=9998 # From top level dir.
```
Try to join another server:
```
$ go run cmd/sd/sd.go -ip=127.0.0.1 -udp=8888 -tcp=8887 -seeds=127.0.0.1:9999
```
Curl the httpserver for hosts:
```
$ curl -G -s http://127.0.0.1:9998/host | python -m json.tool
[
    {
        "IndexedProperties": [
            "Host"
        ],
        "Message": {
            "Host": "127.0.0.1:8888",
            "Name": "127.0.0.1",
            "Port": "8888"
        },
        "MessageKey": "O+5KTQ3rv0sJpeY7RXXwA72wE0AaIqJdlfVdGRN4TvQ=",
        "Source": "127.0.0.1:8888",
        "Timestamp": 1397177531391
    },
    {
        "Control": "seed",
        "IndexedProperties": [
            "Host"
        ],
        "Message": {
            "Host": "127.0.0.1:9999",
            "Name": "127.0.0.1",
            "Port": "9999"
        },
        "MessageKey": "dIyUbdx3A42XWVxMqn98JWkYATPLEjz2YRC/qkz2pzM=",
        "Source": "127.0.0.1:8888",
        "Timestamp": 1397177531545
    }
]
```
Put something in:
```
$ curl -X PUT -H 'Content-Type: application/json' -H 'X-IndexedProperties: ["ServiceName"]' -d '{"ServiceName":"catpics","ServiceAddr":"127.0.0.1:12345"}' http://127.0.0.1:9998
PUT: {"Timestamp":1398114761675,"Source":"127.0.0.1:9999","Message":{"ServiceAddr":"127.0.0.1:12345","ServiceName":"catpics"},"MessageKey":"qmYSnSEMRY86evfpGlSJGJN/DTJpr6yd6rCeYuw5Stc=","IndexedProperties":["ServiceName"]}.
```
Get something back:
```
$ curl -G -s http://127.0.0.1:9998/servicename/catpics | python -m json.tool
{
    "IndexedProperties": [
        "ServiceName"
    ], 
    "Message": {
        "ServiceAddr": "127.0.0.1:12345", 
        "ServiceName": "catpics"
    }, 
    "MessageKey": "G6lCtA8RD4kkcLshG/a59H5A9aE9skvRTUPycTz0JcY=", 
    "Source": "127.0.0.1:9999", 
    "Timestamp": 1413566434650
}
```
