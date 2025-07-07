# seer

A simplified, gossip-based key-value store written in Go.

This project provides a distributed, eventually-consistent key-value store that uses a gossip protocol for node discovery and data propagation.

### Project Structure

This project is organized like so:

*   `cmd/seer/`: The main application binary for the key-value store.
*   `cmd/gossip-sim/`: An in-memory simulation tool for testing gossip strategies.
*   `internal/seer/`: The core library logic, private to this project.

## Quick Start

### Running Tests

To run all tests for the project from the root directory:

```bash
go test ./...
```

### Running the Seer KV Store

You can run `seer` nodes directly using `go run` without needing to build the binary first.

**1. Start the first node (a seed node):**

In your first terminal, run:
```bash
go run ./cmd/seer/ -ip=127.0.0.1 -udp=9999 -tcp=9998
```

**2. In another terminal, start a second node and have it join the first:**

```bash
go run ./cmd/seer/ -ip=127.0.0.1 -udp=8888 -tcp=8887 -seeds=127.0.0.1:9999
```
The second node will discover the first via the `-seeds` flag and they will begin gossiping.

### Interacting with the API

Once the nodes are running, you can use `curl` to interact with the HTTP interface of any node in the cluster.

**Get all known hosts:**

This will return a list of all nodes currently known to the node you query.
```bash
curl -G -s http://127.0.0.1:9998/host | python -m json.tool
```

**Store a new key-value pair:**

The `X-IndexedProperties` header is crucial. It tells `seer` which fields in the JSON body to create an index for, allowing for fast lookups by that key.
```bash
curl -X PUT \
  -H 'Content-Type: application/json' \
  -H 'X-IndexedProperties: ["ServiceName"]' \
  -d '{"ServiceName":"catpics","ServiceAddr":"127.0.0.1:12345"}' \
  http://127.0.0.1:9998
```

**Retrieve a value by its index:**

Because we indexed by `ServiceName` in the previous step, we can now look up the data using the path `/servicename/catpics`.
```bash
curl -G -s http://127.0.0.1:9998/servicename/catpics | python -m json.tool
```

### Running the Gossip Simulator

The project includes an in-memory simulator to test the effectiveness of different gossip propagation strategies. This is useful for tuning parameters and observing how data flows through a hypothetical system without running actual network services.

To run the simulator:
```bash
go run ./cmd/gossip-sim/
```
This will output statistics on message delivery, path length, and more for various gossip algorithms.