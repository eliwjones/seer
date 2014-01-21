// +build gossip_protocol_simulate

package main

import (
        "fmt"
        "math"
        "math/rand"
        "runtime"
        "sort"
        "strconv"
        "strings"
        "time"
)

type ChannelMessage struct {
        Destination string
        Message     Gossip
}

type Gossip struct {
        Key     string
        Path    []int64
        TS      int64
        Bounce  int
}

var (
        totalCounterChannel  chan int
        uniqueCounterChannel chan int

        doneChannel       = make(chan bool, 10)
        node_map          = map[string]map[string]Gossip{}
        node_channels     = map[int]chan ChannelMessage{}
        GossipFunc        = map[string]func(string, Gossip){}
        current_func      = ""
        bounceLimit       = 0
        pathLimit         = 0
        nodeCount         = 0
        gossipeeCount     = 0
        gossipCount       = 0
        uniqueGossipCount = 0
)

func init() {
        // Init mapped GossipGossip funcs here since can't figure out to assign to map on outside.

        GossipFunc["GossipGossip1"] = func(node_key string, gossip Gossip) {
                if gossip.Bounce > bounceLimit {
                        return
                }
                if len(gossip.Path) > pathLimit {
                        return
                }
                // Choose random hosts to gossip to.

                for i := 0; i < gossipeeCount; i++ {
                        //Choose idx.  SendGossip.
                        ready := false
                        idx := int64(0)
                        for !ready {
                                ready = true
                                idx = int64(rand.Intn(len(node_map)))
                                for _, val := range gossip.Path {
                                        if idx == val {
                                                ready = false
                                        }
                                }
                        }
                        next_node_key := fmt.Sprintf("node_%d", idx)
                        SendGossip(next_node_key, gossip)
                }
        }

        GossipFunc["GossipGossip2"] = func(node_key string, gossip Gossip) {
                if gossip.Bounce > bounceLimit {
                        return
                }
                if len(gossip.Path) > pathLimit {
                        return
                }
                // Choose random hosts to gossip to.
                seen_map := map[int64]bool{}
                for _, val := range gossip.Path {
                        seen_map[val] = true
                }
                // Way slower since has to iterate over all nodes.
                for i := 0; i < nodeCount; i++ {
                        if seen_map[int64(i)] {
                                continue
                        }
                        dest_node_key := fmt.Sprintf("node_%d", i)
                        // draw rand to see if should send.
                        rand := rand.Intn(nodeCount)
                        if rand <= gossipeeCount {
                                SendGossip(dest_node_key, gossip)
                        }
                }
        }
}

func main() {
        rand.Seed(time.Now().UTC().UnixNano())
        runtime.GOMAXPROCS(runtime.NumCPU())

        pathLimit = 7
        bounceLimit = 0
        nodeCount = 500
        gossipeeCount = int(math.Log2(float64(nodeCount)))

        initChannels(10, nodeCount)
        test_gossip := Gossip{Key: "test_key", TS: int64(99)}
        
        for fnName, _ := range GossipFunc {
                current_func = fnName

                initNodes(nodeCount)
                initCounters()
                
                // Percolate update gossip.
                SendGossip("node_0", test_gossip)
                loops := 0
                for {
                        select {
                        case <-doneChannel:
                                goto calculate
                        case <-time.After(time.Duration(1) * time.Second):
                                if uniqueGossipCount > int(0.9*float64(nodeCount)) || loops > 10 {
                                        goto calculate
                                }
                                loops += 1
                        }
                }
        calculate:
                calculateStats(test_gossip)
        }
}

func ProcessGossip(node_key string, gossip Gossip) {
        if node_map[node_key][gossip.Key].TS < gossip.TS {
                node_map[node_key][gossip.Key] = gossip
                uniqueCounterChannel <- 1
        } else {
                gossip.Bounce += 1
        }
        gossip.Path = append(gossip.Path, ExtractNodeIDX(node_key))
        GossipFunc[current_func](node_key, gossip)
}

func SendGossip(node_key string, gossip Gossip) {
        // send Gossip down node_channels[node_key]
        totalCounterChannel <- 1
        node_modulus := int(ExtractNodeIDX(node_key)) % len(node_channels)
        node_channels[node_modulus] <- ChannelMessage{Destination: node_key, Message: gossip}
}

func ReceiveGossip(node_modulus int) {
        // Gossip comes down channel.  Take it and update node_map accordingly.
        for {
                select {
                case channel_message := <-node_channels[node_modulus]:
                        ProcessGossip(channel_message.Destination, channel_message.Message)
                }
        }
}

func ExtractNodeIDX(node_key string) int64 {
        node_parts := strings.Split(node_key, "_")
        idx, _ := strconv.ParseInt(node_parts[1], 10, 64)
        return idx
}

func initNodes(nodeCount int) {
        node_map = map[string]map[string]Gossip{}
        for i := 0; i < nodeCount; i++ {
                node_key := fmt.Sprintf("node_%d", i)
                node_map[node_key] = map[string]Gossip{}
        }
}

func initCounters() {
        gossipCount = 0
        uniqueGossipCount = 0
}

func initChannels(channelCount int, nodeCount int) {
        node_channels = map[int]chan ChannelMessage{}
        for i := 0; i < channelCount; i++ {
                // Multiplier (message redundancy) is usually under 20 so..
                node_channels[i] = make(chan ChannelMessage, 20*int(nodeCount/channelCount))
                go ReceiveGossip(i)
        }
        totalCounterChannel = make(chan int, 1000)
        uniqueCounterChannel = make(chan int, 1000)
        go gossipCounter()
}

func gossipCounter() {
        for {
                select {
                case <-totalCounterChannel:
                        gossipCount += 1
                case <-uniqueCounterChannel:
                        uniqueGossipCount += 1
                        if uniqueGossipCount == nodeCount {
                                doneChannel <- true
                        }
                }
        }
}

func calculateStats(test_gossip Gossip) {
        fmt.Printf("******** [%s] ********\n", current_func)
        fmt.Printf("Node Count: %d\nGossipee Count: %d\nBounce Limit: %d\nPath Limit: %d\n", nodeCount, gossipeeCount, bounceLimit, pathLimit)
        missing_updates := 0
        path_lens := []int{}
        max_path_len := 0
        for _, val := range node_map {
                if val["test_key"].TS != test_gossip.TS {
                        missing_updates += 1
                }
                path_len := len(val["test_key"].Path)
                if path_len > max_path_len {
                        max_path_len = path_len
                }
                path_lens = append(path_lens, path_len)
        }
        sort.Ints(path_lens)

        fmt.Printf("Max Path Length: %d\n", path_lens[len(path_lens)-1])
        fmt.Printf("Med Path Length: %d\n", path_lens[int(len(path_lens)/2)])
        fmt.Printf("Missing Updates: %d\n", missing_updates)
        fmt.Printf("Total Gossip: %d\n", gossipCount)
        fmt.Printf("Unique Gossip: %d\n", uniqueGossipCount)
        fmt.Printf("Multiplier: %d\n", gossipCount/nodeCount)
}
