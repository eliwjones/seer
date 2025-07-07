package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ChannelMessage struct {
	Destination string
	Message     Gossip
}

type Gossip struct {
	Key    string
	Path   []int64
	TS     int64
	Bounce int
}

var (
	sentCounterChannel     chan int
	receivedCounterChannel chan int
	uniqueCounterChannel   chan int
	counterQuitChannel     chan bool
	gossipSentCount        int
	gossipReceivedCount    int
	uniqueGossipCount      int
	currentFunc            string

	doneChannel  = make(chan bool, 10)
	nodeMap      = map[string]map[string]Gossip{}
	nodeChannels = map[int]chan ChannelMessage{}
	GossipFunc   = map[string]func(string, Gossip){}

	// Flag Vars
	nodeCount     int
	pathLimit     int
	bounceLimit   int
	gossipeeCount int
	messageLoss   int
)

func init() {
	// Bind to flags if they are passed.
	flag.IntVar(&nodeCount, "nodecount", 500, "How many nodes are there?")
	flag.IntVar(&gossipeeCount, "gossipeecount", int(math.Log2(float64(nodeCount))), "How many nodes should gossiper gossip with?")
	flag.IntVar(&pathLimit, "pathlimit", 7, "How many nodes can a gossip message pass through?")
	flag.IntVar(&bounceLimit, "bouncelimit", 0, "How many times can an 'old' gossip message be bounced around?")
	flag.IntVar(&messageLoss, "messageloss", 0, "How many messages out of 100 will be lost?")

	// Init mapped GossipGossip funcs here since there does not appear to be a sexy way.
	GossipFunc["GossipGossip0"] = func(nodeKey string, gossip Gossip) {
		gossip.Path = append(gossip.Path, ExtractNodeIDX(nodeKey))
		if gossip.Bounce > bounceLimit {
			return
		}
		// Choose random hosts to gossip to.
		for i := 0; i < gossipeeCount; i++ {
			//Choose idx.  SendGossip.
			idx := int64(rand.Intn(len(nodeMap)))
			destNodeKey := fmt.Sprintf("node_%d", idx)
			SendGossip(destNodeKey, gossip)
		}
	}

	GossipFunc["GossipGossip1"] = func(nodeKey string, gossip Gossip) {
		gossip.Path = append(gossip.Path, ExtractNodeIDX(nodeKey))
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
				idx = int64(rand.Intn(len(nodeMap)))
				for _, val := range gossip.Path {
					if idx == val {
						ready = false
					}
				}
			}
			destNodeKey := fmt.Sprintf("node_%d", idx)
			SendGossip(destNodeKey, gossip)
		}
	}

	GossipFunc["GossipGossip2"] = func(nodeKey string, gossip Gossip) {
		gossip.Path = append(gossip.Path, ExtractNodeIDX(nodeKey))
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
			destNodeKey := fmt.Sprintf("node_%d", i)
			// draw rand to see if should send.
			rand := rand.Intn(nodeCount)
			if rand <= gossipeeCount {
				SendGossip(destNodeKey, gossip)
			}
		}
	}

	GossipFunc["GossipGossip3"] = func(nodeKey string, gossip Gossip) {
		if gossip.Bounce > bounceLimit {
			return
		}
		// Manually inflating pathLimit since this func explodes path size.
		if len(gossip.Path) > pathLimit*gossipeeCount {
			return
		}
		// Choose random hosts to gossip to.
		// But, build total seerpath of all peers sending to before sending any.
		gossipees := make([]int64, 0, gossipeeCount)
		for i := 0; i < gossipeeCount; i++ {
			//Choose idx.  SendGossip.
			ready := false
			idx := int64(0)
			for !ready {
				ready = true
				idx = int64(rand.Intn(len(nodeMap)))
				for _, val := range gossip.Path {
					if idx == val {
						ready = false
					}
				}
			}
			gossip.Path = append(gossip.Path, idx)
			gossipees = append(gossipees, idx)
		}
		for _, nodeIdx := range gossipees {
			destNodeKey := fmt.Sprintf("node_%d", nodeIdx)
			SendGossip(destNodeKey, gossip)
		}
	}
}

func main() {
	flag.Parse()

	test_gossip := Gossip{Key: "test_key", TS: int64(99)}
	channelCount := 10

	for fnName := range GossipFunc {
		currentFunc = fnName

		var wg sync.WaitGroup
		initChannels(channelCount, nodeCount, &wg)
		initNodes(nodeCount)
		initCounters()

		// Percolate update gossip.
		GossipFunc[currentFunc]("node_0", test_gossip)
		loops := 0
		for {
			select {
			case <-doneChannel:
				goto calculate
			case <-time.After(time.Duration(1) * time.Second):
				if uniqueGossipCount > int(0.97*float64(nodeCount)) || loops > 3 {
					goto calculate
				}
				loops += 1
			}
		}
	calculate:
		// Close our channels so our goroutines shutdown.
		for _, nodeChannel := range nodeChannels {
			close(nodeChannel)
		}

		counterQuitChannel <- true

		wg.Wait()

		calculateStats(test_gossip)
	}
}

func ProcessGossip(nodeKey string, gossip Gossip) {
	if nodeMap[nodeKey][gossip.Key].TS < gossip.TS {
		nodeMap[nodeKey][gossip.Key] = gossip
		uniqueCounterChannel <- 1
	} else {
		gossip.Bounce += 1
	}
	GossipFunc[currentFunc](nodeKey, gossip)
}

func SendGossip(nodeKey string, gossip Gossip) {
	// send Gossip down nodeChannels[nodeKey]
	sentCounterChannel <- 1
	// Simulate messageloss if set.
	if messageLoss > 0 && rand.Intn(100) < messageLoss {
		return
	}
	nodeModulus := int(ExtractNodeIDX(nodeKey)) % len(nodeChannels)
	nodeChannels[nodeModulus] <- ChannelMessage{Destination: nodeKey, Message: gossip}
}

func ReceiveGossip(nodeModulus int, wg *sync.WaitGroup) {
	defer wg.Done()

	// Gossip comes down channel.  Take it and update nodeMap accordingly.
	for channelMessage := range nodeChannels[nodeModulus] {
		receivedCounterChannel <- 1
		ProcessGossip(channelMessage.Destination, channelMessage.Message)
	}
}

func ExtractNodeIDX(nodeKey string) int64 {
	nodeParts := strings.Split(nodeKey, "_")
	idx, _ := strconv.ParseInt(nodeParts[1], 10, 64)
	return idx
}

func initNodes(nodeCount int) {
	nodeMap = map[string]map[string]Gossip{}
	for i := 0; i < nodeCount; i++ {
		nodeKey := fmt.Sprintf("node_%d", i)
		nodeMap[nodeKey] = map[string]Gossip{}
	}
}

func initCounters() {
	gossipSentCount = 0
	gossipReceivedCount = 0
	uniqueGossipCount = 0
}

func initChannels(channelCount int, nodeCount int, wg *sync.WaitGroup) {
	nodeChannels = map[int]chan ChannelMessage{}
	for i := 0; i < channelCount; i++ {
		// Multiplier (message redundancy) is usually under 20 so..
		nodeChannels[i] = make(chan ChannelMessage, 20*int(nodeCount/channelCount))
	}
	for i := 0; i < channelCount; i++ {
		wg.Add(1)
		go ReceiveGossip(i, wg)
	}
	sentCounterChannel = make(chan int, 1000)
	receivedCounterChannel = make(chan int, 1000)
	uniqueCounterChannel = make(chan int, 1000)
	counterQuitChannel = make(chan bool, channelCount)

	go gossipCounter()
}

func gossipCounter() {
	for {
		select {
		case <-sentCounterChannel:
			gossipSentCount += 1
		case <-receivedCounterChannel:
			gossipReceivedCount += 1
		case <-uniqueCounterChannel:
			uniqueGossipCount += 1
		case <-counterQuitChannel:
			return
		}
		if uniqueGossipCount >= nodeCount && gossipSentCount == gossipReceivedCount {
			doneChannel <- true
		}
	}
}

func calculateStats(test_gossip Gossip) {
	fmt.Printf("******** [%s] ********\n", currentFunc)
	fmt.Printf("Node Count: %d\nGossipee Count: %d\nBounce Limit: %d\nPath Limit: %d\n", nodeCount, gossipeeCount, bounceLimit, pathLimit)
	missing_updates := 0
	path_lens := []int{}
	max_path_len := 0
	for _, val := range nodeMap {
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
	fmt.Printf("Total Gossip: %d\n", gossipSentCount)
	fmt.Printf("Unique Gossip: %d\n", uniqueGossipCount)
	fmt.Printf("Multiplier: %d\n", gossipSentCount/nodeCount)
}
