package main

import (
        "code.google.com/p/go.net/ipv4"

        "archive/tar"
        "bytes"
        "compress/gzip"
        "encoding/json"
        "errors"
        "flag"
        "fmt"
        "io"
        "io/ioutil"
        "log"
        "math"
        "math/rand"
        "net"
        "net/http"
        "os"
        "os/signal"
        "regexp"
        "runtime"
        "sort"
        "strconv"
        "strings"
        "time"
)

type Gossip struct {
        SeerAddr    string
        ServiceName string
        ServiceAddr string
        SeerRequest string
        SeerPath    []string
        Tombstone   bool
        TS          int64

        Metadata json.RawMessage
        ReGossip bool
}

var (
        udpAddress        string
        tcpAddress        string
        gossipSocket      *net.UDPConn
        lastSeedTS        int64
        gossipReceived    chan string
        gossipCount       int
        uniqueGossipCount int

        /* Defined variables go here. */
        BoolChannels    = map[string]chan bool{}
        SeerDirs        = map[string]string{}
        neighborhood    = -1
        commandList     = map[string]bool{"exit": true, "get": true}
        seerNearness    = map[string]int{}
        whitelist       = map[string]bool{}
        tsRegexp        = regexp.MustCompile(`(,?\s*)("TS"\s*:\s*)(\d+)(\s*[,|}])`)
        seerPathRegexp  = regexp.MustCompile(`(\s*)("SeerPath"\s*:\s*\[)(.+?)(\]\s*)`)
        tombstoneRegexp = regexp.MustCompile(`(\s*)("Tombstone"\s*:\s*)(true)(\s*)`)

        /* Flags go here */
        hostIP          = flag.String("ip", "", "REQUIRED! IP address to communicate with other Seers from.")
        udpPort         = flag.String("udp", "9999", "<port> to use for UDP server. Default is: 9999")
        tcpPort         = flag.String("tcp", "9998", "<port> to use for HTTP server.  Default is: 9998")
        bootstrap       = flag.String("bootstrap", "", "<host>:<udp port> for Seer Host to request seed from. Use -bootstrap=magic to search.")
        listenBroadcast = flag.Bool("listenbroadcast", true, "Can disable listening to broadcast UDP packets.  Useful for testing multiple IPs on same machine.")
        raiseTheDead    = flag.Bool("raisethedead", false, "Will gossip out that all previously Tombstone-d services are now up.")

        /* Testing Flags */
        messageLoss   = flag.Int("messageloss", 0, "Use to simulate percent message loss. e.g. --messageloss=10 implies 10% dropped messages.")
        messageDelay  = flag.Int("messagedelay", 0, "Use to simulate millisecond delay in communication between Seers. e.g. --messagedelay=100 waits 100ms before sending.")
        logCounts     = flag.Bool("logcounts", false, "Bool flag for locally logging number of SendGossip calls.  Can then aggregate total messages sent.  e.g. --logcounts=true")
        neighborhoods = flag.Int("neighborhoods", -1, "For defining number of neighborhoods. If --neighborhoods=3, for Seer hostIPs = X.Y.Z.W,  W%3 defines which neighborhood a Seer belongs to.")
        whitelistFlag = flag.String("whitelist", "", "Comma delimited list of whitelisted Seer hostIPs.  Thus, seer can communicate with whitelisted host even if it is not in its neighborhood.")
)

func init() {
        rand.Seed(time.Now().UTC().UnixNano())
        runtime.GOMAXPROCS(int(runtime.NumCPU() / 2))
        flag.Parse()

        if *hostIP == "" {
                fmt.Printf("Please pass -ip=W.X.Y.Z\n")
                os.Exit(1)
        }

        ipParts := strings.Split(*hostIP, ".")
        if len(ipParts) != 4 {
                fmt.Printf("Please pass -ip=W.X.Y.Z\nI received: %v\n", ipParts)
                os.Exit(1)
        }
        if *neighborhoods > -1 {
                var err error
                neighborhood, _, err = GetNeighborhoodAndSeerIP(*hostIP, *neighborhoods)
                if err != nil {
                        fmt.Printf("[GetNeighborhood] Error: %v\nNeighborhood: %v\n", err, neighborhood)
                        os.Exit(1)
                }
                fmt.Printf("NEIGHBORHOOD: %v\n", neighborhood)
        }

        for _, whitelistIP := range strings.Split(*whitelistFlag, ",") {
                whitelist[whitelistIP] = true
        }

        if *logCounts {
                BoolChannels["gossipCounter"] = make(chan bool, 100)
                BoolChannels["uniqueGossipCounter"] = make(chan bool, 100)

        }
        gossipCount = 0
        BoolChannels["seerReady"] = make(chan bool, 1)
        gossipReceived = make(chan string, 100)

        udpAddress = *hostIP + ":" + *udpPort
        tcpAddress = *hostIP + ":" + *tcpPort

        /* Guarantee folder paths */
        SeerDirs["root"] = "seer/" + udpAddress
        SeerDirs["op"] = SeerDirs["root"] + "/gossip/oplog"
        SeerDirs["data"] = SeerDirs["root"] + "/gossip/data"
        SeerDirs["metadata"] = SeerDirs["root"] + "/gossip/metadata"

        os.MkdirAll(SeerDirs["op"], 0777)
        os.MkdirAll(SeerDirs["data"], 0777)
}

func main() {
        signaler := make(chan os.Signal, 1)
        signal.Notify(signaler, os.Interrupt, os.Kill)

        /* Single cleanup on start. */
        //TombstoneReaper()
        //AntiEntropy()

        go UpdateSeerNearness()
        go UDPServer(*hostIP, *udpPort)
        go ServiceServer(tcpAddress)

        /*
           Not really sure how want to build this out.
           Sort of annoying since need to wait for seed
           to announce self to peers.
        */
        createGossipSocket()
        /* Must wait for UDPServer to be ready. */
        ready := <-BoolChannels["seerReady"]
        if !ready {
                fmt.Println("UDPServer NOT READY!")
                os.Exit(1)
        }

        if *bootstrap != "" {
                BootStrap(*bootstrap, tcpAddress)
                ready = <-BoolChannels["seerReady"]
                fmt.Printf("[Bootstrap] Received seerReady from processSeed(): %v\n", ready)
                if !ready {
                        fmt.Println("How am I not ready?")
                        os.Exit(1)
                }
        }

        /* Presumably should be in working state with full set of peers.  So, announce. */
        HowAmINotMyself()

        /* Run background cleanup on 10 second cycle. */
        //BackgroundLoop("Janitorial Work", 10, TombstoneReaper, AntiEntropy)

        /* Run background routine for GossipOps() */
        //BackgroundLoop("Gossip Oplog", 1, GossipOps)

        if *raiseTheDead {
                /* If bootstrapping, might want to wait for seed before doing this. */
                RaiseServicesFromTheDead(udpAddress)
        }
        for {
                select {
                case <-BoolChannels["gossipCounter"]:
                        gossipCount += 1
                        fmt.Printf("[gossipCounter] gossip count: %d\n", gossipCount)
                case <-BoolChannels["uniqueGossipCounter"]:
                        uniqueGossipCount += 1
                case ready := <-BoolChannels["seerReady"]:
                        if !ready {
                                /* Someone sent false to seerReady, so shut down. */
                                fmt.Println("[seerReady] Seer is un-ready. Shutting down.")
                                os.Exit(1)
                        }
                case mySignal := <-signaler:
                        fmt.Println("Got Signal: ", mySignal)
                        TombstoneServices(udpAddress)
                        os.Exit(1)
                }
        }
}

func createGossipSocket() {
        c, err := net.ListenPacket("udp4", ":0")
        if err != nil {
                fmt.Printf("[createGossipSocket] ERR: %s\n", err)
                os.Exit(1)
        }
        gossipSocket = c.(*net.UDPConn)
}

func SendGossip(gossip string, seerAddr string) {
        seer, err := net.ResolveUDPAddr("udp4", seerAddr)
        if err != nil {
                fmt.Printf("[SendGossip] ERR: %s\n", err)
                return
        }
        _, err = ExtractTSFromJSON(gossip)
        if err != nil && err.Error() == "no TS" {
                newTs := fmt.Sprintf(`,"TS":%d}`, MS(time.Now()))
                gossip = gossip[:len(gossip)-1] + newTs
        }
        seerPath, err := ExtractSeerPathFromJSON(gossip, false)
        if err != nil && err.Error() == "no SeerPath" {
                seerPath = fmt.Sprintf(`,"SeerPath":["%s"]}`, udpAddress)
                gossip = gossip[:len(gossip)-1] + seerPath
        } else if strings.LastIndex(seerPath, udpAddress) == -1 {
                /* Do not want to add myself to myself. */
                gossip = UpdateSeerPath(gossip, `"`+udpAddress+`"`)
        }
        fmt.Printf("[SendGossip] gossip: %s\n    TO: %s\n", gossip, seer)
        gossipSocket.WriteToUDP([]byte(gossip), seer)
}

func TombstoneServices(seerAddress string) {
        /* For all my services, Gossip out Tombstones. */
        myServices, _ := getGossipArray(seerAddress, "host", "data")
        for _, service := range myServices {
                gossip := RemoveTombstone(RemoveSeerPath(UpdateTS(service)))
                gossip = fmt.Sprintf(`%s,"Tombstone":true}`, gossip[:len(gossip)-1])
                ProcessGossip(gossip, *hostIP, *hostIP)
        }
}

func RaiseServicesFromTheDead(seerAddress string) {
        myServices, _ := getGossipArray(seerAddress, "host", "data")
        for _, service := range myServices {
                gossip := RemoveTombstone(RemoveSeerPath(UpdateTS(service)))
                ProcessGossip(gossip, *hostIP, *hostIP)
        }
}

func HowAmINotMyself() {
        gossip := fmt.Sprintf(`{"SeerAddr":"%s","TS":%d}`, udpAddress, MS(time.Now()))
        /* Gossip self to self which will then get GossipGossip-ed. */
        ProcessGossip(gossip, *hostIP, *hostIP)
}

func BootStrap(seeder string, seedee string) {
        /* Sexier */
        if seeder == "magic" {
                /* Broadcast to whoever.  Must still handle handshake so that not all Seers send their data. */
                seeder = "255.255.255.255:9999"
        }
        if strings.HasPrefix(seeder, "255.") {
                /* If Broadcasting, must send udpAddress so they can send back "SeedYou" query. */
                seedee = udpAddress
        }
        message := fmt.Sprintf(`{"SeerAddr":"%s","SeerRequest":"SeedMe"}`, seedee)
        SendGossip(message, seeder)
}

func FreshGossip(filePath string, newTS int64) (bool, string) {
        serviceData, err := ioutil.ReadFile(filePath)
        if err != nil {
                fmt.Printf("[FreshGossip] Could not read existing gossip. filePath: [%s]\n", filePath)
                return true, ""
        }
        currentTS, err := ExtractTSFromJSON(string(serviceData))
        if err != nil {
                fmt.Printf("[FreshGossip] Not TS found. serviceData: [%s]\n", string(serviceData))
        }
        return newTS > currentTS, string(serviceData)
}

func ExtractSeerPathFromJSON(gossip string, trimquotes bool) (string, error) {
        seerPath := seerPathRegexp.FindStringSubmatch(gossip)
        if len(seerPath) == 5 {
                if trimquotes {
                        seerPath[3] = strings.Replace(seerPath[3], `"`, ``, -1)
                }
                return seerPath[3], nil
        }
        return "", errors.New("no SeerPath")
}

func UpdateSeerPath(gossip string, newpathitems string) string {
        if newpathitems != "" {
                gossip = seerPathRegexp.ReplaceAllString(gossip, `${1}${2}`+newpathitems+`,${3}${4}`)
        }
        return gossip
}

func RemoveSeerPath(gossip string) string {
        /* Guess this remove can be slow since happens infrequently. */
        gossip = seerPathRegexp.ReplaceAllString(gossip, ``)
        for _, replaceme := range []string{`{,`, `,,`, `,}`} {
                with := strings.Replace(replaceme, `,`, ``, 1)
                gossip = strings.Replace(gossip, replaceme, with, 1)
        }
        return gossip
}

func RemoveTombstone(gossip string) string {
        /* Infrequent, so can be "slow". */
        gossip = tombstoneRegexp.ReplaceAllString(gossip, ``)
        for _, replaceme := range []string{`{,`, `,,`, `,}`} {
                with := strings.Replace(replaceme, `,`, ``, 1)
                gossip = strings.Replace(gossip, replaceme, with, 1)
        }
        return gossip
}

func ExtractTSFromJSON(gossip string) (int64, error) {
        /* Looks for "TS" in JSON like:
           {"TS":<numbers>,...}
           {...,"TS":<numbers>}
           {...,"TS":<numbers>,...}
        */
        ts := tsRegexp.FindStringSubmatch(gossip)
        if len(ts) == 5 {
                return strconv.ParseInt(ts[3], 10, 64)
        }
        return 0, errors.New("no TS")
}

func UpdateTS(gossip string) string {
        newTS := fmt.Sprintf("%d", MS(Now()))
        return tsRegexp.ReplaceAllString(gossip, `${1}${2}`+newTS+`${4}`)
}

func ChooseNFromArray(n int, array []string) []string {
        m := len(array)
        if m > 4*n {
                return ChooseNFromArrayNonDeterministically(n, array)
        }
        indexArray := make([]int, m)
        for i := 0; i < m; i++ {
                indexArray[i] = i
        }
        intSlice := sort.IntSlice(indexArray)
        /* Do the Knuth shuffle. */
        /* Can this really be better than randomly choosing over and over? */
        i := m
        for i > 0 {
                i = i - 1
                j := rand.Intn(i + 1)
                intSlice.Swap(i, j)
        }
        tracker := map[string]bool{}
        counter := 0
        chosenItems := make([]string, 0, n)
        for _, arrayIndex := range intSlice {
                item := array[arrayIndex]
                if tracker[item] {
                        continue
                }
                chosenItems = append(chosenItems, item)
                tracker[item] = true
                counter += 1
                if counter == n {
                        break
                }
        }
        return chosenItems
}

func ChooseNFromArrayNonDeterministically(n int, array []string) []string {
        chosenItems := make([]string, 0, n)
        tracker := map[string]bool{}
        counter := 0
        tries := 0
        m := len(array)
        for counter < n {
                tries += 1
                if tries > MaxInt(m, 4*n) {
                        /* Do not wish to try forever. */
                        break
                }
                item := array[rand.Intn(m)]
                if tracker[item] {
                        continue
                }
                chosenItems = append(chosenItems, item)
                tracker[item] = true
                counter += 1
        }
        return chosenItems
}

func GossipGossip(gossip string) {
        fmt.Printf("[GossipGossip] Gossiping Gossip: %s\n", gossip)
        seerPath, _ := ExtractSeerPathFromJSON(gossip, true)
        seerPeers := GetSeerPeers(udpAddress, seerPath)
        if len(seerPeers) == 0 {
                fmt.Println("[GossipGossip] No peers! No one to gossip with.")
                return
        }
        gossipees := int(math.Log2(float64(len(seerPeers)))) + 1
        fmt.Printf("Gossipees: %d\nSeerPeers: %v\n", gossipees, seerPeers)

        /*
           Normalizing by nearness.  Increases the probability that Seer Peer will try to message a "Hub" that it is "touching".
           And, decreases probability that Seer Peer will try to message a peer in another neighborhood.

           Highly likely this is useless being baked in.  Probably want vanilla random selection, and then handle special case of "Hub" peers.
           Mainly, if the topology is broken out into sub-neighborhoods, what is the value of each neighborhood knowing about the other?
           Presumably, the peers cannot see across the neighborhoods.. so.. why should they attempt to be aware of eachother's services?

           More useful for a generalized key-value store.
        */
        normalizedSeerPeers := NormalizeSeerPeersByNearness(seerPeers)
        randSeerPeers := ChooseNFromArray(gossipees, normalizedSeerPeers)
        for _, seerPeer := range randSeerPeers {
                SendGossip(gossip, seerPeer)
        }
}

func GetSeerPeers(filters ...string) []string {
        seerHostDir, err := os.Open(SeerDirs["data"] + "/host")
        if err != nil {
                fmt.Printf("[GetSeerPeers] ERR: %s\n", err)
                return []string{}
        }

        seerPeers, err := seerHostDir.Readdirnames(-1)
        seerHostDir.Close()
        if err != nil {
                fmt.Printf("[GetSeerPeers] ERR: %s\n", err)
                return []string{}
        }
        seerMap := map[string]bool{}
        for _, seerPeer := range seerPeers {
                seerMap[seerPeer] = true
        }
        for _, filter := range filters {
                for _, seerPeer := range strings.Split(filter, ",") {
                        delete(seerMap, seerPeer)
                }
        }
        seerPeers = make([]string, 0, len(seerMap))
        for seerPeer, _ := range seerMap {
                peer := true
                /* Check neighborhood and whitelist */
                if neighborhood > -1 {
                        nhbd, seerIP, _ := GetNeighborhoodAndSeerIP(seerPeer, *neighborhoods)
                        if nhbd != neighborhood {
                                peer = false
                        }
                        /* Guess could be point in future where whitelist will need to exist outside Neighborhood check. */
                        if whitelist[seerIP] {
                                peer = true
                        }
                }
                if peer {
                        seerPeers = append(seerPeers, seerPeer)
                }
        }
        return seerPeers
}

func NormalizeSeerPeersByNearness(seerPeers []string) []string {
        /* Most likely plenty of more-optimal approaches, but this feels most immediately clear. */
        minval := 0
        for _, seerPeer := range seerPeers {
                minval = MinInt(minval, seerNearness[seerPeer])
        }
        normalizedPeers := make([]string, 0, len(seerPeers))
        for _, seerPeer := range seerPeers {
                indexMultiplier := 1 + AbsInt(minval) + seerNearness[seerPeer]
                /* "Least Near" peer will only have its index appear once. */
                for i := 0; i < indexMultiplier; i++ {
                        normalizedPeers = append(normalizedPeers, seerPeer)
                }
        }
        return normalizedPeers

}

func UpdateSeerNearness() {
        for {
                select {
                case gossip := <-gossipReceived:
                        seerPeers, _ := ExtractSeerPathFromJSON(gossip, true)
                        for index, seerPeer := range strings.Split(seerPeers, ",") {
                                if index > 1 || seerPeer == "" {
                                        continue
                                } else if index == 0 {
                                        seerNearness[seerPeer] += 1
                                } else if index == 1 {
                                        seerNearness[seerPeer] -= 1
                                }
                        }
                        fmt.Printf("[UpdateSeerConnectivity] seerNearness: %#v\n", seerNearness)
                }
        }
}

func GetNeighborhoodAndSeerIP(seerIP string, nhbds int) (int, string, error) {
        portIndex := strings.Index(seerIP, ":")
        if portIndex > -1 {
                seerIP = seerIP[:portIndex]
        }
        ipParts := strings.Split(seerIP, ".")
        nbhdPart, err := strconv.ParseInt(ipParts[3], 10, 0)
        nhbd := int(nbhdPart) % nhbds
        return nhbd, seerIP, err
}

func ProcessSeerRequest(decodedGossip Gossip, destinationIp string) {
        if decodedGossip.SeerRequest == "SeedMe" && strings.HasPrefix(destinationIp, "255.") {
                /* If receive broadcast, send back "SeedYou" query. */
                if udpAddress == decodedGossip.SeerAddr {
                        /* No need to seed oneself. */
                        /* Might be sexier to have SendGossip() block gossip to self? */
                        return
                }
                message := fmt.Sprintf(`{"SeerAddr":"%s","SeerRequest":"SeedYou"}`, udpAddress)
                SendGossip(message, decodedGossip.SeerAddr)
        } else if decodedGossip.SeerRequest == "SeedMe" {
                /* Received direct "SeedMe" request.  Send seed. */
                sendSeed(decodedGossip.SeerAddr)
        } else if decodedGossip.SeerRequest == "SeedYou" {
                /* Seer thinks I want a Seed.. do I? */
                if time.Now().UnixNano() >= lastSeedTS+1000000000*30 {
                        fmt.Printf("[SeedYou] Requesting Seed!\n  NOW - THEN = %s", time.Now().UnixNano()-lastSeedTS)
                        message := fmt.Sprintf(`{"SeerAddr":"%s","SeerRequest":"SeedMe"}`, tcpAddress)
                        SendGossip(message, decodedGossip.SeerAddr)
                } else {
                        fmt.Printf("[SeedYou] No need to seed!\n  NOW - THEN = %s", time.Now().UnixNano()-lastSeedTS)
                }
        } else if decodedGossip.SeerRequest == "RequestMetadata" {
                /* For all peers, send {"SeerAddr":udpAddress,"SeerRequest":"Metadata"} */
                requestMetadata()
                /* Generate and process my own metadata. */
                metadata := generateMetadata()
                message := fmt.Sprintf(`{"SeerAddr":"%s","ServiceName":"Seer","Metadata":%s,"TS":%d}`, udpAddress, metadata, MS(time.Now()))
                ProcessGossip(message, *hostIP, *hostIP)
        } else if decodedGossip.SeerRequest == "Metadata" {
                metadata := generateMetadata()
                message := fmt.Sprintf(`{"SeerAddr":"%s","ServiceName":"Seer","Metadata":%s}`, udpAddress, metadata)
                SendGossip(message, decodedGossip.SeerAddr)
        }
        return
}

func ProcessGossip(gossip string, sourceIp string, destinationIp string) {
        /* Implement simulated Message Loss and Delays here. */
        if *messageLoss > 0 && rand.Intn(100) < *messageLoss {
                fmt.Printf("[ProcessGossip] Simulated message loss of %d%\n", *messageLoss)
                return
        }
        if *messageDelay > 0 {
                fmt.Printf("[ProcessGossip] messagedelay of %dms\n", *messageDelay)
                time.Sleep(time.Duration(*messageDelay) * time.Millisecond)
        }
        /* Also, log gossip counts here?  Or at server? */
        if *logCounts {
                /* Inc some global counter? */
                BoolChannels["gossipCounter"] <- true
        }
        decodedGossip, err := VerifyGossip(gossip)

        if decodedGossip.SeerRequest != "" {
                ProcessSeerRequest(decodedGossip, destinationIp)
                return
        }
        if err != nil || decodedGossip.SeerAddr == "" {
                fmt.Printf("\nBad Gossip!: %s\nErr: %s\nDestination: %s\n", gossip, err, destinationIp)
                return
        }
        /* Write to oplog. opName limits updates from single host to 10 per nanosecond.. */
        opName := fmt.Sprintf("%d_%s_%d", time.Now().UnixNano(), decodedGossip.SeerAddr, (rand.Int()%10)+10)
        _ = LazyWriteFile(SeerDirs["op"], opName+".op", []byte(gossip))
        /* Save it. */
        put, fresherGossip := PutGossip(gossip, decodedGossip)

        gossipReceived <- gossip

        /* put == true implies gossip was "fresh".  Thus, spread the good news. */
        if put {
                if *logCounts {
                        BoolChannels["uniqueGossipCounter"] <- true
                }
                /* Spread the word? Or, use GossipOps()? */
                GossipGossip(gossip)
        } else if fresherGossip != "" && !strings.Contains(gossip, `"ReGossip":true`) {
                /* Merge SeerPaths and re-gossip. */
                gossip = gossip[:len(gossip)-1] + `,"ReGossip":true}`
                seerPath, _ := ExtractSeerPathFromJSON(fresherGossip, false)
                gossip = UpdateSeerPath(gossip, seerPath)
                GossipGossip(gossip)
        }
}

func VerifyGossip(gossip string) (Gossip, error) {
        /*
           { "SeerAddr" : "remotehost1:9999",
             "ServiceName" : "mongod",
             "ServiceAddr" : "remotehost1:2107",
             "TS" : 123312123123 }
        */
        var g Gossip
        err := json.Unmarshal([]byte(gossip), &g)

        /* Reflection feels stupid, so bruteforcing it. */
        /* A Heartbeat is just {SeerAddr,ts}.. so that's all need check. */
        errorStr := ""
        if g.SeerAddr == "" {
                errorStr += ",SeerAddr"
        }
        if g.TS == 0 {
                errorStr += ",TS"
        }
        if err == nil && errorStr != "" {
                err = errors.New(fmt.Sprintf("[%s] are missing!", errorStr[1:len(errorStr)]))
        }
        return g, err
}

func PutGossip(gossip string, decodedGossip Gossip) (bool, string) {
        /* Only checking SeerServiceDir for current TS. */
        /* Current code has odd side effect of allowing Seer data to exist for host even if we missed initial Seer HELO gossip. */
        /* We leave it to AntiEntropy to patch that up. */
        if decodedGossip.ServiceName == "" {
                decodedGossip.ServiceName = "Seer"
        }
        gossipType := "data"
        if decodedGossip.Metadata != nil {
                gossipType = "metadata"
        }
        fresh, currentGossip := FreshGossip(SeerDirs[gossipType]+"/service/"+decodedGossip.ServiceName+"/"+decodedGossip.SeerAddr, decodedGossip.TS)
        if !fresh {
                fmt.Printf("[Old Assed Gossip] I ain't writing that.\n[Gossip]: %s\n", gossip)
                return false, currentGossip
        }
        _ = LazyWriteFile(SeerDirs[gossipType]+"/service/"+decodedGossip.ServiceName, decodedGossip.SeerAddr, []byte(gossip))
        _ = LazyWriteFile(SeerDirs[gossipType]+"/host/"+decodedGossip.SeerAddr, decodedGossip.ServiceName, []byte(gossip))
        if gossipType == "metadata" {
                /* For now, don't want metadata gossipped all over the place. */
                return false, ""
        }
        return true, ""
}

func LazyWriteFile(folderName string, fileName string, data []byte) error {
        err := ioutil.WriteFile(folderName+"/"+fileName, data, 0777)
        if err != nil {
                os.MkdirAll(folderName, 0777)
                err = ioutil.WriteFile(folderName+"/"+fileName, data, 0777)
        }
        if err != nil {
                fmt.Printf("[LazyWriteFile] Could not WriteFile: %s\nErr: %s\n", folderName+"/"+fileName, err)
        }
        return err
}

func GossipOps() {
        /*
           1. Pick N random peers.
           2. Gossip all ops that have occured since last GossipOps() to peers.
           3. Ops examined by file create date and encoded "ts"?
        */
        fmt.Printf("[GossipOps] I grab all (plus some padding?) ops that have appeared since last GossipOps()")
}

/*******************************************************************************
    UDP Server
*******************************************************************************/

func UDPServer(ipAddress string, port string) {
        /* If UDPServer dies, I don't want to live anymore. */
        defer func() { BoolChannels["seerReady"] <- false }()

        /*
           "Have" to hack this since want to receive broadcast packets.. yet.. they don't appear to show up
           if net.ListenPacket() gets called on specific IP address?
           Really annoying since prevents listening on same port using different IP addresses on same machine.
           Thus, I have added more hack.
        */
        var err error
        var c net.PacketConn

        if *listenBroadcast {
                c, err = net.ListenPacket("udp", ":"+port)
        } else {
                c, err = net.ListenPacket("udp", udpAddress)
        }
        if err != nil {
                log.Fatal(err)
        }
        defer c.Close()
        udpLn := ipv4.NewPacketConn(c)
        err = udpLn.SetControlMessage(ipv4.FlagDst, true)
        if err != nil {
                log.Fatal(err)
        }
        /* Assuming that MTU can't be below 576, which implies one has 508 bytes for payload after overhead. */
        udpBuff := make([]byte, 508)
        BoolChannels["seerReady"] <- true
        for {
                n, cm, _, err := udpLn.ReadFrom(udpBuff)
                if err != nil {
                        log.Fatal(err)
                }
                if cm.Dst.String() == *hostIP || strings.HasPrefix(cm.Dst.String(), "255.") {
                        /* Only process broadcast traffic OR traffic destined for me. */
                        /* Only necessary when listenbroadcast=true */
                        message := strings.Trim(string(udpBuff[:n]), "\n")
                        if message == "exit" {
                                return
                        }
                        go ProcessGossip(message, cm.Src.String(), cm.Dst.String())
                }
        }
}

/*******************************************************************************
    HTTP related and seeding functions.
*******************************************************************************/

func ServiceServer(address string) {
        /* If HTTPServer dies, guess I don't want to live anymore either. */
        defer func() { BoolChannels["seerReady"] <- false }()

        http.HandleFunc("/", ServiceHandler)
        http.ListenAndServe(address, nil)
}

func errorResponse(w http.ResponseWriter, message string) {
        w.WriteHeader(http.StatusInternalServerError)
        w.Write([]byte(message))
}

func ServiceHandler(w http.ResponseWriter, r *http.Request) {
        requestTypeMap := map[string]string{
                "host":              "host",
                "seer":              "host",
                "service":           "service",
                "metadata":          "metadata",
                "data":              "data",
                "op":                "op",
                "aggregateMetadata": "aggregateMetadata",
                "generateMetadata":  "generateMetadata",
        }
        /* Allow GET by 'service' or 'seeraddr' */
        splitURL := strings.Split(r.URL.Path, "/")

        switch r.Method {
        case "GET":
                if requestTypeMap[splitURL[1]] == "" {
                        errorResponse(w, "Please query for 'seer', 'service', 'metadata', or 'op'")
                        return
                }
                jsonPayload := ""
                if splitURL[1] == "aggregateMetadata" {
                        /* Do thangs. */
                        aggregate, _ := json.MarshalIndent(aggregateMetadata(), "", "    ")
                        jsonPayload = string(aggregate) + "\n"
                } else if splitURL[1] == "generateMetadata" {
                        jsonPayload = generateMetadata()
                } else {
                        dataType := "data"
                        requestType := requestTypeMap[splitURL[1]]
                        name := splitURL[2]
                        if splitURL[1] == "metadata" || splitURL[1] == "data" {
                                if requestTypeMap[splitURL[2]] == "" {
                                        errorResponse(w, "Please query for 'seer', 'service', 'metadata', or 'op'")
                                        return
                                }
                                dataType = splitURL[1]
                                requestType = requestTypeMap[splitURL[2]]
                                name = splitURL[3]
                        }
                        jsonPayload = getServiceData(name, requestType, dataType)
                }
                fmt.Fprintf(w, jsonPayload)
        case "PUT":
                if splitURL[1] != "seed" {
                        errorResponse(w, "I only accept PUTs for 'seed'")
                }
                path := fmt.Sprintf("/tmp/seer_received_seed_%s_%d.tar.gz", udpAddress, MS(time.Now()))
                file, err := os.Create(path)
                if err != nil {
                        errorResponse(w, err.Error())
                        return
                }
                defer file.Close()
                _, err = io.Copy(file, r.Body)
                if err != nil {
                        errorResponse(w, err.Error())
                        return
                }
                /* Made it here.. now need to do go routine to unzip. */
                go processSeed(path)
        }
}

func getGossipArray(name string, requestType string, dataType string) ([]string, []time.Time) {
        /* dataType in ["data","metadata"] */

        servicePath := SeerDirs[dataType] + "/" + requestType + "/" + name
        if requestType == "op" {
                servicePath = SeerDirs["op"]
        }
        gossipFiles, _ := ioutil.ReadDir(servicePath)
        /* read files from servicePath, append to jsonPayload. */
        gossipArray := make([]string, 0, len(gossipFiles))
        modTimeArray := make([]time.Time, 0, len(gossipFiles))
        for _, gossipFile := range gossipFiles {
                if gossipFile.IsDir() {
                        continue
                }
                gossipData, err := ioutil.ReadFile(servicePath + "/" + gossipFile.Name())
                if err == nil {
                        gossipArray = append(gossipArray, string(gossipData))
                        modTimeArray = append(modTimeArray, gossipFile.ModTime())
                } else {
                        err := fmt.Sprintf("[getGossipArray] ERROR: %s\nFILE: %s", err, gossipFile.Name())
                        fmt.Println(err)
                }
        }
        return gossipArray, modTimeArray
}

func getServiceData(name string, requestType string, dataType string) string {
        /* Construct JSON Array.  */
        data, _ := getGossipArray(name, requestType, dataType)
        return fmt.Sprintf(`[%s]`, strings.Join(data, ","))
}

// Once receive UDP notification that seerDestinationAddr needs seed
// tar gz 'host' and 'service' dirs and PUT to seerDestinationAddr.
func sendSeed(seerDestinationAddr string) {
        tarredGossipFile := fmt.Sprintf("/tmp/seer_generated_seed_%s_%s.tar.gz", udpAddress, seerDestinationAddr)
        err := createTarGz(tarredGossipFile, SeerDirs["data"]+"/service", SeerDirs["data"]+"/host")
        if err != nil {
                fmt.Printf("[sendSeed] Failed to create tar.gz. ERR: %s\n", err)
                return
        }
        /* Open file */
        rbody, _ := os.Open(tarredGossipFile)
        /* Put file */
        request, err := http.NewRequest("PUT", "http://"+seerDestinationAddr+"/seed", rbody)
        if err != nil {
                fmt.Printf("[sendSeed] ERROR MAKING REQUEST: %s\n", err)
                return
        }
        stat, err := rbody.Stat()
        if err != nil {
                fmt.Printf("[sendSeed] ERROR WITH STAT(): %s\n", err)
                return
        } else {
                request.ContentLength = stat.Size()
        }
        response, err := http.DefaultClient.Do(request)
        fmt.Printf("[sendSeed] RESPONSE: %s\nERR: %s\n", response, err)
}

// Zips up seer/<hostname>/gossip/data/ (aka SeerDirs["data"])
// General usage is targeting 'host' and 'service' subfolders.
// Removes SeerDirs["data"] root for "easier" untarring into destination.
// TODO: make less stupid.
func createTarGz(tarpath string, folders ...string) error {
        for _, folder := range folders {
                if !strings.HasPrefix(folder, SeerDirs["data"]) {
                        return errors.New(fmt.Sprintf("folder: [%s] does not start with: [%s]", folder, SeerDirs["data"]))
                }
        }
        tarfile, err := os.Create(tarpath)
        if err != nil {
                return err
        }
        defer tarfile.Close()
        gw, err := gzip.NewWriterLevel(tarfile, gzip.BestCompression)
        if err != nil {
                return err
        }
        defer gw.Close()
        tw := tar.NewWriter(gw)
        defer tw.Close()
        walkme := ""
        for _, folder := range folders {
                /* How deep do we walk? Is this really worth doing just to avoid filepath.Walk()? */
                levelsdeep := 1
                dirs := []string{folder}
                for len(dirs) > 0 {
                        /* Pop directory to be walked from dirs array. */
                        walkme, dirs = dirs[len(dirs)-1], dirs[:len(dirs)-1]
                        fileinfos, err := ioutil.ReadDir(walkme)
                        if err != nil {
                                fmt.Printf("[createTarGz] Err: %s\n", err)
                                return err
                        }
                        for _, fileinfo := range fileinfos {
                                path := walkme + "/" + fileinfo.Name()
                                if fileinfo.IsDir() {
                                        if levelsdeep > 0 {
                                                dirs = append(dirs, path)
                                        }
                                        continue
                                }
                                header, err := tar.FileInfoHeader(fileinfo, path)
                                header.Name = path[len(SeerDirs["data"])+1:]
                                err = tw.WriteHeader(header)
                                if err != nil {
                                        fmt.Printf("Failed to write Tar Header. Err: %s\n", err)
                                        return err
                                }
                                /* Add file. */
                                file, err := os.Open(path)
                                if err != nil {
                                        fmt.Printf("Failed to open path: [%s] Err: %s\n", path, err)
                                        continue
                                }
                                _, err = io.Copy(tw, file)
                                if err != nil {
                                        fmt.Printf("Failed to copy file into tar: [%s] Err: %s\n", path, err)
                                        file.Close()
                                        continue
                                }
                                file.Close()
                        }
                        levelsdeep -= 1
                }
        }
        return err
}

func processSeed(targzpath string) {
        lastSeedTS = time.Now().UnixNano()
        fmt.Printf("[processSeed] lastSeedTS: %s\n", lastSeedTS)
        targzfile, err := os.Open(targzpath)
        if err != nil {
                lastSeedTS = 0
                return
        }
        defer targzfile.Close()
        gzr, err := gzip.NewReader(targzfile)
        if err != nil {
                lastSeedTS = 0
                return
        }
        defer gzr.Close()
        tr := tar.NewReader(gzr)
        for {
                hdr, err := tr.Next()
                if err == io.EOF {
                        break
                }
                if err != nil {
                        fmt.Printf("[processSeed] tr ERR: %s\n", err)
                        break
                }
                buf := bytes.NewBuffer(nil)
                io.Copy(buf, tr)
                fullpath := SeerDirs["data"] + "/" + hdr.Name
                filename, dir := GetFilenameAndDir(fullpath)
                err = LazyWriteFile(dir, filename, buf.Bytes())
                if err != nil {
                        fmt.Printf("[processSeed] ERRR: %s", err)
                }
        }
        /* Suppose might want a seedReceived channel. */
        BoolChannels["seerReady"] <- true
        return
}

/*******************************************************************************
    Background work related stuffs.
*******************************************************************************/

func BackgroundLoop(name string, seconds int, fns ...func()) {
        go func() {
                for {
                        time.Sleep(time.Duration(seconds) * time.Second)
                        fmt.Printf("\n[%s] : LOOPING : %s\n", time.Now(), name)
                        for _, f := range fns {
                                f()
                        }
                }
        }()
}

func AntiEntropy() {
        /*
           1. Wrapped by background jobs. (Sleep Q seconds then do.)
           2. Check random host gossip/source (or gossip/services) zip checksum.
           3. Sync if checksum no match.

        */
        fmt.Printf("[AntiEntropy] I grab fill copies of other host dbs occassionally.\n")
}

func TombstoneReaper() {
        /*
           1. Wrapped by background jobs. (Sleep Q seconds then do.)
           2. Remove anything but most recent doc that is older than P seconds.
           3. Remove remaining docs if they are Tombstoned and older than certain time.
        */
        fmt.Printf("[TombstoneReaper] I remove stuff that has been deleted or older timestamped source docs.\n")
}

/*******************************************************************************
    Metadata calculation
*******************************************************************************/

type Metadata struct {
        TS          []int64
        TSLag       []int64
        MessageData map[string]int
        PeerData    int
}

type MetadataAggregate struct {
        HostList    []string
        PeerCounts  []int
        MessageData map[string][]int

        TSLag   [][]int64
        TS      [][]int64
}

func requestMetadata() {
        /* For all peers, send {"SeerAddr":udpAddress,"SeerRequest":"Metadata"} */
        message := fmt.Sprintf(`{"SeerAddr":"%s","SeerRequest":"Metadata"}`, udpAddress)
        seerPeers := GetSeerPeers(udpAddress)
        for _, peer := range seerPeers {
                SendGossip(message, peer)
        }
}

func generateMetadata() string {
        var metadata Metadata
        metadata.TS = getStats(getSortedTSArray(udpAddress))
        metadata.TSLag = getStats(getSortedTSLagArray(udpAddress))
        ops, _ := getGossipArray("", "op", "data")
        metadata.MessageData = map[string]int{
                "GossipCount":       gossipCount,
                "UniqueGossipCount": uniqueGossipCount,
                "OpCount":           len(ops),
        }
        metadata.PeerData = len(GetSeerPeers(udpAddress))

        json, err := json.Marshal(metadata)
        if err != nil {
                return ""
        }
        return string(json)
}

func aggregateMetadata() MetadataAggregate {
        /* Aggregate TS, TSLag, MessageData and PeerData into global view. */
        aggregate := MetadataAggregate{}
        aggregate.MessageData = map[string][]int{}

        metadataGossip, _ := getGossipArray("Seer", "service", "metadata")
        for _, metadatumGossip := range metadataGossip {
                var g Gossip
                var metadatum Metadata
                _ = json.Unmarshal([]byte(metadatumGossip), &g)
                _ = json.Unmarshal([]byte(g.Metadata), &metadatum)

                aggregate.HostList = append(aggregate.HostList, g.SeerAddr)

                aggregate.PeerCounts = append(aggregate.PeerCounts, metadatum.PeerData)
                for _, keyName := range []string{"GossipCount", "UniqueGossipCount", "OpCount"} {
                        aggregate.MessageData[keyName+"s"] = append(aggregate.MessageData[keyName+"s"], metadatum.MessageData[keyName])
                }

                aggregate.TSLag = appendPercentileData(metadatum.TSLag, aggregate.TSLag)
                aggregate.TS = appendPercentileData(metadatum.TS, aggregate.TS)
        }
        return aggregate
}

func appendPercentileData(sourceArray []int64, destinationArray [][]int64) [][]int64 {
        lengthDiff := len(sourceArray) - len(destinationArray)
        for i := 0; i < lengthDiff; i++ {
                destinationArray = append(destinationArray, []int64{})
        }
        for idx, percentile := range sourceArray {
                destinationArray[idx] = append(destinationArray[idx], percentile)
        }
        return destinationArray
}

func getStats(int64Array []int64) []int64 {
        if len(int64Array) == 0 {
                return []int64{0, 0, 0, 0}
        }
        /* Return Percentiles.  Currently: [0th(min), 5th, 50th(med), 95th, 100th(max)] */
        return []int64{Percentile(int64Array, 0.0), Percentile(int64Array, 0.05), Percentile(int64Array, 0.5), Percentile(int64Array, 0.95), Percentile(int64Array, 1.0)}
}

func getSortedTSArray(excludeHost string) []int64 {
        seerPeers := GetSeerPeers(excludeHost)
        tsArray := make([]int64, 0, len(seerPeers))
        for _, peer := range seerPeers {
                gossips, _ := getGossipArray(peer, "host", "data")
                for _, gossip := range gossips {
                        TS, err := ExtractTSFromJSON(gossip)
                        if err != nil {
                                continue
                        }
                        tsArray = append(tsArray, TS)
                }
        }
        sort.Sort(Int64Array(tsArray))
        return tsArray
}

func getSortedTSLagArray(excludeHost string) []int64 {
        seerPeers := GetSeerPeers(excludeHost)
        lagArray := make([]int64, 0, len(seerPeers))
        for _, peer := range seerPeers {
                gossips, modTimes := getGossipArray(peer, "host", "data")
                for idx, gossip := range gossips {
                        lag, err := getTSLag(gossip, modTimes[idx])
                        if err != nil {
                                continue
                        }
                        lagArray = append(lagArray, lag)
                }
        }
        sort.Sort(Int64Array(lagArray))
        return lagArray
}

func getTSLag(gossip string, modtime time.Time) (int64, error) {
        TS, err := ExtractTSFromJSON(gossip)
        if err != nil {
                return 0, err
        }
        lag := MS(modtime) - TS
        return lag, nil
}

/*******************************************************************************
    Silly helper functions.
*******************************************************************************/

type Int64Array []int64

func (p Int64Array) Len() int           { return len(p) }
func (p Int64Array) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Array) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func GetFilenameAndDir(fullpath string) (string, string) {
        /* Down and dirty, just don't like using 'filepath' package. */
        splitfullpath := strings.Split(fullpath, "/")

        filename := splitfullpath[len(splitfullpath)-1]
        dir := fullpath[:len(fullpath)-(len(filename)+1)]
        return filename, dir
}

/* Thanks Hugo Steinhaus. */
func GetCombinations(keys []string) [][]string {
        combinations := [][]string{}
        combinations = append(combinations, []string{keys[0]})
        for i := 1; i < len(keys); i++ {
                newcombinations := [][]string{}
                for j := 0; j < len(combinations); j++ {
                        combinations[j] = append(combinations[j], "")
                        for k := 0; k < i+1; k++ {
                                holderarray := make([]string, i+1)
                                copy(holderarray, combinations[j])
                                if k != i {
                                        copy(holderarray[k+1:], holderarray[k:])
                                }
                                holderarray[k] = keys[i]
                                newcombinations = append(newcombinations, holderarray)
                        }
                }
                combinations = newcombinations
        }
        return combinations
}

func Percentile(sortedArray []int64, percentile float64) int64 {
        maxIdx := len(sortedArray) - 1
        idx := int(math.Ceil(percentile * float64(maxIdx)))
        return sortedArray[idx]
}

func AbsInt(x int) int {
        if x < 0 {
                return -1 * x
        }
        return x
}

func MinInt(x int, y int) int {
        if x < y {
                return x
        }
        return y
}

func MaxInt(x int, y int) int {
        if x > y {
                return x
        }
        return y
}

func MS(time time.Time) int64 {
        return time.UnixNano() / 1000000
}

/* Freaky Voodoo for unittesting. */

var Now = func() time.Time { return time.Now() }
