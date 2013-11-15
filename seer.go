package main

import (
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
        "math/rand"
        "net"
        "net/http"
        "os"
        "path/filepath"
        "regexp"
        "runtime"
        "strconv"
        "strings"
        "time"
)

type Gossip struct {
        SeerAddr    string
        ServiceName string
        ServiceAddr string
        SeerRequest string
        Tombstone   bool
        TS          int64
}

var (
        SeerRoot       string
        SeerOpDir      string
        SeerDataDir    string
        SeerServiceDir string
        SeerHostDir    string
        udpAddress     = flag.String("udp", "localhost:9999", "<host>:<port> to use for UDP server.")
        tcpAddress     = flag.String("tcp", "localhost:9998", "<host>:<port> to use for UDP server.")
        commandList    = map[string]bool{"exit": true, "get": true}
        tsRegexp       = regexp.MustCompile(`[,|{]\s*"TS"\s*:\s*(\d+)\s*[,|}]`)
)

func main() {
        runtime.GOMAXPROCS(int(runtime.NumCPU() / 2))
        flag.Parse()

        /* Guarantee folder paths */
        SeerRoot = "seer/" + *udpAddress
        SeerOpDir = SeerRoot + "/gossip/oplog"
        SeerDataDir = SeerRoot + "/gossip/data"
        SeerServiceDir = SeerDataDir + "/service"
        SeerHostDir = SeerDataDir + "/host"
        os.MkdirAll(SeerOpDir, 0777)
        os.MkdirAll(SeerDataDir, 0777)

        /* Single cleanup on start. */
        TombstoneReaper()
        AntiEntropy()

        /* Set up channels and start servers. */
        udpServerChannel := make(chan string)
        serviceServerChannel := make(chan string)

        go UDPServer(udpServerChannel, *udpAddress)
        go ServiceServer(*tcpAddress)

        /* Run background cleanup on 10 second cycle. */
        BackgroundLoop("Janitorial Work", 10, TombstoneReaper, AntiEntropy)

        /* Run background routine for GossipOps() */
        //BackgroundLoop("Gossip Oplog", 1, GossipOps)

        /* Wait for and handle any messages. */
        for {
                select {
                case message := <-udpServerChannel:
                        if message == "exit" {
                                fmt.Printf("\nBYE BYE\n")
                                return
                        } else if message == "get" {
                                fmt.Printf("\nGOT A 'GET' COMMAND!\n")
                        }
                case message := <-serviceServerChannel:
                        fmt.Printf("\nServiceServer message: %s\n", message)
                }
        }
}

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

func ServiceServer(address string) {
        fmt.Printf("[ServiceServer] Can query for service by name.\nI listen on %s", address)
        http.HandleFunc("/", ServiceHandler)
        http.ListenAndServe(address, nil)
}

func errorResponse(w http.ResponseWriter, message string) {
        w.WriteHeader(http.StatusInternalServerError)
        w.Write([]byte(message))
}

func ServiceHandler(w http.ResponseWriter, r *http.Request) {
        requestTypeMap := map[string]string{
                "host":    "seer",
                "seer":    "seer",
                "service": "service",
        }
        /* Allow GET by 'service' or 'seeraddr' */
        splitURL := strings.Split(r.URL.Path, "/")

        switch r.Method {
        case "GET":
                if requestTypeMap[splitURL[1]] == "" {
                        errorResponse(w, "Please query for 'seer' or 'service'")
                        return
                }
                jsonPayload := getServiceData(splitURL[2], requestTypeMap[splitURL[1]])
                fmt.Fprintf(w, jsonPayload)
        case "PUT":
                if splitURL[1] != "seed" {
                        errorResponse(w, "I only accept PUTs for 'seed'")
                }
                path := fmt.Sprintf("/tmp/seer_received_seed_%s_%d.tar.gz", *udpAddress, time.Now().Unix())
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

func getServiceData(name string, requestType string) string {
        var servicePath string
        if requestType == "service" {
                servicePath = SeerServiceDir + "/" + name
        } else if requestType == "seer" {
                servicePath = SeerHostDir + "/" + name
        }

        serviceHosts, _ := ioutil.ReadDir(servicePath)
        /* read files from servicePath, append to jsonPayload. */
        jsonPayload := ""
        for _, serviceHost := range serviceHosts {
                if serviceHost.IsDir() {
                        continue
                }
                serviceData, err := ioutil.ReadFile(servicePath + "/" + serviceHost.Name())
                if err == nil {
                        jsonPayload += fmt.Sprintf("%s,", string(serviceData))
                } else {
                        jsonPayload += fmt.Sprintf("ERROR: %s\nFILE: %s", err, serviceHost.Name())
                }
        }
        if len(jsonPayload) > 0 {
                jsonPayload = jsonPayload[:len(jsonPayload)-1]
        }
        return "[" + jsonPayload + "]"
}

// Once receive UDP notification that seerDestinationAddr needs seed
// tar gz 'host' and 'service' dirs and PUT to seerDestinationAddr.
func sendSeed(seerDestinationAddr string) {
        tarredGossipFile := fmt.Sprintf("/tmp/seer_generated_seed_%s_%s.tar.gz", *udpAddress, seerDestinationAddr)
        err := createTarGz(tarredGossipFile, SeerServiceDir, SeerHostDir)
        if err != nil {
                fmt.Printf("[sendSeed] Failed to create tar.gz. ERR: %s", err)
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
                fmt.Printf("[sendSeed] ERROR WITH STAT(): %s", err)
                return
        } else {
                request.ContentLength = stat.Size()
        }
        response, err := http.DefaultClient.Do(request)
        fmt.Printf("[sendSeed] RESPONSE: %s\nERR: %s\n", response, err)
}

// Zips up seer/<hostname>/gossip/data/ (aka SeerDataDir)
// General usage is targeting 'host' and 'service' subfolders.
// Removes SeerDataDir root for "easier" untarring into destination.
// TODO: make less stupid.
func createTarGz(tarpath string, folders ...string) error {
        tarfile, err := os.Create(tarpath)
        if err != nil {
                return err
        }
        defer tarfile.Close()
        gw, err := gzip.NewWriterLevel(tarfile, gzip.BestCompression)
        defer gw.Close()
        tw := tar.NewWriter(gw)
        defer tw.Close()
        for _, folder := range folders {
                filepath.Walk(folder, func(path string, fileinfo os.FileInfo, err error) error {
                        if fileinfo.IsDir() {
                                return nil
                        }
                        header, err := tar.FileInfoHeader(fileinfo, path)
                        header.Name = path[len(SeerDataDir)+1:]
                        err = tw.WriteHeader(header)
                        if err != nil {
                                fmt.Printf("Failed to write Tar Header. Err: %s\n", err)
                                return err
                        }
                        /* Add file. */
                        file, err := os.Open(path)
                        if err != nil {
                                fmt.Printf("Failed to open path: [%s] Err: %s\n", path, err)
                                return err
                        }
                        defer file.Close()
                        _, err = io.Copy(tw, file)
                        if err != nil {
                                fmt.Printf("Failed to copy file into tar: [%s] Err: %s\n", path, err)
                                return err
                        }
                        return nil
                })
        }

        return err
}

func processSeed(targzpath string) {
        targzfile, err := os.Open(targzpath)
        if err != nil {

        }
        defer targzfile.Close()
        gzr, err := gzip.NewReader(targzfile)
        if err != nil {

        }
        defer gzr.Close()
        tr := tar.NewReader(gzr)
        for {
                hdr, err := tr.Next()
                if err == io.EOF {
                        fmt.Printf("[processSeed] Reached end of tr!!\n")
                        break
                }
                if err != nil {
                        fmt.Printf("[processSeed] tr ERR: %s\n", err)
                        break
                }
                fmt.Printf("[processSeed] filename: %s\n", hdr.Name)
                buf := bytes.NewBuffer(nil)
                io.Copy(buf, tr)
                folder := filepath.Dir(SeerDataDir + "/" + hdr.Name)
                filename := filepath.Base(folder)
                err = LazyWriteFile(folder, filename, buf.Bytes())
                if err != nil {
                        fmt.Printf("[processSeed] ERRR: %s", err)
                }
        }
        return
}

func UDPServer(ch chan<- string, address string) {
        /*
           1. Listen for gossip.
           2. Possibly handle special messages?
             - Someone heard you were down?
        */
        udpLn, err := net.ListenPacket("udp", address)
        if err != nil {
                log.Fatal(err)
        }
        defer udpLn.Close()
        udpBuff := make([]byte, 128)
        for {
                n, _, err := udpLn.ReadFrom(udpBuff)
                if err != nil {
                        log.Fatal(err)
                }
                message := strings.Trim(string(udpBuff[:n]), "\n")
                if commandList[message] {
                        ch <- message
                } else {
                        go ProcessGossip(message)
                }
        }
}

func ProcessGossip(gossip string) {
        /*
           1. Write Gossip to gossiplog.
           2. Share gossip?
        */

        decodedGossip, err := VerifyGossip(gossip)
        if decodedGossip.SeerRequest == "SeedMe" {
                /* Have received a request, maybe respond? */
                go sendSeed(decodedGossip.SeerAddr)
                fmt.Printf("Seed Requested! Sending to: %s\n", decodedGossip.SeerAddr)
                return
        }
        if err != nil || decodedGossip.SeerAddr == "" {
                fmt.Printf("\nBad Gossip!: %s\nErr: %s\n", gossip, err)
                return
        }
        /* Write to oplog. opName limits updates from single host to 10 per nanosecond.. */
        opName := fmt.Sprintf("%d_%s_%d", time.Now().UnixNano(), decodedGossip.SeerAddr, (rand.Int()%10)+10)
        err = LazyWriteFile(SeerOpDir, opName+".op", []byte(gossip))
        if err != nil {
                /* Not sure want to panic here.  Move forward at all costs? */
                panic(err)
        }
        /* Save it. */
        PutGossip(gossip, decodedGossip)

        /* Spread the word? Or, use GossipOps()? */
        // GossipGossip(gossip)
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

func PutGossip(gossip string, decodedGossip Gossip) {
        /* Only checking SeerServiceDir for current TS. */
        /* Current code has odd side effect of allowing Seer data to exist for host even if we missed initial Seer HELO gossip. */
        /* We leave it to AntiEntropy to patch that up. */
        if decodedGossip.ServiceName == "" {
                decodedGossip.ServiceName = "Seer"
        }
        if !FreshGossip(SeerServiceDir+"/"+decodedGossip.ServiceName+"/"+decodedGossip.SeerAddr, decodedGossip.TS) {
                fmt.Printf("[Old Assed Gossip] I ain't writing that.\n")
                return
        }
        err := LazyWriteFile(SeerServiceDir+"/"+decodedGossip.ServiceName, decodedGossip.SeerAddr, []byte(gossip))
        if err != nil {
                fmt.Printf("Could not PutGossip to: [%s]! Error:\n%s", SeerServiceDir+"/"+decodedGossip.ServiceName, err)
        }
        err = LazyWriteFile(SeerHostDir+"/"+decodedGossip.SeerAddr, decodedGossip.ServiceName, []byte(gossip))
        if err != nil {
                fmt.Printf("Could not PutGossip to: [%s]! Error:\n%s", SeerHostDir+"/"+decodedGossip.SeerAddr, err)
        }
}

func FreshGossip(filePath string, newTS int64) bool {
        serviceData, err := ioutil.ReadFile(filePath)
        if err != nil {
                return true
        }
        currentTS, err := ExtractTSFromJSON(string(serviceData))
        return newTS > currentTS
}

func ExtractTSFromJSON(gossip string) (int64, error) {
        /* Looks for "TS" in JSON like:
           {"TS":<numbers>,...}
           {...,"TS":<numbers>}
           {...,"TS":<numbers>,...}
        */
        ts := tsRegexp.FindStringSubmatch(gossip)
        if len(ts) == 2 {
                return strconv.ParseInt(ts[1], 10, 64)
        }
        return 0, errors.New("Could not find TS.")
}

func LazyWriteFile(folderName string, fileName string, data []byte) error {
        err := ioutil.WriteFile(folderName+"/"+fileName, data, 0777)
        if err != nil {
                os.MkdirAll(folderName, 0777)
                err = ioutil.WriteFile(folderName+"/"+fileName, data, 0777)
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

func GossipGossip(gossip string) {
        fmt.Printf("[GossipGossip] Gossiping Gossip: %s", gossip)
        /*
           if HostList > M:
             return
           Add self to Gossip HostList.
           for N random Gossipees:
             send Gossip
        */
}
