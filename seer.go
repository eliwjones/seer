package main
import (
    "encoding/json"
    "flag"
    "fmt"
    "io/ioutil"
    "log"
    "net"
    "net/http"
    "os"
    "runtime"
    "strings"
    "time"
)

type Gossip struct {
    Name string
    Body string
    Time int64
}

var (
  SeerRoot string
  udpAddress = flag.String("udp", "localhost:9999", "<host>:<port> to use for UDP server.")
  tcpAddress = flag.String("tcp", "localhost:9998", "<host>:<port> to use for UDP server.")
)

func main() {
  runtime.GOMAXPROCS(int(runtime.NumCPU()/2))
  flag.Parse()

  /* Guarantee folder paths */
  SeerRoot = "seer/" + *udpAddress
  os.MkdirAll(SeerRoot + "/gossip/oplog", 0777)

  /* Single cleanup on start. */
  TombstoneReaper()
  AntiEntropy()

  /* Set up channels and start servers. */
  udpServerChannel := make(chan string)
  serviceServerChannel := make(chan string)

  go UDPServer(udpServerChannel, *udpAddress)
  go ServiceServer(serviceServerChannel, *tcpAddress)

  /* Run background cleanup on 10 second cycle. */
  go func() {
    for {
      time.Sleep(10000 * time.Millisecond)
      fmt.Printf("\nTIME: %s\n", time.Now())
      TombstoneReaper()
      AntiEntropy()
    }
  }()

  /* Wait for and handle any messages. */
  for {
    select {
      case message := <- udpServerChannel:
        if message == "exit" {
          fmt.Printf("\nBYE BYE\n")
          return
        }
      case message := <- serviceServerChannel:
        fmt.Printf("\nServiceServer message: %s\n", message)
    }
  }
}

func AntiEntropy() {
  /*
    1. Wrapped by background jobs. (Sleep Q seconds then do.)
    2. Check random host gossip/source (or gossip/services) zip checksum.
    3. Sync if checksum no match.

  */
  fmt.Printf("I grab fill copies of other host dbs occassionally.\n")
}

func TombstoneReaper() {
  /*
    1. Wrapped by background jobs. (Sleep Q seconds then do.)
    2. Remove anything but most recent doc that is older than P seconds.
    3. Remove remaining docs if they are Tombstoned and older than certain time.
  */
  fmt.Printf("I remove stuff that has been deleted or older timestamped source docs.\n")
}

func ServiceServer(ch chan<- string, address string) {
  fmt.Printf("Can query for service by name.\nI listen on %s", address)
  ch <- "I am a message!"
  http.HandleFunc("/", ServiceHandler)
  http.ListenAndServe(address, nil)
}

func ServiceHandler(w http.ResponseWriter, r *http.Request){
  jsonPayload := fmt.Sprintf(`{"Name":"Huh huh", "Body":"%s"}`, r.URL.Path[1:])
  fmt.Fprintf(w, jsonPayload)
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
    if message == "exit" {
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

  gossipSource, err := VerifyGossip(gossip)
  if err != nil || gossipSource == "" {
    fmt.Printf("\nBad Gossip!: %s\nErr: %s\n", gossip, err)
    return
  }
  /* Write to oplog. ?? */
  err = ioutil.WriteFile(SeerRoot + "/gossip/oplog/" + gossipSource  + ".txt", []byte(gossip), 0777)
  if err != nil {
    panic(err)
  }
  /* Save it. */
  PutGossip(gossip)

  /* Spread the word. */
  GossipGossip(gossip)
}

func VerifyGossip(gossip string) (string, error) {
  var g Gossip
  sourceHost := ""
  err := json.Unmarshal([]byte(gossip), &g)
  if err == nil {
    sourceHost = g.Name
  }
  return sourceHost, err
}

func PutGossip(gossip string) {
  /* Merge into local "DB"
     1. Write service type info to gossip/service/source ?
     2. What about gossip/source/service ?? (Would be needed for removes?)
       - source = hostname:port
       - folder is empty if no services
     ** Need Tombstones
     ** Guess there can be a reaper who deletes Tombstones older than M.
  */
  fmt.Printf("Putting Gossip.")
}

func GossipGossip(gossip string) {
  fmt.Printf("Gossiping Gossip: %s", gossip)
  /*
  if HostList > M:
    return
  Add self to Gossip HostList.
  for N random Gossipees:
    send Gossip
  */
}
