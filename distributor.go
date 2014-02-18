package main

import (
   "fmt"
   zmq "github.com/alecthomas/gozmq"
   "time"
   "os"
   // "flag"
   "bufio"
   "strings"
   "strconv"
   "runtime"
)

type Distributor struct {
   context        *zmq.Context
   sender         *zmq.Socket
   receiver       *zmq.Socket
   publisher      *zmq.Socket
   input_count    int
   result_count   int
   data_file      string
   index_file     string
}

func MakeDistributor() *Distributor {
   d := new(Distributor)
   d.context, _ = zmq.NewContext()
   d.publisher, _ = d.context.NewSocket(zmq.PUB)
   d.publisher.Bind("tcp://127.0.0.1:5556")
   // subscriber won't get (first) message without this delay
   time.Sleep(500*time.Millisecond)

   d.sender, _ = d.context.NewSocket(zmq.PUSH)
   d.sender.Bind("tcp://127.0.0.1:5557")
   d.receiver, _ = d.context.NewSocket(zmq.PULL)
   d.receiver.Bind("tcp://127.0.0.1:5558")
   return d
}

func (d *Distributor) Configure(data_file, index_file string) {
   d.data_file = data_file
   d.index_file = index_file
   d.publisher.Send([]byte("I " + data_file + " " + index_file), 0)
   fmt.Println("Send I", data_file, index_file)
}

func (d *Distributor) Distribute(queries_file string, done_dist chan bool, separator byte) {
   f, err := os.Open(queries_file)
   if err != nil { panic("error opening file " + queries_file) }
   r := bufio.NewReader(f)
   d.input_count = 0
   d.result_count = 0

   err = nil
   var line []byte
   for err == nil {
      line, err = r.ReadBytes(separator)
      if len(line) > 1 {
         msg := fmt.Sprintf("%d %s", d.input_count, line)
         d.sender.Send([]byte(msg), 0)
         fmt.Println("Send", msg)
         d.input_count++
      }
   }
   done_dist <- true
}

func (d *Distributor) CollectResult(done_dist chan bool,  f func (int64, string)) {
   var done bool = false
   var items []string
   var msg []byte
   var qid int64
   var ans string
   var err error

   for ! done || d.result_count < d.input_count {
      select {
         case done =<- done_dist: break
         default: break
      }
      msg, err = d.receiver.Recv(0)
      items = strings.SplitN(string(msg), " ", 2)
      if items[0] == "ANS" {
         items = strings.SplitN(items[1], " ", 2)
         qid, err = strconv.ParseInt(items[0], 10, 64)
         if err != nil {
            fmt.Println("Invalid format from worker message.", items)
         }
         ans = items[1]
         f(qid, ans)
         d.result_count++
      }
   }
}

func (d *Distributor) Run( f func (int64, string) ) {
   done_dist := make(chan bool)
   go d.Distribute("queries.txt", done_dist, '\n')
   d.CollectResult(done_dist, f)
}

func main() {
   runtime.GOMAXPROCS(2)
   simply_print_out := func (qid int64, res string ) {
      fmt.Println("Got qid", qid, ", and answer:", res)
   }

   d := MakeDistributor()
   d.Configure("genome.txt", "genome.index")
   d.Run(simply_print_out)

   fmt.Println("finished")
   // time.Sleep(1 * time.Second)
}
