package main

import (
   "fmt"
   zmq "github.com/alecthomas/gozmq"
   "time"
   "os"
   // "flag"
   "bufio"
)

type Distributor struct {
   context        *zmq.Context
   sender         *zmq.Socket
   receiver       *zmq.Socket
   broadcaster    *zmq.Socket
   input_count    int
   result_count   int
}

func MakeDistributor() *Distributor {
   d = new(Distributor)
   d.context, _ = zmq.NewContext()
   d.sender, _ = d.context.NewSocket(zmq.PUSH)
   d.sender.Bind("tcp://*:5557")
   d.receiver, _ = d.context.NewSocket(zmq.PULL)
   d.receiver.Bind("tcp://*:5558")
   d.broadcaster, _ = d.context.NewSocket(zmq.PUB)
   d.broadcaster.Bind("tcp://*:5559")
   return d
}

func (d *Distributor) Init(name string) {
   for i=0; i<2; i++ {
      time.Sleep(500 * time.Millisecond)
      d.broadcaster.Send([]byte("init "+name), 0)
   }
}

func (d *Distributor) Distribute(queries_file string, done_dist chan bool, separator byte) {
   f, err := os.Open(queries_file)
   if err != nil { panic("error opening file " + queries_file) }
   r := bufio.NewReader(f)
   d.input_count = 0
   d.result_count = 0
   for {
      line, _ := r.ReadBytes(separator)
      if len(line) > 1 {
         msg = fmt.Sprintf("%d %s", d.input_count, line)
         d.sender.Send([]byte(msg), 0)
         d.input_count++
         fmt.Println("Distribute", msg)
      }
   }
   done_dist <- true
}

func (d *Distributor) CollectResult(done_dist chan bool) {
   done := false
   for ! done || d.result_count < d.input_count {
      select {
      case done =<- done_dist:
      }
      msg, _ := receiver.Recv(0)
      d.result_count++
      fmt.Println("Collect Result", d.result_count, string(msg))
   }
}

func (d *Distributor) Run() {
   done_dist := make(chan bool)
   d.Init("genome1.txt")
   go d.Distribute("queries.txt", done_dist, '\n')
   d.CollectResult(done_dist)
}

func main() {
   d = MakeDistributor()
   d.Run()
   fmt.Println("finished")
}
