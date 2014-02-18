package main

import (
   "fmt"
   zmq "github.com/alecthomas/gozmq"
   // "bytes"
   "strings"
   // "time"
   "os"
   "strconv"
   // "flag"
   // "bufio"
)

type WorkerInterface interface {
   Build(filename string)
   Load(filename string)
   Process(qid int, query string) string
}

type Worker struct {
   context        *zmq.Context
   sender         *zmq.Socket
   receiver       *zmq.Socket
   subscriber     *zmq.Socket
   id             string
}

func MakeWorker() *Worker {
   w := new(Worker)
   w.id = ""
   w.context, _ = zmq.NewContext()
   w.subscriber, _ = w.context.NewSocket(zmq.SUB)
   w.subscriber.SetSubscribe("")
   w.subscriber.Connect("tcp://127.0.0.1:5556")
   w.receiver, _ = w.context.NewSocket(zmq.PULL)
   w.receiver.Connect("tcp://127.0.0.1:5557")
   w.sender, _ = w.context.NewSocket(zmq.PUSH)
   w.sender.Connect("tcp://127.0.0.1:5558")
   return w
}


func (w *Worker) Run(thing WorkerInterface) {
   pi := zmq.PollItems{
      zmq.PollItem{Socket: w.receiver, Events: zmq.POLLIN},
      zmq.PollItem{Socket: w.subscriber, Events: zmq.POLLIN},
   }
   var msg []byte
   var items []string
   var state, query, result, data_file, index_file string
   var qid int64
   var err error

   state = "I"
   for {
      _, _ = zmq.Poll(pi, -1)
      switch {
      // query is a pushed message from distributor
      case pi[0].REvents&zmq.POLLIN != 0:
         msg, _ = pi[0].Socket.Recv(0)
         items = strings.SplitN(string(msg), " ", 2)
         fmt.Println("Get", string(msg))
         qid, err = strconv.ParseInt(items[0], 10, 64)
         if err != nil {
            w.sender.Send([]byte("ERR qid is not an integer"), 0)
         }
         query = items[1]
         if state == "Q"{
            result = thing.Process(int(qid), query)
            w.sender.Send([]byte(fmt.Sprintf("ANS %d %s", qid,result)), 0)
            fmt.Println("Send", "ANS", qid, result)
         }

      // broadcast is a published message from distributor
      case pi[1].REvents&zmq.POLLIN != 0:
         msg, _ = pi[1].Socket.Recv(0)
         fmt.Println("Get", string(msg))
         items = strings.SplitN(string(msg), " ", 3)
         data_file = items[1]
         index_file = items[2]
         if items[0]=="I" && state=="I" {
            if _, err = os.Stat(index_file); err == nil {
               thing.Load(index_file)
            } else if _, err = os.Stat(data_file); err == nil {
               thing.Build(data_file)
            } else {
               w.sender.Send([]byte("ERR Neither data nor index file exists."), 0)
            }
            state = "Q"
         }
      }
   }
}

//---------------------------------------------

type Test struct {}

func (t Test) Build(f string) {
   fmt.Println("Test:Build ", f)
}
func (t Test) Load(f string) {
   fmt.Println("Test:Load ", f)
}
func (t Test) Process(qid int, query string) string {
   return "Echo " + query
}

func main() {
   w := MakeWorker()
   w.Run( Test{} )
}