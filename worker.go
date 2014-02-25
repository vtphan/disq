// Author: Vinhthuy Phan, 2014
package disq

import (
   "fmt"
   zmq "github.com/alecthomas/gozmq"
   "strings"
   "os"
   "strconv"
   "flag"
   // "time"
   // "bytes"
)

type WorkerInterface interface {
   Build(filename string)
   Load(filename string)
   Process(qid int, query string) string
}

type Worker struct {
   context           *zmq.Context
   sub_socket        *zmq.Socket
   query_socket      *zmq.Socket
   result_socket     *zmq.Socket
   index_path        string
   handle            WorkerInterface
   host              string
   pubsub_port, query_port, result_port   int

   // STOP (0), IDLE (1), ACTIVE (2)
   state             int
}

// -----------------------------------------------------------------------
func NewWorker(handle WorkerInterface) *Worker {
   flag.BoolVar(&DEBUG, "debug", DEBUG, "turn on debug mode")
   flag.Parse()

   var items         []string

   w := new(Worker)
   w.handle = handle
   w.index_path = ""

   items = strings.SplitN(PUBSUB, ":", 2)
   w.host = items[0]
   w.pubsub_port, _ = strconv.Atoi(items[1])

   w.context, _ = zmq.NewContext()
   w.sub_socket, _ = w.context.NewSocket(zmq.SUB)
   w.sub_socket.SetSubscribe("")
   w.sub_socket.Connect(fmt.Sprintf("tcp://%s:%d", w.host, w.pubsub_port))
   fmt.Printf("Worker subscribing to tcp://%s:%d\n", w.host,w.pubsub_port)
   return w
}


// -----------------------------------------------------------------------

func (w *Worker) Run() {
   defer w.sub_socket.Close()

   w.state = 1
   for w.state > 0 {
      if w.state == 1 {  // idle
         w.Listen()
      } else if w.state == 2 { // active
         w.ProcessQuery()
      }
   }
}

// -----------------------------------------------------------------------

func (w *Worker) Listen() {
   var index_path, data_path string
   var items []string
   var msg []byte
   var err error

   msg, _ = w.sub_socket.Recv(0)
   items = strings.SplitN(string(msg), " ", 5)
   if items[0] == "REQ" {
      w.query_port, _ = strconv.Atoi(items[1])
      w.result_port, _ = strconv.Atoi(items[2])
      data_path = items[3]
      index_path = items[4]
      query_address := fmt.Sprintf("tcp://%s:%d",w.host,w.query_port)
      result_address := fmt.Sprintf("tcp://%s:%d",w.host,w.result_port)
      fmt.Printf("Worker push/pull on %s %s\n",query_address,result_address)

      w.query_socket, _ = w.context.NewSocket(zmq.PULL)
      w.query_socket.Connect(query_address)
      w.result_socket, _ = w.context.NewSocket(zmq.PUSH)
      w.result_socket.Connect(result_address)
      w.state = 2

      if w.index_path != index_path {
         w.index_path = index_path
         if _, err = os.Stat(index_path); err == nil {
            w.handle.Load(index_path)
            if DEBUG { fmt.Println("Loading", index_path) }
         } else if _, err = os.Stat(data_path); err == nil {
            w.handle.Build(data_path)
            if DEBUG { fmt.Println("Building from", data_path) }
         } else {
            w.query_socket.Send([]byte("ERR data/index not found."),0)
            fmt.Printf("%s and %s not found", data_path,index_path)
         }
      } else if DEBUG {
         fmt.Println("Skip building & loading index.")
      }
   }
}

// -----------------------------------------------------------------------

func (w *Worker) ProcessQuery() {
   var msg []byte
   var items []string
   var query, result string
   var qid int64

   pi := zmq.PollItems{
      zmq.PollItem{Socket: w.query_socket, Events: zmq.POLLIN},
      zmq.PollItem{Socket: w.sub_socket, Events: zmq.POLLIN},
   }

   for {
      _, _ = zmq.Poll(pi, -1)
      switch {
      // wait for queries
      case pi[0].REvents&zmq.POLLIN != 0:
         msg, _ = pi[0].Socket.Recv(0)
         items = strings.SplitN(string(msg), " ", 2)
         qid, _ = strconv.ParseInt(items[0], 10, 64)
         query = items[1]
         result = w.handle.Process(int(qid), query)
         w.result_socket.Send([]byte(fmt.Sprintf("%d %s", qid,result)), 0)

         if DEBUG { fmt.Println("Query:",qid,query,"\nResult:",result) }

      // wait for END signal broadcast from client
      case pi[1].REvents&zmq.POLLIN != 0:
         msg, _ = pi[1].Socket.Recv(0)
         if string(msg) == "END" {
            w.state = 1
            w.query_socket.Close()
            w.result_socket.Close()
            if DEBUG { fmt.Println("Service ended") }
            return
         }
      }
   }
}

