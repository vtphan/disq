// Author: Vinhthuy Phan, 2014
package disq

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
   context           *zmq.Context
   sub_socket        *zmq.Socket
   query_socket      *zmq.Socket
   result_socket     *zmq.Socket
   data_file         string
}

// -----------------------------------------------------------------------
func NewWorker() *Worker {
   w := new(Worker)
   w.data_file = ""
   w.context, _ = zmq.NewContext()
   w.sub_socket, _ = w.context.NewSocket(zmq.SUB)
   w.sub_socket.SetSubscribe("")
   w.sub_socket.Connect("tcp://127.0.0.1:5555")

   w.query_socket, _ = w.context.NewSocket(zmq.PULL)
   w.query_socket.Connect("tcp://127.0.0.1:5557")

   w.result_socket, _ = w.context.NewSocket(zmq.PUSH)
   w.result_socket.Connect("tcp://127.0.0.1:5558")

   return w
}


// -----------------------------------------------------------------------
func (w *Worker) Run(thing WorkerInterface) {
   var msg []byte
   var items []string
   var state, query, result, data_file, index_file string
   var qid int64
   var err error

   pi := zmq.PollItems{
      zmq.PollItem{Socket: w.query_socket, Events: zmq.POLLIN},
      zmq.PollItem{Socket: w.sub_socket, Events: zmq.POLLIN},
   }

   state = "I"
   for {
      _, _ = zmq.Poll(pi, -1)
      switch {
      // process query
      case pi[0].REvents&zmq.POLLIN != 0:
         if state == "Q" {
            msg, _ = pi[0].Socket.Recv(0)
            items = strings.SplitN(string(msg), " ", 2)
            qid, err = strconv.ParseInt(items[0], 10, 64)
            if err != nil {
               w.result_socket.Send([]byte("ERR qid is not an integer"), 0)
            }
            query = items[1]
            result = thing.Process(int(qid), query)
            w.result_socket.Send([]byte(fmt.Sprintf("ANS %d %s", qid,result)), 0)
            if DEBUG { fmt.Println("Query:",qid,query,"\nResult:",result) }
         }

      // initialize index
      case pi[1].REvents&zmq.POLLIN != 0:
         if state == "I" {
            msg, _ = pi[1].Socket.Recv(0)
            items = strings.SplitN(string(msg), " ", 4)
            if items[0] == "CONF" {
               data_file = items[1]
               index_file = items[2]
               DEBUG = items[3] == "true"
               state = "Q"

               if w.data_file != data_file {
                  w.data_file = data_file
                  if _, err = os.Stat(index_file); err == nil {
                     if DEBUG { fmt.Println("Loading", index_file) }
                     thing.Load(index_file)
                  } else if _, err = os.Stat(data_file); err == nil {
                     if DEBUG { fmt.Println("Building from", data_file) }
                     thing.Build(data_file)
                  } else {
                     w.query_socket.Send([]byte("ERR data/index not found."), 0)
                  }
               } else if DEBUG { fmt.Println("Skip build/load.") }
            } else if items[0] == "END" {
               state = "I"
               if DEBUG { fmt.Println("Distributor sent all queries.") }
            }
         }
      }
   }
}

