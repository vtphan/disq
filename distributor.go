// Author: Vinhthuy Phan, 2014

package disq

import (
   "fmt"
   zmq "github.com/alecthomas/gozmq"
   "time"
   "os"
   // "flag"
   "bytes"
   "bufio"
   "strings"
   "strconv"
   "runtime"
)


type Distributor struct {
   context              *zmq.Context
   pub_socket           *zmq.Socket
   sub_socket           *zmq.Socket
   query_socket         *zmq.Socket
   result_socket        *zmq.Socket
   input_count    int
   result_count   int
   data_file      string
   index_file     string
}

func NewDistributor() *Distributor {
   d := new(Distributor)
   d.context, _ = zmq.NewContext()

   d.pub_socket, _ = d.context.NewSocket(zmq.PUB)
   d.pub_socket.Bind("tcp://127.0.0.1:5555")
   d.sub_socket, _ = d.context.NewSocket(zmq.SUB)
   d.sub_socket.SetSubscribe("")
   d.sub_socket.Connect("tcp://127.0.0.1:5555")

   // d.end_pub_socket, _ = d.context.NewSocket(zmq.PUB)
   // d.end_pub_socket.Bind("tcp://127.0.0.1:5556")

   // d.end_sub_socket, _ = d.context.NewSocket(zmq.SUB)
   // d.end_sub_socket.SetSubscribe("")
   // d.end_sub_socket.Connect("tcp://127.0.0.1:5556")

   d.query_socket, _ = d.context.NewSocket(zmq.PUSH)
   d.query_socket.Bind("tcp://127.0.0.1:5557")

   d.result_socket, _ = d.context.NewSocket(zmq.PULL)
   d.result_socket.Bind("tcp://127.0.0.1:5558")
   return d
}

// -----------------------------------------------------------------------
func (d *Distributor) Configure(data_file, index_file string, debug_mode bool) {
   d.data_file = data_file
   d.index_file = index_file

   DEBUG = debug_mode
   if DEBUG {
      fmt.Println("Configure", data_file, index_file)
   }

   // give some time for subscribers to get message
   time.Sleep(500*time.Millisecond)
   msg := fmt.Sprintf("CONF %s %s %t", data_file, index_file, DEBUG)
   d.pub_socket.Send([]byte(msg), 0)
   time.Sleep(500*time.Millisecond)
}

// -----------------------------------------------------------------------
func (d *Distributor) Distribute(queries_file string) {
   f, err := os.Open(queries_file)
   if err != nil { panic("error opening file " + queries_file) }
   r := bufio.NewReader(f)
   d.input_count = 0
   d.result_count = 0

   err = nil
   var line []byte

   for err == nil {
      line, err = r.ReadBytes('\n')
      line = bytes.Trim(line, "\n\r")
      if len(line) > 1 {
         msg := fmt.Sprintf("%d %s", d.input_count, line)
         d.query_socket.Send([]byte(msg), 0)
         d.input_count++
         if DEBUG { fmt.Println("Distribute", msg) }
      }
   }

   d.pub_socket.Send([]byte("END 0 0 0"), 0)
   fmt.Println("Send END message")
}

// -----------------------------------------------------------------------
func (d *Distributor) ProcessResult(processor func (int64, string)) {
   var items []string
   var msg []byte
   var qid int64
   var ans string
   distribute_all_queries := false

   pi := zmq.PollItems{
      zmq.PollItem{Socket: d.result_socket, Events: zmq.POLLIN},
      zmq.PollItem{Socket: d.sub_socket, Events: zmq.POLLIN},
   }

   for ! distribute_all_queries || d.result_count < d.input_count {
      _, _ = zmq.Poll(pi, -1)

      switch {
      // Receives result from workers.
      case pi[0].REvents&zmq.POLLIN != 0:
         msg, _ = pi[0].Socket.Recv(0)
         items = strings.SplitN(string(msg), " ", 2)
         if items[0] == "ANS" {
            items = strings.SplitN(items[1], " ", 2)
            qid, _ = strconv.ParseInt(items[0], 10, 64)
            ans = items[1]
            d.result_count++
            if DEBUG { fmt.Println("Process:", ans) }
            processor(qid, ans)
         } else if items[0] == "ERR" {
            fmt.Println("Error:", items[1])
         }

      // Distribute notifies that all queries have been distributed
      case pi[1].REvents&zmq.POLLIN != 0:
         msg, _ = pi[1].Socket.Recv(0)
         items = strings.SplitN(string(msg), " ", 4)
         if items[0] == "END" {
            distribute_all_queries = true
            if DEBUG { fmt.Println("Process: all queries distributed.") }
         }
      }
   }
}

// -----------------------------------------------------------------------
func (d *Distributor) Run( processor func (int64, string) ) {
   runtime.GOMAXPROCS(2)
   go d.Distribute("queries.txt")
   d.ProcessResult(processor)
}

