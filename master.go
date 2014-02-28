// Author: Vinhthuy Phan, 2014

package disq

import (
   "fmt"
   zmq "github.com/alecthomas/gozmq"
   "time"
   "os"
   "bytes"
   "bufio"
   "strings"
   "strconv"
   "runtime"
)

type Master struct {
   context              *zmq.Context
   pub_socket           *zmq.Socket
   sub_socket           *zmq.Socket
   query_socket         *zmq.Socket
   result_socket        *zmq.Socket
   input_count, result_count              int
   data_path, index_path, host            string
   pubsub_port, query_port, result_port   int
}

// -----------------------------------------------------------------------

func (m *Master) NewSocket(socket_type zmq.SocketType, connect_type string, port int) *zmq.Socket {
   var sock *zmq.Socket
   var err error
   sock, err = m.context.NewSocket(socket_type)
   if err != nil {
      panic(fmt.Sprintf("Socket (%s) error on port (%d): %s", connect_type, port, err))
   }
   if connect_type == "Bind" {
      err = sock.Bind(fmt.Sprintf("tcp://%s:%d", m.host, port))
   } else {
      err = sock.Connect(fmt.Sprintf("tcp://%s:%d", m.host, port))
   }
   if err != nil {
      sock.Close()
      panic(fmt.Sprintf("Connection (%s) error on port (%d): %s", connect_type, port, err))
   }
   return sock
}

// -----------------------------------------------------------------------

func NewMaster(config_file string) *Master {
   var items         []string

   m := new(Master)
   items = strings.SplitN(PUBSUB, ":", 2)
   m.host = items[0]
   m.pubsub_port, _ = strconv.Atoi(items[1])
   m.query_port, m.result_port, m.data_path, m.index_path = ReadConfig(config_file)

   m.context, _ = zmq.NewContext()
   m.pub_socket = m.NewSocket(zmq.PUB, "Bind", m.pubsub_port)
   m.sub_socket = m.NewSocket(zmq.SUB, "Connect", m.pubsub_port)
   m.sub_socket.SetSubscribe("")
   m.query_socket = m.NewSocket(zmq.PUSH, "Bind", m.query_port)
   m.result_socket = m.NewSocket(zmq.PULL, "Bind", m.result_port)

   m.SendRequest()

   fmt.Printf("Master connecting to %s:%d:%d:%d\n", m.host, m.pubsub_port, m.query_port, m.result_port)
   return m
}

// -----------------------------------------------------------------------
func (m *Master) SendRequest() {
   // give some time for subscribers to get message
   time.Sleep(500*time.Millisecond)
   msg := fmt.Sprintf("REQ %d %d %s %s",m.query_port,m.result_port,m.data_path,m.index_path)
   m.pub_socket.Send([]byte(msg), 0)
   time.Sleep(500*time.Millisecond)

}
// -----------------------------------------------------------------------

func (m *Master) Close() {
   m.pub_socket.Close()
   m.sub_socket.Close()
   m.query_socket.Close()
   m.result_socket.Close()
}

// -----------------------------------------------------------------------

func (m *Master) Run(query_file string, processor func (int64, string)) {
   var startTime, endTime time.Time
   if TIMING {
      startTime = time.Now()
   }
   defer m.Close()
   runtime.GOMAXPROCS(2)
   go m.SendQueries(query_file)
   m.ProcessResult(processor)

   if TIMING {
      endTime = time.Now()
      fmt.Println("Run time\t", endTime.Sub(startTime))
   }
}

// -----------------------------------------------------------------------

func (m *Master) SendQueries(query_file string) {
   f, err := os.Open(query_file)
   if err != nil { panic("error opening file " + query_file) }
   r := bufio.NewReader(f)
   m.input_count = 0
   m.result_count = 0

   err = nil
   var line []byte

   for err == nil {
      line, err = r.ReadBytes('\n')
      line = bytes.Trim(line, "\n\r")
      if len(line) > 1 {
         msg := fmt.Sprintf("%d %s", m.input_count, line)
         m.query_socket.Send([]byte(msg), 0)
         m.input_count++
         if DEBUG { fmt.Println("Distribute", msg) }
      }
   }

   m.pub_socket.Send([]byte("END"), 0)
   if DEBUG { fmt.Println("END request") }
}

// -----------------------------------------------------------------------

func (m *Master) ProcessResult(processor func (int64, string)) {
   var items []string
   var msg []byte
   var qid int64
   var ans string
   distribute_all_queries := false

   pi := zmq.PollItems{
      zmq.PollItem{Socket: m.result_socket, Events: zmq.POLLIN},
      zmq.PollItem{Socket: m.sub_socket, Events: zmq.POLLIN},
   }

   for ! distribute_all_queries || m.result_count < m.input_count {
      _, _ = zmq.Poll(pi, -1)

      switch {
      // Receives result from servers.
      case pi[0].REvents&zmq.POLLIN != 0:
         msg, _ = pi[0].Socket.Recv(0)
         items = strings.SplitN(string(msg), " ", 2)
         qid, _ = strconv.ParseInt(items[0], 10, 64)
         ans = items[1]
         if qid >= 0 {
            m.result_count++
            processor(qid, ans)
         } else {
            fmt.Println("Error:", qid, ans)
         }

      // Distribute notifies that all queries have been distributed
      case pi[1].REvents&zmq.POLLIN != 0:
         msg, _ = pi[1].Socket.Recv(0)
         items = strings.SplitN(string(msg), " ", 4)
         if items[0] == "END" {
            distribute_all_queries = true
         }
      }
   }
}

// -----------------------------------------------------------------------


