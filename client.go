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
   "flag"
)

type Client struct {
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

func (c *Client) NewSocket(socket_type zmq.SocketType, connect_type string, port int) *zmq.Socket {
   var sock *zmq.Socket
   var err error
   sock, err = c.context.NewSocket(socket_type)
   if err != nil {
      panic(fmt.Sprintf("Socket (%s) error on port (%d): %s", connect_type, port, err))
   }
   if connect_type == "Bind" {
      err = sock.Bind(fmt.Sprintf("tcp://%s:%d", c.host, port))
   } else {
      err = sock.Connect(fmt.Sprintf("tcp://%s:%d", c.host, port))
   }
   if err != nil {
      sock.Close()
      panic(fmt.Sprintf("Connection (%s) error on port (%d): %s", connect_type, port, err))
   }
   return sock
}

// -----------------------------------------------------------------------

func NewClient(config_file string) *Client {
   flag.BoolVar(&DEBUG, "debug", DEBUG, "turn on debug mode")
   flag.BoolVar(&TIMING, "timing", TIMING, "turn on timing")
   flag.Parse()

   var items         []string

   c := new(Client)
   items = strings.SplitN(PUBSUB, ":", 2)
   c.host = items[0]
   c.pubsub_port, _ = strconv.Atoi(items[1])
   c.query_port, c.result_port, c.data_path, c.index_path = ReadConfig(config_file)

   c.context, _ = zmq.NewContext()
   c.pub_socket = c.NewSocket(zmq.PUB, "Bind", c.pubsub_port)
   c.sub_socket = c.NewSocket(zmq.SUB, "Connect", c.pubsub_port)
   c.sub_socket.SetSubscribe("")
   c.query_socket = c.NewSocket(zmq.PUSH, "Bind", c.query_port)
   c.result_socket = c.NewSocket(zmq.PULL, "Bind", c.result_port)

   c.SendRequest()

   fmt.Printf("Client connecting to %s:%d:%d:%d\n", c.host, c.pubsub_port, c.query_port, c.result_port)
   return c
}

// -----------------------------------------------------------------------
func (c *Client) SendRequest() {
   // give some time for subscribers to get message
   time.Sleep(500*time.Millisecond)
   msg := fmt.Sprintf("REQ %d %d %s %s",c.query_port,c.result_port,c.data_path,c.index_path)
   c.pub_socket.Send([]byte(msg), 0)
   time.Sleep(500*time.Millisecond)

}
// -----------------------------------------------------------------------

func (c *Client) Close() {
   c.pub_socket.Close()
   c.sub_socket.Close()
   c.query_socket.Close()
   c.result_socket.Close()
}

// -----------------------------------------------------------------------

func (c *Client) Run(query_file string, processor func (int64, string)) {
   var startTime, endTime time.Time
   if TIMING {
      startTime = time.Now()
   }
   defer c.Close()
   runtime.GOMAXPROCS(2)
   go c.SendQueries(query_file)
   c.ProcessResult(processor)

   if TIMING {
      endTime = time.Now()
      fmt.Println("Run time\t", endTime.Sub(startTime))
   }
}

// -----------------------------------------------------------------------

func (c *Client) SendQueries(query_file string) {
   f, err := os.Open(query_file)
   if err != nil { panic("error opening file " + query_file) }
   r := bufio.NewReader(f)
   c.input_count = 0
   c.result_count = 0

   err = nil
   var line []byte

   for err == nil {
      line, err = r.ReadBytes('\n')
      line = bytes.Trim(line, "\n\r")
      if len(line) > 1 {
         msg := fmt.Sprintf("%d %s", c.input_count, line)
         c.query_socket.Send([]byte(msg), 0)
         c.input_count++
         if DEBUG { fmt.Println("Distribute", msg) }
      }
   }

   c.pub_socket.Send([]byte("END"), 0)
   if DEBUG { fmt.Println("END request") }
}

// -----------------------------------------------------------------------

func (c *Client) ProcessResult(processor func (int64, string)) {
   var items []string
   var msg []byte
   var qid int64
   var ans string
   distribute_all_queries := false

   pi := zmq.PollItems{
      zmq.PollItem{Socket: c.result_socket, Events: zmq.POLLIN},
      zmq.PollItem{Socket: c.sub_socket, Events: zmq.POLLIN},
   }

   for ! distribute_all_queries || c.result_count < c.input_count {
      _, _ = zmq.Poll(pi, -1)

      switch {
      // Receives result from servers.
      case pi[0].REvents&zmq.POLLIN != 0:
         msg, _ = pi[0].Socket.Recv(0)
         items = strings.SplitN(string(msg), " ", 2)
         qid, _ = strconv.ParseInt(items[0], 10, 64)
         ans = items[1]
         if qid >= 0 {
            c.result_count++
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


