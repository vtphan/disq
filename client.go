// Author: Vinhthuy Phan, 2014

package disq

import (
   "fmt"
   zmq "github.com/alecthomas/gozmq"
   "time"
   "os"
   "flag"
   "bytes"
   "bufio"
   "io/ioutil"
   "strings"
   "strconv"
   "runtime"
)

// flag.BoolVar(&DEBUG, "DEBUG", DEBUG, "turn on debug mode")


type Client struct {
   context              *zmq.Context
   pub_socket           *zmq.Socket
   sub_socket           *zmq.Socket
   query_socket         *zmq.Socket
   result_socket        *zmq.Socket
   input_count, result_count              int
   data_file, index_file host             string
   pubsub_port, query_port, result_port   int
}

// -----------------------------------------------------------------------

func NewClient(config_file string) *Client {
   var err           error
   var host          []byte
   var items         []string
   var address       string

   address, err = ioutil.ReadFile(CONFIG_FILE)

   if err != nil {
      panic(fmt.Sprintf("Problem reading %s.", CONFIG_FILE))
   }

   d := new(Client)
   items = strings.Split(PUBSUB, ":", 2)
   d.host = items[0]
   d.pubsub_port, _ = strconv.Atoi(items[1])
   d.query_port, d.result_port, d.data_file, d.index_file = ReadConfig(config_file)

   d.context, _ = zmq.NewContext()
   d.pub_socket, _ = d.context.NewSocket(zmq.PUB)
   d.pub_socket.Bind(fmt.Sprintf("tcp://%s:%d", d.host, d.pubsub_port))
   d.sub_socket, _ = d.context.NewSocket(zmq.SUB)
   d.sub_socket.SetSubscribe("")
   d.sub_socket.Connect(fmt.Sprintf("tcp://%s:%d", d.host, d.pubsub_port))
   d.query_socket, _ = d.context.NewSocket(zmq.PUSH)
   d.query_socket.Bind(fmt.Sprintf("tcp://%s:%d", d.host, d.query_port))
   d.result_socket, _ = d.context.NewSocket(zmq.PULL)
   d.result_socket.Bind(fmt.Sprintf("tcp://%s:%d", d.host, d.result_port))

   // give some time for subscribers to get message
   time.Sleep(500*time.Millisecond)
   msg := fmt.Sprintf("REQ %s %s %s %s %t",d.query_port,d.result_port,data_file,index_file,DEBUG)
   d.pub_socket.Send([]byte(msg), 0)
   time.Sleep(500*time.Millisecond)

   fmt.Printf("Client serving on %s:%d:%d:%d\n", d.host, d.pubsub_port, d.query_port, d.result_port)
   return d
}

// -----------------------------------------------------------------------

func (d *Client) Close() {
   d.pub_socket.Close()
   d.sub_socket.Close()
   d.query_socket.Close()
   d.result_socket.Close()
}

// -----------------------------------------------------------------------

func (d *Client) Run(query_file string, processor func (int64, string)) {
   defer d.Close()
   runtime.GOMAXPROCS(2)
   go d.SendQueries(query_file)
   d.ProcessResult(processor)
}

// -----------------------------------------------------------------------

func (d *Client) SendQueries(query_file string) {
   f, err := os.Open(query_file)
   if err != nil { panic("error opening file " + query_file) }
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

   d.pub_socket.Send([]byte("END"), 0)

   if DEBUG { fmt.Println("Send END message") }
}

// -----------------------------------------------------------------------

func (d *Client) ProcessResult(processor func (int64, string)) {
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
         qid, _ = strconv.ParseInt(items[0], 10, 64)
         ans = items[1]
         if qid >= 0 {
            d.result_count++
            if DEBUG { fmt.Println("Process:", ans) }
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
            if DEBUG { fmt.Println("Process: all queries distributed.") }
         }
      }
   }
}

// -----------------------------------------------------------------------


