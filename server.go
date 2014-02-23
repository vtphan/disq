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

type ServerInterface interface {
   Build(filename string)
   Load(filename string)
   Process(qid int, query string) string
}

type Server struct {
   context           *zmq.Context
   sub_socket        *zmq.Socket
   query_socket      *zmq.Socket
   result_socket     *zmq.Socket
   index_path        string
   handle            ServerInterface
   host              string
   pubsub_port, query_port, result_port   int

   // STOP (0), IDLE (1), ACTIVE (2)
   state             int
}

// -----------------------------------------------------------------------
func NewServer(handle ServerInterface) *Server {
   flag.BoolVar(&DEBUG, "debug", DEBUG, "turn on debug mode")
   flag.Parse()

   var items         []string

   s := new(Server)
   s.handle = handle
   s.index_path = ""

   items = strings.SplitN(PUBSUB, ":", 2)
   s.host = items[0]
   s.pubsub_port, _ = strconv.Atoi(items[1])

   s.context, _ = zmq.NewContext()
   s.sub_socket, _ = s.context.NewSocket(zmq.SUB)
   s.sub_socket.SetSubscribe("")
   s.sub_socket.Connect(fmt.Sprintf("tcp://%s:%d", s.host, s.pubsub_port))
   fmt.Printf("Server subscribing to tcp://%s:%d\n", s.host,s.pubsub_port)
   return s
}


// -----------------------------------------------------------------------

func (s *Server) Serve() {
   defer s.sub_socket.Close()

   s.state = 1
   for s.state > 0 {
      if s.state == 1 {  // idle
         s.Listen()
      } else if s.state == 2 { // active
         s.ProcessQuery()
      }
   }
}

// -----------------------------------------------------------------------

func (s *Server) Listen() {
   var index_path, data_path string
   var items []string
   var msg []byte
   var err error

   msg, _ = s.sub_socket.Recv(0)
   items = strings.SplitN(string(msg), " ", 5)
   if items[0] == "REQ" {
      s.query_port, _ = strconv.Atoi(items[1])
      s.result_port, _ = strconv.Atoi(items[2])
      data_path = items[3]
      index_path = items[4]
      query_address := fmt.Sprintf("tcp://%s:%d",s.host,s.query_port)
      result_address := fmt.Sprintf("tcp://%s:%d",s.host,s.result_port)
      fmt.Printf("Server push/pull on %s %s\n",query_address,result_address)

      s.query_socket, _ = s.context.NewSocket(zmq.PULL)
      s.query_socket.Connect(query_address)
      s.result_socket, _ = s.context.NewSocket(zmq.PUSH)
      s.result_socket.Connect(result_address)
      s.state = 2

      if s.index_path != index_path {
         s.index_path = index_path
         if _, err = os.Stat(index_path); err == nil {
            s.handle.Load(index_path)
            if DEBUG { fmt.Println("Loading", index_path) }
         } else if _, err = os.Stat(data_path); err == nil {
            s.handle.Build(data_path)
            if DEBUG { fmt.Println("Building from", data_path) }
         } else {
            s.query_socket.Send([]byte("ERR data/index not found."),0)
            fmt.Printf("%s and %s not found", data_path,index_path)
         }
      } else if DEBUG {
         fmt.Println("Skip building & loading index.")
      }
   }
}

// -----------------------------------------------------------------------

func (s *Server) ProcessQuery() {
   var msg []byte
   var items []string
   var query, result string
   var qid int64

   pi := zmq.PollItems{
      zmq.PollItem{Socket: s.query_socket, Events: zmq.POLLIN},
      zmq.PollItem{Socket: s.sub_socket, Events: zmq.POLLIN},
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
         result = s.handle.Process(int(qid), query)
         s.result_socket.Send([]byte(fmt.Sprintf("%d %s", qid,result)), 0)

         if DEBUG { fmt.Println("Query:",qid,query,"\nResult:",result) }

      // wait for END signal broadcast from client
      case pi[1].REvents&zmq.POLLIN != 0:
         msg, _ = pi[1].Socket.Recv(0)
         if string(msg) == "END" {
            s.state = 1
            s.query_socket.Close()
            s.result_socket.Close()
            if DEBUG { fmt.Println("Service ended") }
            return
         }
      }
   }
}

