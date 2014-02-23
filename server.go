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
   index_file        string
   handle            ServerInterface
   host              string
   pubsub_port, query_port, result_port   int

   // STOP (0), IDLE (1), ACTIVE (2)
   state             int
}

// -----------------------------------------------------------------------
func NewServer(handle ServerInterface) *Server {
   var err           error
   var host          []byte
   var items         []string
   var address       string

   address, err = ioutil.ReadFile(CONFIG_FILE)

   if err != nil {
      panic(fmt.Sprintf("Problem reading %s.", CONFIG_FILE))
   }

   s := new(Server)
   s.handle = handle
   s.index_file = ""

   items = strings.Split(PUBSUB, ":", 2)
   s.host = items[0]
   s.pubsub_port, _ = strconv.Atoi(items[1])

   s.context, _ = zmq.NewContext()
   s.sub_socket, _ = s.context.NewSocket(zmq.SUB)
   s.sub_socket.SetSubscribe("")
   s.sub_socket.Connect(fmt.Sprintf("tcp://%s:%d", s.host, s.pubsub_port))

   return s
}


// -----------------------------------------------------------------------

func (s *Server) Serve() {
   defer s.sub_socket.Close()

   s.state = 1
   for s.state > 0 {
      if s.state == 1 {  // idle
         d.Listen()
         defer s.query_socket.Close()
         defer s.result_socket.Close()
      } else if s.state == 2 { // active
         d.ProcessQuery()
      }
   }
}

// -----------------------------------------------------------------------

func (s *Server) Listen() {
   var msg, index_file, data_file string
   var items []string

   msg, _ = s.sub_socket.Recv(0)
   items = strings.SplitN(string(msg), " ", 6)
   if items[0] == "REQ" {
      d.query_port, _ = strconv.Atoi(items[1])
      d.result_port, _ = strconv.Atoi(items[2])
      data_file = items[3]
      index_file = items[4]
      DEBUG = (items[5] == "true")
      s.query_socket, _ = s.context.NewSocket(zmq.PULL)
      s.query_socket.Connect(fmt.Sprintf("tcp://%s:%s",s.host,s.query_port))
      s.result_socket, _ = s.context.NewSocket(zmq.PUSH)
      s.result_socket.Connect(fmt.Sprintf("tcp://%s:%s",s.host,s.result_port))
      s.state = 2

      if s.index_file != index_file {
         s.index_file = index_file
         if _, err = os.Stat(index_file); err == nil {
            s.handle.Load(index_file)
            if DEBUG { fmt.Println("Loading", index_file) }
         } else if _, err = os.Stat(data_file); err == nil {
            s.handle.Build(data_file)
            if DEBUG { fmt.Println("Building from", data_file) }
         } else {
            s.state = 1
            s.query_socket.Send([]byte("ERR data/index not found."),0)
            if DEBUG { fmt.Println("Data file & index file not found.") }
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
   var query, result, data_file, index_file string
   var qid int64
   var err error

   pi := zmq.PollItems{
      zmq.PollItem{Socket: s.query_socket, Events: zmq.POLLIN},
      zmq.PollItem{Socket: s.sub_socket, Events: zmq.POLLIN},
   }

   for {
      _, _ = zmq.Poll(pi, -1)
      switch {
      // process query
      case pi[0].REvents&zmq.POLLIN != 0:
         msg, _ = pi[0].Socket.Recv(0)
         items = strings.SplitN(string(msg), " ", 2)
         qid, err = strconv.ParseInt(items[0], 10, 64)
         query = items[1]
         result = s.handle.Process(int(qid), query)
         s.result_socket.Send([]byte(fmt.Sprintf("%d %s", qid,result)), 0)

         if DEBUG { fmt.Println("Query:",qid,query,"\nResult:",result) }

      // wait for END signal broadcast from client
      case pi[1].REvents&zmq.POLLIN != 0:
         msg, _ = pi[1].Socket.Recv(0)
         if string(msg) == "END" {
            s.state = 1
            if DEBUG { fmt.Printf("Service stopped") }
            return
         }
      }
   }
}

