/*
Author: Vinhthuy Phan, 2014
*/
package disq

import (
   "fmt"
   "log"
   "net"
   "bufio"
   "strings"
   "strconv"
   "sync"
)

type Worker interface {
   ProcessQuery(qid int, query string) string
}

type WorkerStub struct {
   client_conn    net.Conn
   worker         Worker
}

type Node struct {
   addr           string
   data_dir       string
   listener       net.Listener
   init_worker    func(string) Worker
   // clients        map[string]WorkerStub

}

func NewNode(config_file string, setup func(string) Worker) *Node {
   n := new(Node)
   n.init_worker = setup
   // n.clients = make(map[string]WorkerStub)
   n.addr, n.data_dir = ReadNodeConfig(config_file)

   var err error
   n.listener, err = net.Listen("tcp", n.addr)
   if err != nil {
      log.Fatalln("Unable to listen to", n.addr)
   }
   return n
}


func (n *Node) Close() {
   n.listener.Close()
}

/*
   Join node, if addr is not taken, then be the first node.
*/
func (n *Node) Start() {
   defer n.Close()
   log.Println("Listening at", n.addr)
   for {
      conn, err := n.listener.Accept()
      fmt.Println("Starting to accept queries from client", conn)
      if err == nil {
         go n.handle_connection(conn)
      }
   }
}

/*
   Each connection is dedicated to each client
*/
func (n *Node) handle_connection(conn net.Conn) {
   var worker Worker
   var wg sync.WaitGroup
   var items []string
   var qid int
   var result string
   no_more_queries := make(chan bool)
   scanner := bufio.NewScanner(conn)

   go func() {
      <-no_more_queries    // all queries have been received
      wg.Wait()            // all queries have been processed
      conn.Close()
   }()

   for scanner.Scan() {
      items = strings.Split(strings.Trim(scanner.Text(), "\n\r"), " ")

      switch (items[0]) {
      case "handshake":
         worker = n.init_worker(items[1])

      case "query":
         qid, _ = strconv.Atoi(items[1])
         wg.Add(1)
         go func(query_id int, query string) {
            // fmt.Println("Got", query_id, query)
            defer wg.Done()
            result = worker.ProcessQuery(query_id, query)
            fmt.Fprintf(conn, "%d %s\n", query_id, result)
         }(qid, items[2])

      case "done":
         no_more_queries <- true

      default:
         log.Fatalln("Unknown message type", items)
      }
   }
}
