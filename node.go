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
   New(input_file string) Worker
}

type Node struct {
   addr           string
   data_dir       string
   listener       net.Listener
   init_worker    Worker
}

func NewNode(config_file string, init_worker Worker) *Node {
   var err error
   n := new(Node)
   n.init_worker = init_worker
   n.addr, n.data_dir = ReadNodeConfig(config_file)

   fmt.Println(n.addr)
   fmt.Println(n.data_dir)
   fmt.Println(n.init_worker)
   
   n.listener, err = net.Listen("tcp", n.addr)
   if err != nil {
      log.Fatalln("Unable to listen to", n.addr)
   }
   return n
}

func (n *Node) Close() {
   n.listener.Close()
}

func (n *Node) Start() {
   defer n.Close()
   log.Println("Listening at", n.addr)
   for {
      conn, err := n.listener.Accept()
      if err == nil {
         go n.handle_connection(conn)
      }
   }
}

func (n *Node) handle_connection(conn net.Conn) {
   var wg sync.WaitGroup
   no_more_queries := make(chan bool)

   go func() {
      <-no_more_queries    // all queries have been received
      wg.Wait()            // all queries have been processed
      conn.Close()
   }()

   var worker Worker       // a worker is allocated for each client/conn
   var items []string
   var qid int
   var result string
   routinenum := make(chan int, 1)
   scanner := bufio.NewScanner(conn)
   for scanner.Scan() {
      items = strings.Split(strings.Trim(scanner.Text(), "\n\r"), " ")

      switch (items[0]) {
      case "handshake":
         worker = n.init_worker.New(items[1])

      case "query":
         qid, _ = strconv.Atoi(items[1])
         wg.Add(1)
         go func(query_id int, query string) {
            routinenum <- 1
            // fmt.Println("Got", query_id, query)
            defer wg.Done()
            result = worker.ProcessQuery(query_id, query)
            fmt.Fprintf(conn, "%d %s\n", query_id, result)
            <- routinenum
         }(qid, items[2])

      case "done":
         no_more_queries <- true

      default:
         log.Fatalln("Unknown message type", items)
      }
   }
}
