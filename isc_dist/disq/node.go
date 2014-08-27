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
   ProcessQuery(query string, i int) string
   // PreProcessReads(query_id int, query string, queries chan Query)
   New(input_file string) Worker
}
type Query struct{
   Query_id int
   Query_co string
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
   no_more_queries1 := make(chan bool)
   no_more_queries2 := make(chan bool)

   go func() {
      <-no_more_queries1   // all queries have been received
      fmt.Println("close conn1")
      wg.Wait()            // all queries have been processed
      fmt.Println("close conn2")
      conn.Close()
   }()

   var worker Worker       // a worker is allocated for each client/conn
   var items []string
   var qid int
   var result string
   var routinenum = 8
   queries := make(chan Query)
   go func() {
      <-no_more_queries2   // all queries have been received
      fmt.Println("close queries")
      close(queries)
   }()

   scanner := bufio.NewScanner(conn)
   for scanner.Scan() {
      items = strings.Split(strings.Trim(scanner.Text(), "\n\r"), " ")
      switch (items[0]) {
      case "handshake":
         fmt.Println(items[1])
         worker = n.init_worker.New(items[1])

      case "query":
         qid, _ = strconv.Atoi(items[1])

         go func(query_id int, query string, queries chan Query) {
            wg.Add(1)
            query_info := Query{query_id, query}
            queries <- query_info
         }(qid, items[2], queries)

         for mm := 0; mm < routinenum; mm++ {
            go func(queries chan Query, m int) {
               for query_info := range queries {
                  defer wg.Done()
                  query_id := query_info.Query_id
                  query := query_info.Query_co
                  result = worker.ProcessQuery(query, m)
                  fmt.Fprintf(conn, "%d %s:", query_id, result)
                  fmt.Println(query_id)
               }
            }(queries, mm)
         }

      case "done":
         no_more_queries1 <- true
         no_more_queries2 <- true


      default:
         log.Fatalln("Unknown message type", items)
      }
   }
}
