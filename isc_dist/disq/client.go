/*
Author: Vinhthuy Phan, 2014
*/
package disq

import (
   "net"
   "log"
   "fmt"
   "strings"
   "bufio"
   "strconv"
   "sync"
   "bytes"
)

type CollectorInterface interface {
   // GenerateQuery()
   ProcessResult(qid int, result string)
}

type NodeStub struct {
   addr  string
   conn  net.Conn
}

type Client struct {
   nodes          []NodeStub
   collector      CollectorInterface
   config_file    string
   mode        string  // 0: distributed; 1: broadcast
}

func NewClient(config_file string) *Client {
   c := new(Client)
   c.config_file = config_file
   return c
}

func (c *Client) Start(index_file string, query_file chan Query, collector CollectorInterface) {
   c.collector = collector

   // Connect and distribute queries
   c.connect(index_file)
   if c.mode == "1" {
      go func(qfile chan Query) {
         c.distribute_queries(qfile)
      }(query_file)
   } else {
      go func(qfile chan Query) {
         c.broadcast_queries(qfile)
      }(query_file)
   }

   // Collect and process results
   results := make(chan string)
   c.collect_results(results)

   for r := range(results) {
      items := strings.SplitN(r, " ", 2)
      qid, err := strconv.Atoi(items[0])
      if err != nil {
         log.Fatalln("Missing query id", items)
      }
      res := items[1]
      c.collector.ProcessResult(qid, res)
   }
}

func (c *Client) collect_results(results chan string) {
   var wg sync.WaitGroup

   wg.Add(len(c.nodes))

   go func() {
      wg.Wait()
      close(results)
   }()

   for _, node := range(c.nodes) {
      go func(conn net.Conn) {
         defer wg.Done()
         scanner := bufio.NewScanner(conn)
         split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
            if atEOF && len(data) == 0 {
               return 0, nil, nil
            }
            if i := bytes.IndexByte(data, ':'); i >= 0 {
               return i + 1, dropCR(data[0:i]), nil
            }
            if atEOF {
               return len(data), dropCR(data), nil
            }
            return 0, nil, nil
         }
         scanner.Split(split)
         for scanner.Scan() {
            results <- scanner.Text()
         }
         conn.Close()
      }(node.conn)
   }
}

func dropCR(data []byte) []byte {
   if len(data) > 0 && data[len(data)-1] == '\r' {
      return data[0 : len(data)-1]
   }
   return data
}

func (c *Client) connect(index_file string) {
   no_connection := true
   addresses, flag := ReadClientConfig(c.config_file)
   for _, addr := range(addresses) {
      conn, err := net.Dial("tcp", addr)
      if err == nil {
         c.nodes = append(c.nodes, NodeStub{addr, conn})
         fmt.Fprintf(conn, "handshake %s\n", index_file)
         log.Println("connect to", addr)
         no_connection = false
      }
   }
   c.mode = flag
   if no_connection {
      log.Fatalln("Cannot connect to any node.")
   }
}

func (c *Client) distribute_queries(queries chan Query) {
   for query := range queries {
      for _, node := range(c.nodes) {
         fmt.Fprintf(node.conn,"query %d %s\n",query.Query_id, query.Query_co)
      }
   }

   for _, node := range(c.nodes) {
      fmt.Fprintf(node.conn, "done\n")
   }
}

func (c *Client) broadcast_queries(queries chan Query) {
   for _, node := range(c.nodes) {
      for query := range queries {
         fmt.Fprintf(node.conn,"query %d %s\n", query.Query_id, query.Query_co)
      }
   }

   for _, node := range(c.nodes) {
      fmt.Fprintf(node.conn, "done\n")
   }
}
