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
   "os"
   "strconv"
)

type CollectorInterface interface {
   ProcessResult(qid int, result string)
}

type Client struct {
   addr           string
   listener       net.Listener
   collector      CollectorInterface
   nodes          map[string]net.Conn
   output_dir     string
   node_addresses []string
   count          chan int
   done_dist      chan bool
}


func (c *Client) send_queries(query_file string) {
   file, e := os.Open(query_file)
   if e != nil {
      log.Fatalln("Unable to open file", query_file)
   }
   defer file.Close()
   scanner := bufio.NewScanner(file)
   stop := false
   var count int
   for ! stop {
      for _, node := range c.nodes {
         if stop {
            break
         } else if scanner.Scan() {
            count =<- c.count
            fmt.Fprintf(node,"query %s %d %s\n",c.addr,count,scanner.Text())
            c.count <- count + 1
         } else {
            stop = true
         }
      }
   }
}


func NewClient(config_file string) *Client {
   c := new(Client)
   c.nodes = make(map[string]net.Conn)
   c.addr, c.node_addresses, c.output_dir = ReadClientConfig(config_file)
   c.done_dist = make(chan bool, 1)
   c.count = make(chan int, 1)
   c.count <- 0
   return c
}

func (c *Client) Start(index_file, query_file string, collector CollectorInterface) {
   var err error
   var conn net.Conn

   c.collector = collector
   c.listener, err = net.Listen("tcp", c.addr)
   if err != nil {
      log.Fatalln("Unable to listen to", c.addr)
   }

   // Connect to nodes and distribute queries
   go func(ifile, qfile string) {
      for _, addr := range(c.node_addresses) {
         conn, e := net.Dial("tcp", addr)
         if e == nil {
            c.nodes[addr] = conn
            fmt.Fprintf(conn, "handshake %s %s\n", c.addr, ifile)
            log.Println("connect to",addr)
         }
      }
      c.send_queries(qfile)
      c.done_dist <- true
   }(index_file, query_file)

   // Now listen for response from nodes
   stop := false
   for !stop {
      conn, err = c.listener.Accept()
      if err == nil {
         go c.handle_connection(conn)
      } else {
         stop = true
      }
   }
}


func (c *Client) handle_connection(conn net.Conn) {
   var items []string
   scanner := bufio.NewScanner(conn)
   for scanner.Scan() {
      mesg := strings.Trim(scanner.Text(), "\n\r")
      items = strings.SplitN(mesg, " ", 2)
      qid, err := strconv.Atoi(items[0])
      result := items[1]
      if err != nil {
         log.Fatalln("Missing query id", items)
      }
      if c.collector == nil {
         // to do: save to output file.
         fmt.Println(">", qid, result)
      } else {
         c.collector.ProcessResult(qid, result)
      }
      c.count <- (<-c.count)-1
      c.check_to_close()
   }
}


func (c *Client) check_to_close(){
   count :=<- c.count
   done_dist :=<- c.done_dist
   if done_dist && count == 0 {
      for _, node := range c.nodes {
         fmt.Fprintf(node, "stop %s\n", c.addr)
         node.Close()
      }
      c.listener.Close()
   }
   c.count <- count
   c.done_dist <- done_dist
}
