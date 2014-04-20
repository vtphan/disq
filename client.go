package disq

import (
   "net"
   "log"
   "fmt"
   "strings"
   "bufio"
   "os"
   "strconv"
   // "time"
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
}


func (c *Client) send_queries(query_file string) {
   file, e := os.Open(query_file)
   if e != nil {
      log.Fatalln("Unable to open file", query_file)
   }
   defer file.Close()
   scanner := bufio.NewScanner(file)
   stop := false
   i := 0
   for ! stop {
      for _, node := range c.nodes {
         if stop {
            break
         } else if scanner.Scan() {
            fmt.Fprintf(node, "query %s %d %s\n", c.addr, i, scanner.Text())
            i++
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
   return c
}

/*
   Start distributing queries in query_file to nodes whose addresses are
   stored in node_addr_file.
*/
func (c *Client) Start(index_file, query_file string, collector CollectorInterface) {
   var err error
   var conn net.Conn

   c.collector = collector
   c.listener, err = net.Listen("tcp", c.addr)
   if err != nil {
      log.Fatalln("Unable to listen to", c.addr)
   }

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
   }(index_file, query_file)

   // Now listen for response from nodes
   for {
      conn, err = c.listener.Accept()
      if err == nil {
         go c.handle_connection(conn)
      }
   }
}


func (c *Client) handle_connection(conn net.Conn) {
   var items []string
   stop := false
   scanner := bufio.NewScanner(conn)
   for !stop && scanner.Scan() {
      mesg := strings.Trim(scanner.Text(), "\n\r")
      items = strings.SplitN(mesg, " ", 2)
      qid, err := strconv.Atoi(items[0])
      result := items[1]
      if err != nil {
         log.Fatalln("Missing query id", items)
      }
      if c.collector == nil {
         fmt.Println(">", qid, result)
      } else {
         c.collector.ProcessResult(qid, result)
      }
   }
}


