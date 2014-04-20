package disq

import (
   "net"
   "log"
   "fmt"
   "strings"
   "bufio"
   "os"
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
}


func NewClient(addr string, collector CollectorInterface) *Client {
   c := new(Client)
   c.collector = collector
   c.addr = addr
   c.nodes = make(map[string]net.Conn)
   return c
}


func (c *Client) do_handshake(node_addr_file, index_file string) {
   file, e := os.Open(node_addr_file)
   if e != nil {
      log.Fatalln("Unable to open file", node_addr_file)
   }
   defer file.Close()
   scanner := bufio.NewScanner(file)
   for scanner.Scan() {
      addr := scanner.Text()
      conn, e := net.Dial("tcp", addr)
      if e == nil {
         c.nodes[addr] = conn
         fmt.Fprintf(conn, "handshake %s %s\n", c.addr, index_file)
      }
   }
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

func (c *Client) Run(node_addr_file, index_file, query_file string) {
   var err error
   var conn net.Conn

   c.listener, err = net.Listen("tcp", c.addr)
   if err != nil {
      log.Fatalln("Unable to listen to", c.addr)
   }

   go func() {
      c.do_handshake(node_addr_file, index_file)
      c.send_queries(query_file)
   }()

   for {
      conn, err = c.listener.Accept()
      if err == nil {
         go c.handle_connection(conn)
      }
   }
}


func (c *Client) handle_connection(conn net.Conn) {
   stop := false
   scanner := bufio.NewScanner(conn)
   for !stop && scanner.Scan() {
      mesg := strings.Trim(scanner.Text(), "\n\r")
      fmt.Println(mesg)
   }
}


