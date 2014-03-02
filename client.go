package disq

import (
   "net"
   "log"
   "fmt"
   "strings"
   "bufio"
)

type CollectorInterface interface {
   ProcessResult(qid int, result string)
}

type Client struct {
   addr           string
   listener       net.Listener
   collector      CollectorInterface
}


func NewClient(addr string) *Client {
   var err error
   c := new(Client)
   c.addr = addr
   c.listener, err = net.Listen("tcp", c.addr)
   if err != nil {
      log.Fatalln("Unable to connect to", c.addr)
      return nil
   }
   return c
}


func (c *Client) request_service(addr, query_file string) {
   conn, err := net.Dial("tcp", addr)
   if err != nil {
      log.Println("Unable to connect to address", addr)
   }
   defer conn.Close()
   fmt.Printf("[%s] request service from %s\n", c.addr, addr)
   fmt.Fprintf(conn, "request %s %s", c.addr, query_file)
}


func (c *Client) Run(node_addr, query_file string, collector CollectorInterface) {

   c.collector = collector
   go c.request_service(node_addr, query_file)

   var err error
   var conn net.Conn

   for {
      conn, err = c.listener.Accept()
      if err == nil {
         go c.handle_connection(conn)
      }
   }
}


func (c *Client) handle_connection(conn net.Conn) {
   var items []string

   scanner := bufio.NewScanner(conn)
   scanner.Scan()
   line := scanner.Text()
   items = strings.Split(line, " ")
   switch (items[0]) {
   case "accept":
      fmt.Println("accept from", items[1])

   default:
      log.Fatalln("Unknown message type", line)
   }
}