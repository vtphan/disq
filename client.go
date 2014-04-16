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
   nodes          map[string]net.Conn
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
   c.nodes = make(map[string]net.Conn)
   return c
}


func (c *Client) request_service(addr, query_file string) {
   conn, err := net.Dial("tcp", addr)
   if err != nil {
      log.Println("Unable to connect to node", addr)
   }
   defer conn.Close()
   fmt.Printf("[%s] request service from %s\n", c.addr, addr)
   fmt.Fprintf(conn, "request %s %s", c.addr, query_file)
}


func (c *Client) Run(node_addr, query_file string, collector CollectorInterface) {
   var err error
   var conn net.Conn

   c.collector = collector
   go c.request_service(node_addr, query_file)

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
      stop = c.handle_message(mesg, scanner)
   }
}

func (c *Client) handle_message(mesg string, scanner *bufio.Scanner) bool {
   var items []string

   items = strings.Split(mesg, " ")
   switch (items[0]) {
   case "accept":
      c.add_node(items[1])

   default:
      log.Fatalln("Unknown message type", mesg)
   }
   return false
}


func (c *Client) add_node(addr string) {
   _, ok := c.nodes[addr]
   if ok {
      c.nodes[addr].Close()
   }

   conn, err := net.Dial("tcp", addr)
   if err != nil {
      log.Println(err)
      if ok {
         delete(c.nodes, addr)
      }
   }

   c.nodes[addr] = conn

   for k,v := range c.nodes {
      fmt.Println(k,v)
   }
}
