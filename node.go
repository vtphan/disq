package disq

import (
   "fmt"
   "log"
   "net"
   "bufio"
   "strings"
)

type Node {
   addr        string
   peers       map[string]bool
   listener    net.Listener
}

func NewNode(addr, peer_addr string) *Node {
   var err error
   n := new(Node)
   n.addr = addr
   n.listener, err = net.Listen("tcp", n.addr)
   if err != nil {
      log.Fatalln("Unable to connect to", addr)
      return nil
   }
   go n.Listen()
   n.peers = make(map[string]bool)
   if peer_addr != nil {
      n.peer_greet(peer_addr)
   }
}


func (n *Node) Listen() {
   for {
      conn, err := n.listener.Accept()
      if err == nil {
         go n.handle_connection(conn)
      }
   }
}


func (n *Node) handle_connection(conn net.Conn) {
   var items []string

   scanner := bufio.NewScanner(conn)
   scanner.Scan()
   line := scanner.Text()
   fmt.Println(n.addr, "receive", line)

   items = strings.SplitN(line, " ", 2)
   switch (items[0]) {
   case "hi":
      n.peer_update(items[1])
   }
}


func (n *Node) peer_greet(peer_addr string) {
   conn, err := net.Dial("tcp", peer_addr)
   if err != nil {
      log.Println("Unable to connect to peer", peer_addr)
      return n
   }
   defer conn.Close()
   fmt.Println("greet", peer_addr)
   fmt.Fprintf(peer_conn, "hi %s", n.addr)
   n.peers[peer_addr] = true
}


func (n *Node) peer_update(peer_addr string) {
   if len(n.peers) == 0 {
      return
   }
   conn, err := net.Dial("tcp", peer_adddr)
   if err != nil {
      log.Println("Unable to update to", peer_addr)
   }
   defer conn.Close()
   msg := "update "
   for addr := range n.peers {
      msg += addr + " "
   }
   fmt.Println("update", peer_addr, msg)
   fmt.Fprintf(conn, msg)
}