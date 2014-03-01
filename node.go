package disq

import (
   "fmt"
   "log"
   "net"
   "bufio"
   "strings"
   "time"
)


type Node struct {
   addr        string
   group       map[string]bool
   listener    net.Listener
}

func NewNode(addr string) *Node {
   var err error
   n := new(Node)
   n.addr = addr
   n.listener, err = net.Listen("tcp", n.addr)
   if err != nil {
      log.Fatalln("Unable to connect to", addr)
      return nil
   }
   n.group = make(map[string]bool)
   n.group[addr] = true
   return n
}


func (n *Node) Join(peer_addr string) {
   fmt.Println("New node listening on", n.addr)

   if peer_addr != "" {
      go func() {
         time.Sleep(100 * time.Millisecond)
         n.group_join(peer_addr)
      }()
   }

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
   items = strings.SplitN(line, " ", 2)
   switch (items[0]) {
   case "join":
      n.group[items[1]] = true
      n.group_update(items[1])
      n.group_print()

   case "update":
      n.group = make(map[string]bool)
      addrs := strings.Split(items[1], " ")
      for _, a := range addrs {
         n.group[a] = true
      }
      n.group_print()

   case "ping":
      break

   default:
      log.Fatalf("[%s] unknown message type: %s\n", n.addr, line)
   }
}

/*
   Join group.  Peer will update all nodes in group.
*/
func (n *Node) group_join(peer_addr string) {
   conn, err := net.Dial("tcp", peer_addr)
   if err != nil {
      log.Println("Unable to connect to peer", peer_addr)
   }
   defer conn.Close()
   fmt.Printf("[%s] join via %s\n", n.addr, peer_addr)
   fmt.Fprintf(conn, "join %s", n.addr)
   n.group[peer_addr] = true
}


/*
   Update all nodes in group if necessary.
   Periodic update can be done by calling group_update("").
*/
func (n *Node) group_update(peer_addr string) {
   var err error
   var conn []net.Conn
   var c net.Conn
   dead_node := false

   group := []string{n.addr}

   for addr := range n.group {
      if addr != n.addr {
         c, err = net.Dial("tcp", addr)
         if err != nil {
            log.Println("Detect dead node", addr)
            delete(n.group, addr)
            dead_node = true
         } else {
            defer c.Close()
            conn = append(conn, c)
            group = append(group, addr)
         }
      }
   }

   if peer_addr != "" || dead_node {
      mesg := "update " + strings.Join(group, " ")
      for _, c := range conn {
         fmt.Fprintf(c, mesg)
      }
   }
}


func (n *Node) group_print() {
   fmt.Printf("Group: ")
   for a := range n.group {
      if a == n.addr {
         fmt.Print("*")
      }
      fmt.Printf("%s ", a)
   }
   fmt.Printf("\n")
}