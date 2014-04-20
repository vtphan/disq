/*
Author: Vinhthuy Phan
- nodes form a clique.
- multiple clients.
*/
package disq

import (
   "fmt"
   "log"
   "net"
   "bufio"
   "strings"
   "strconv"
   // "time"
)

type WorkerInterface interface {
   ProcessQuery(qid int, query string) string
}

type Worker struct {
   client_conn    net.Conn
   work           WorkerInterface
}

type Node struct {
   addr           string
   group          map[string]net.Conn
   listener       net.Listener
   init_worker    func(string) WorkerInterface
   clients        map[string]Worker

}

func NewNode(setup func(string) WorkerInterface) *Node {
   n := new(Node)
   n.init_worker = setup
   n.clients = make(map[string]Worker)
   n.group = make(map[string]net.Conn)
   return n
}


func (n *Node) Close() {
   n.listener.Close()
}

/*
   Join node, if addr is not taken, then be the first node.
*/
func (n *Node) Join(addr_file string) {
   var err error
   free_addr, taken_addr := ScanAddresses(addr_file)
   if free_addr == "" {
      log.Fatalln("There is no free address in", addr_file)
   }

   n.addr = free_addr
   n.listener, err = net.Listen("tcp", n.addr)
   n.add_to_group(n.addr)

   if taken_addr != "" {
      n.group_join(taken_addr)
   }

   var conn net.Conn
   fmt.Println("Listening on", n.addr)
   for {
      conn, err = n.listener.Accept()
      if err == nil {
         go n.handle_connection(conn)
      }
   }
}


/*
   Join group.  Peer will update all nodes in group.
*/
func (n *Node) group_join(peer_addr string) {
   if n.add_to_group(peer_addr) {
      fmt.Printf("[%s] join via %s\n", n.addr, peer_addr)
      mesg := []byte("join " + n.addr + "\n")
      _, err := n.group[peer_addr].Write(mesg)
      if err != nil {
         fmt.Println("Unable to write to", peer_addr)
         n.group[peer_addr].Close()
         delete(n.group, peer_addr)
      }
   }
}

/*
   Each message sent to a node consists of 2 space-separated strings:
      1. type: join, update, request
      2. address of the party that sends the message.
*/
func (n *Node) handle_connection(conn net.Conn) {
   scanner := bufio.NewScanner(conn)
   var items []string
   for scanner.Scan() {
      mesg := strings.Trim(scanner.Text(), "\n\r")
      items = strings.Split(mesg, " ")
      switch (items[0]) {
      case "join": /* receive a join request from another node */
         fmt.Println("join")
         n.add_to_group(items[1])
         n.group_update()
         n.group_print()

      case "update": /* receive a network update from another node */
         fmt.Println("update")
         n.group = make(map[string]net.Conn)
         for i:=1; i<len(items); i++ {
            n.add_to_group(items[i])
         }
         n.group_print()

      case "handshake":
         fmt.Println("handshake", items[1])
         conn, err := net.Dial("tcp", items[1])
         if err != nil {
            log.Println(err)
         } else {
            n.clients[items[1]] = Worker{conn, n.init_worker(items[2])}
         }

      case "query":
         fmt.Println("query", items)
         client := n.clients[items[1]]
         qid, _ := strconv.Atoi(items[2])
         q := items[3]
         go client.work.ProcessQuery(qid, q)

      default:
         log.Fatalf("[%s] unknown message type: %s\n", n.addr, mesg)
      }
   }
}


func (n *Node) add_to_group(addr string) bool {
   conn, err := net.Dial("tcp", addr)
   if err != nil {
      if addr == n.addr {
         log.Fatalln("Unable to connect to", n.addr)
      }
      log.Println(err)
      return false
   }

   n.group[addr] = conn
   return true
}



func (n *Node) group_addresses() string {
   var group []string
   for addr, _ := range n.group {
      group = append(group, addr)
   }
   return strings.Join(group, " ")
}


/*
   Update all nodes (including self) in group.
*/
func (n *Node) group_update() {
   var err error
   mesg := []byte("update " + n.group_addresses() + "\n")
   for addr, conn := range n.group {
      _, err = conn.Write(mesg)
      if err != nil {
         conn.Close()
         delete(n.group, addr)
         mesg = []byte("update " + n.group_addresses() + "\n")
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




