package disq

import (
   "fmt"
   "log"
   "net"
   "bufio"
   "strings"
   "time"
)

type WorkerInterface interface {
   Process(qid int, query []string) string
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

func NewNode(addr string) *Node {
   var err error
   n := new(Node)
   n.addr = addr
   n.listener, err = net.Listen("tcp", n.addr)
   if err != nil {
      log.Fatalln("Unable to listen on", addr)
      return nil
   }
   n.group = make(map[string]net.Conn)
   if ! n.add_node(addr) {
      log.Fatalln("Unable to connect to", addr)
      return nil
   }
   n.clients = make(map[string]Worker)
   return n
}


func (n *Node) Close() {
   n.listener.Close()
}


/*
   Codes dealing with nodes
*/
func (n *Node) Join(peer_addr string, setup func(string) WorkerInterface) {
   fmt.Println("New node listening on", n.addr)
   n.init_worker = setup

   if peer_addr != "" {
      go func() {
         time.Sleep(100 * time.Millisecond)
         n.group_join(peer_addr)
      }()
   }

   var err error
   var conn net.Conn

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
   if n.add_node(peer_addr) {
      fmt.Printf("[%s] join via %s\n", n.addr, peer_addr)
      mesg := []byte("join " + n.addr + "\n")
      _, err := n.group[peer_addr].Write(mesg)
      if err != nil {
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
   stop := false
   scanner := bufio.NewScanner(conn)
   for !stop && scanner.Scan() {
      mesg := strings.Trim(scanner.Text(), "\n\r")
      stop = n.handle_message(mesg, scanner)
   }
}

func (n *Node) handle_message(mesg string, scanner *bufio.Scanner) bool {
   var items []string

   items = strings.Split(mesg, " ")
   switch (items[0]) {
   case "join":
      n.add_node(items[1])
      n.group_update(items[1])
      n.group_print()

   case "update":
      n.group = make(map[string]net.Conn)
      for i:=1; i<len(items); i++ {
         n.add_node(items[i])
      }
      n.group_print()

   case "request":
      n.broadcast_adding_client(items[1], items[2])

   case "add":
      n.setup_client(items[1], items[2])

   case "ping":
      break

   default:
      log.Fatalf("[%s] unknown message type: %s\n", n.addr, mesg)

   }

   return false
}


func (n *Node) add_node(addr string) bool {
   _, ok := n.group[addr]
   if ok {
      n.group[addr].Close()
   }

   conn, err := net.Dial("tcp", addr)
   if err != nil {
      log.Println("Fail to add node", addr, err)
      if ok {
         delete(n.group, addr)
      }
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
   Update all nodes in group.  Periodic update is achieved by calling group_update("").
*/
func (n *Node) group_update(peer_addr string) {
   if peer_addr != "" {
      for _, conn := range n.group {
         conn.Write([]byte("update " + n.group_addresses() + "\n"))
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



/*
   Store client, and create a new worker specifically for this client.
*/
func (n *Node) setup_client(addr, query_file string) {
   fmt.Println("setting up client", addr)
   conn, err := net.Dial("tcp", addr)
   if err != nil {
      log.Println("Unable to connect to client", addr)
   }
   n.clients[addr] = Worker{conn, n.init_worker(query_file)}
   fmt.Fprintf(conn, "accept %s\n", n.addr)
}


/*
   Broadcast an "add client" message to all nodes in the group.
*/
func (n *Node) broadcast_adding_client(client_addr, query_file string) {
   // n.group_update("")

   for _, conn := range n.group {
      fmt.Fprintf(conn, "add %s %s\n", client_addr, query_file)
   }
}
