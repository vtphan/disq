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

type Worker interface {
   ProcessQuery(qid int, query string) string
}

type WorkerStub struct {
   conn     net.Conn
   worker   Worker
}

type Node struct {
   addr           string
   data_dir       string
   listener       net.Listener
   init_worker    func(string) Worker
   clients        map[string]WorkerStub

}

func NewNode(config_file string, setup func(string) Worker) *Node {
   n := new(Node)
   n.init_worker = setup
   n.clients = make(map[string]WorkerStub)
   n.addr, n.data_dir = ReadConfig(config_file)

   var err error
   n.listener, err = net.Listen("tcp", n.addr)
   if err != nil {
      log.Fatalln("Unable to listen to", n.addr)
   }
   return n
}


func (n *Node) Close() {
   n.listener.Close()
   for _, c := range(n.clients) {
      c.conn.Close()
   }
}

/*
   Join node, if addr is not taken, then be the first node.
*/
func (n *Node) Start() {
   defer n.Close()
   log.Println("Listening at", n.addr)
   for {
      conn, err := n.listener.Accept()
      if err == nil {
         go n.handle_connection(conn)
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
      case "handshake":
         fmt.Println("handshake", items[1])
         addr, query_file := items[1], items[2]
         conn, err := net.Dial("tcp", addr)
         if err != nil {
            log.Println(err)
         } else {
            n.clients[addr] = WorkerStub{conn, n.init_worker(query_file)}
         }

      case "query":
         fmt.Println("query", items)
         client := n.clients[items[1]]
         qid, _ := strconv.Atoi(items[2])
         q := items[3]
         go client.worker.ProcessQuery(qid, q)

      default:
         log.Fatalf("[%s] unknown message type: %s\n", n.addr, mesg)
      }
   }
}





