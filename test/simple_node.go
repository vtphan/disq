package main

import (
   "github.com/vtphan/disq"
   "flag"
   "log"
   "fmt"
)

var (
   address = ""
   join_address = ""
)

type myWorker struct { }

func (m *myWorker) Process(qid int, query []string) string {
   fmt.Println("Process", qid, query[0])
   return "Process " + string(qid) + " " + query[0]
}

func Setup(filename string) disq.WorkerInterface {
   fmt.Println("Setup", filename)
   w := new(myWorker)
   return w
}

func main() {
   flag.StringVar(&address, "a", "", "address of this node")
   flag.StringVar(&join_address, "j", "", "address of node to join group")
   flag.Parse()
   if address == "" {
      log.Panic("Must provide address for this node.")
   }

   node := disq.NewNode(address)
   node.Join(join_address, Setup)
}