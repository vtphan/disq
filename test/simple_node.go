package main

import (
   "github.com/vtphan/disq"
   "fmt"
   "os"
)

var (
   address = ""
   join_address = ""
)

type myWorker struct { }

func (m *myWorker) ProcessQuery(qid int, query string) string {
   fmt.Println("ProcessQuery", qid, query)
   return "Process " + string(qid) + " " + query
}

func Setup(filename string) disq.WorkerInterface {
   fmt.Println("SimpleNode.Setup", filename)
   w := new(myWorker)
   return w
}

func main() {
   node := disq.NewNode(Setup)
   node.Join(os.Args[1])  // addresses.txt
}