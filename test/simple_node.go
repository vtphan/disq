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
   mesg := fmt.Sprintf("[%d Eatching %s]", qid, query)
   return mesg
}

func Setup(filename string) disq.Worker {
   fmt.Println("\tSimpleNode.Setup", filename)
   w := new(myWorker)
   return w
}

func main() {
   node := disq.NewNode(os.Args[1], Setup)
   node.Start()
}