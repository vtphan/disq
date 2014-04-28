package main

import (
   "github.com/vtphan/disq"
   "fmt"
   "os"
)

type MyWorker struct { }

func (m *MyWorker) ProcessQuery(qid int, query string) string {
   mesg := fmt.Sprintf("[%d Process query %s]", qid, query)
   fmt.Println(mesg)
   return mesg
}

func Configure(input_file string) disq.Worker {
   fmt.Println("\tSimpleNode.Configure", input_file)
   w := new(MyWorker)
   return w
}

func main() {
   node := disq.NewNode(os.Args[1], Configure)
   node.Start()
}