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

type Worker struct { }

func (m *Worker) ProcessQuery(qid int, query string) string {
   mesg := fmt.Sprintf("[%d Eatching %s]", qid, query)
   return mesg
}

func NewWorker(filename string) disq.Worker {
   fmt.Println("\tSimpleNode.NewWorker", filename)
   w := new(Worker)
   return w
}

func main() {
   node := disq.NewNode(os.Args[1], NewWorker)
   node.Start()
}