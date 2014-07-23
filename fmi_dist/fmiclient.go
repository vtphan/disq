package main

import (
   "github.com/vtphan/disq"
   "fmt"
   "os"
)

type Collector struct {}

func (c *Collector) ProcessResult(qid int, result string) {
   fmt.Println("Client::ProcessResult	", qid, "	", result)
}

func main() {
   c := disq.NewClient(os.Args[1])
   c.Start(os.Args[2], os.Args[3], &Collector{})
}