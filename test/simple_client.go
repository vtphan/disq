package main

import (
   "github.com/vtphan/disq"
   "fmt"
   "os"
)

type Collector struct {}

func (c *Collector) ProcessResult(qid int, result string) {
   fmt.Println("got", result)
}

func main() {
   c := disq.NewClient("127.0.0.1:6000", &Collector{})
   // os.Args[1] = addresses.txt
   c.Run(os.Args[1], "index", "queries.txt")
}