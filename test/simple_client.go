package main

import (
   "github.com/vtphan/disq"
   "fmt"
)

type Collector struct {}

func (c *Collector) ProcessResult(qid int, result string) {
   fmt.Println("got", result)
}

func main() {
   address := "127.0.0.1:6000"
   request_address := "127.0.0.1:5000"
   c := disq.NewClient(address)
   c.Run(request_address, "query.txt", &Collector{})
}