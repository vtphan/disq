package main

import (
   "github.com/vtphan/disq"
   "fmt"
)

type Collector struct {}

func (c *Collector) ProcessResult(qid int, result string) {
   fmt.Println("Client::ProcessResult", qid, result)
}

func main() {
   c := disq.NewClient("config-client.json")
   c.Start("index", "queries.txt", &Collector{})
}