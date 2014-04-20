package main

import (
   "github.com/vtphan/disq"
   "fmt"
)

func main() {
   c := disq.NewClient("config-client.json")
   c.Start("index", "queries.txt", nil)
}