package main

import (
   "github.com/vtphan/disq"
)

func main() {
   c := disq.NewClient("config-client.json")
   c.Start("index", "queries.txt", nil)
}