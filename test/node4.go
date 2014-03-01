package main

import (
   "github.com/vtphan/disq"
   // "fmt"
)

func main() {
   node := disq.NewNode("127.0.0.1:5003")
   node.Run("127.0.0.1:5001")
}