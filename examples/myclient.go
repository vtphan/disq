package main

import (
   "github.com/vtphan/disq"
   "fmt"
   "os"
)

func print (qid int64, res string) {
   fmt.Println(">", qid, res)
}

func main() {
   if len(os.Args) < 3 {
      fmt.Println("must provide config.json queries.txt")
   }
   d := disq.NewClient(os.Args[1])
   d.Run(os.Args[2], print)
}