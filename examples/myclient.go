package main

import (
   "github.com/vtphan/disq"
   "fmt"
)

func print (qid int64, res string) {
   fmt.Println(">", qid, res)
}

func main() {
   config_file := "meal_config.json"
   d := disq.NewClient(config_file)
   d.Run("queries.txt", print)
}