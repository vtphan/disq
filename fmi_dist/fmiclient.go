package main

import (
   "github.com/vtphan/disq"
   "fmt"
   "os"
   "log"
   "math"
   "bufio"
)

type Collector struct {}

func (c *Collector) ProcessResult(qid int, result string) {
   fmt.Println("Client::ProcessResult	", qid, "	", result)
}

func main() {
   c := disq.NewClient(os.Args[1])
   queries := make(chan disq.Query, 1)

   go func(query_file string, queries chan disq.Query) {
   		file, e := os.Open(query_file)
   		if e != nil {
   			log.Fatalln("Unable to open file", query_file)
   		}
   		defer file.Close()

   		scanner := bufio.NewScanner(file)
   		for stop,count:=false,0; !stop; {
   			if scanner.Scan() {
   				if math.Mod(float64(count),4) == 1 {
   					query := disq.Query{count/4, scanner.Text()}
   					queries <- query
   				}
               count++
   			} else {
	   			stop = true
	   			break
   			}
   		}
   }(os.Args[3], queries)

   c.Start(os.Args[2], queries, &Collector{})
}