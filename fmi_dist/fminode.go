package main

import (
   "github.com/vtphan/disq"
   fmi "github.com/CoralGao/fmindex"
   "os"
   "strconv"   
   "strings"
   "fmt"
      "log"
)

type MyWorker struct { 
   idx *fmi.Index
}

// var idx *fmi.Index

func format_result(index string, intarray []int) []byte{
   str := index
   for i := 0; i < len(intarray); i++ {
      str = str + "  "+ strconv.Itoa(intarray[i]) + "  "
   }
   return []byte(str)
}

func (m *MyWorker) ProcessQuery(qid int, query string) string {
   result := fmi.Search(m.idx, []byte(query))
   fmt.Println(format_result(query, result))
   return string(format_result(query, result))
}

func (m *MyWorker) New(input_file string) disq.Worker {
   sequencename := strings.TrimRight(input_file, ".fm")
   var filename = ""
      if _, err := os.Stat(input_file); os.IsNotExist(err) {
         if _, err := os.Stat(sequencename); os.IsNotExist(err) {
            log.Fatal("Exit!")
         } else {
            filename = input_file
            fmt.Printf("No such index: %s, build it now!\n", filename)
            m.idx = fmi.Build(sequencename)
         }
      } else {
         filename = input_file
         m.idx = fmi.Load(input_file)
         fmt.Println("Download the index file.")
      }

/*   fmt.Println("Load index", input_file)
   idx = fmi.Load(input_file)*/
   fmt.Println("\tSimpleNode.Configure", input_file)
   w := new(MyWorker)
   w.idx = m.idx

   // w := MyWorker{m.idx}
   return w
}

func main() {
   m := new(fmi.Index)
   node := disq.NewNode(os.Args[1], &MyWorker{m})
   node.Start()
}