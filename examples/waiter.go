package main

import (
   "github.com/vtphan/disq"
   "fmt"
)

type Handle struct {}

func (t Handle) Build(f string) {
   fmt.Println("Buidn index", f)
}

func (t Handle) Load(f string) {
   fmt.Println("Load index", f)
}

func (t Handle) Process(qid int, query string) string {
   switch query {
      case "breakfast":
         return "cereal, orange juice"
      case "snack":
         return "cookies"
      case "dinner":
         return "steak, salad, water"
      case "latenight":
         return "red wine"
   }
   return "chocolate"
}

func main() {
   w := disq.NewServer(Handle{})
   w.Serve()
}