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
         return "breakfast menu: cereal, orange juice"
      case "snack":
         return "snack menu: cookies"
      case "lunch":
         return "lunch menu: chicken, salad, noodles"
      case "dinner":
         return "dinner menu: steak, salad, water"
      case "latenight":
         return "latenight menu: red wine"
   }
   return "others: chocolate"
}

func main() {
   w := disq.NewServer(Handle{})
   w.Serve()
}