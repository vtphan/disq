package main

import (
   "fmt"
   "net"
   "bufio"
   // "time"
)
func main() {
   conn, err := net.Dial("tcp", "127.0.0.1:8080")
   if err != nil {
      fmt.Println("Can't dial address")
   } else {
      result := make(chan string)
      go func() {
         defer close(result)
         scanner := bufio.NewScanner(conn)
         for scanner.Scan() {
            msg := scanner.Text()
            if msg == "stop" {
               break
            } else {
               result <- msg
            }
         }
      }()

      for i:=0; i<20; i++ {
         fmt.Fprintf(conn, "hello %d\n", i)
         // status, _ := bufio.NewReader(conn).ReadString('\n')
      }
      fmt.Fprintf(conn, "done\n")

      // for stop:=false; !stop; {
      //    select {
      //    case r :=<- result:
      //       fmt.Println(r)
      //    case <-time.After(1 * time.Second):
      //       stop = true
      //    }
      // }
      for r := range(result) {
         fmt.Println(r)
      }
   }
}