package main

import (
   "fmt"
   "net"
   "bufio"
)
func main() {
   conn, err := net.Dial("tcp", "127.0.0.1:8080")
   if err != nil {
      fmt.Println("Can't dial address")
   } else {
      for i:=0; i<20; i++ {
         fmt.Fprintf(conn, "hello %d\n", i)
         // status, _ := bufio.NewReader(conn).ReadString('\n')
         scanner := bufio.NewScanner(conn)
         scanner.Scan()
         fmt.Println(scanner.Text())
      }
   }
}