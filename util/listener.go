package main

import (
   "net"
   "fmt"
   "bufio"
)
func main() {
   ln, err := net.Listen("tcp", "127.0.0.1:8080")
   if err != nil {
      fmt.Println("can't open port")
   }
   for {
      conn, err := ln.Accept()
      if err != nil {
         fmt.Println("error accepting connection")
      } else {
         go handle(conn)
      }
   }
}

func handle(conn net.Conn) {
   scanner := bufio.NewScanner(conn)
   for scanner.Scan() {
      msg := scanner.Text()
      fmt.Println(msg)
      fmt.Fprintf(conn, "echo %s\n", msg)
   }
}