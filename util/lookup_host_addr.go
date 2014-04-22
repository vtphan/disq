package main

import (
   "fmt"
   "net"
   "os"
)

func find_addr() []string {
   var addrs []string
   name, err := os.Hostname()
   if err != nil {
      fmt.Printf("Cannot lookup hostname: %v\n", err)
      return addrs
   }

   ips, err := net.LookupHost(name)
   if err != nil {
      fmt.Printf("Cannot lookup host: %v\n", err)
      return addrs
   }
   for _, a := range ips {
      con, e := net.Listen("tcp", a + ":0")
      if e == nil {
         addrs = append(addrs, con.Addr().String())
         con.Close()
      }
   }
   return addrs
}

func main() {
   fmt.Println(find_addr())
}
