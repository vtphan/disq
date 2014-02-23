package main

import (
   "fmt"
   "os"
   "encoding/json"
)
type Configuration struct {
    Ports   []int
    DataPath  string
    IndexPath  string
}

func main() {
   file, _ := os.Open("config_test.json")
   decoder := json.NewDecoder(file)
   configuration := &Configuration{}
   decoder.Decode(&configuration)

   fmt.Println(configuration)
   // for i,v := range configuration.Ports {
   //    fmt.Println(i,v)
   // }
   // fmt.Println(configuration.DataPath)
}
