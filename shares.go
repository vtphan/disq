
package disq

import (
   // "fmt"
   "os"
   "encoding/json"
)


var (
   DEBUG = true
   TIMING = false
   CONFIG_FILE = "config.txt"
   PUBSUB = "127.0.0.1:5555"
)

/*
A config json-formated file may look like this:
   {
      "Ports" : [5556, 5557],
      "DataPath" : "genome.txt",
      "IndexPath" : "genome.index"
   }
*/
type Config struct {
    Ports   []int
    DataPath  string
    IndexPath  string
}

func ReadConfig(json_file string) (int, int, string, string) {
   file, _ := os.Open(json_file)
   decoder := json.NewDecoder(file)
   c := &Configuration{}
   decoder.Decode(&c)
   return c.Ports[0], c.Ports[1], c.DataPath, c.IndexPath
}