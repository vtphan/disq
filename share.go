package disq

import (
   "os"
   "net"
   "encoding/json"
   "fmt"
   // "bytes"
   // "io/ioutil"
   "bufio"
   "log"
)

func ScanAddresses(filename string) (string, string) {
  file, err := os.Open(filename)
  if err != nil {
    log.Fatalln("Unable to open file",filename)
  }
  scanner := bufio.NewScanner(file)
  free_addr := ""
  taken_addr := ""

  for scanner.Scan() {
    addr := scanner.Text()
    con, e := net.Listen("tcp", addr)
    if e == nil {
      if free_addr == "" {
        free_addr = addr
      }
      con.Close()
    } else {
      if taken_addr == "" {
        taken_addr = addr
      }
    }
  }
  return free_addr, taken_addr
}

/*
A config json-formated file may look like this:
   {
      "address" : "127.0.0.1:5000",
   }
*/
type Config struct {
    Address string
    Data_dir string
}

func ReadConfig(json_file string) (string, string) {
  file, _ := os.Open(json_file)
  defer file.Close()
  decoder := json.NewDecoder(file)
  c := &Config{}
  decoder.Decode(&c)
  return c.Address, c.Data_dir
}

// func WriteConfig(json_file string, addrs string) {
//   var out bytes.Buffer
//   conf := Config{addrs}
//   encoder := json.NewEncoder(&out)
//   encoder.Encode(&conf)
//   ioutil.WriteFile(json_file, out.Bytes(), 0600)
//   fmt.Println("finish encoding", conf)
// }

func FindAddress() []string {
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
        defer con.Close()
      }
   }
   return addrs
}