/*
Author: Vinhthuy Phan, 2014
*/
package disq

import (
   "os"
   "encoding/json"
   "log"
)

type NodeConfig struct {
  Address string
  DataDir string
}

type ClientConfig struct {
  Address     string
  Nodes       []string
  OutputDir   string
}

func ReadNodeConfig(json_file string) (string, string) {
  file, err := os.Open(json_file)
  if err != nil {
    log.Fatalln("Unable to open", json_file)
  }
  defer file.Close()
  decoder := json.NewDecoder(file)
  c := &NodeConfig{}
  decoder.Decode(&c)
  return c.Address, c.DataDir
}

func ReadClientConfig(json_file string) (string, []string, string) {
  file, err := os.Open(json_file)
  if err != nil {
    log.Fatalln("Unable to open", json_file)
  }
  defer file.Close()
  decoder := json.NewDecoder(file)
  c := &ClientConfig{}
  decoder.Decode(&c)
  return c.Address, c.Nodes, c.OutputDir
}

// func WriteConfig(json_file string, addrs string) {
//   var out bytes.Buffer
//   conf := Config{addrs}
//   encoder := json.NewEncoder(&out)
//   encoder.Encode(&conf)
//   ioutil.WriteFile(json_file, out.Bytes(), 0600)
//   fmt.Println("finish encoding", conf)
// }

// func FindAddress() []string {
//    var addrs []string
//    name, err := os.Hostname()
//    if err != nil {
//       fmt.Printf("Cannot lookup hostname: %v\n", err)
//       return addrs
//    }

//    ips, err := net.LookupHost(name)
//    if err != nil {
//       fmt.Printf("Cannot lookup host: %v\n", err)
//       return addrs
//    }
//    for _, a := range ips {
//       con, e := net.Listen("tcp", a + ":0")
//       if e == nil {
//         addrs = append(addrs, con.Addr().String())
//         defer con.Close()
//       }
//    }
//    return addrs
// }
// func ScanAddresses(filename string) (string, string) {
//   file, err := os.Open(filename)
//   if err != nil {
//     log.Fatalln("Unable to open file",filename)
//   }
//   scanner := bufio.NewScanner(file)
//   free_addr := ""
//   taken_addr := ""

//   for scanner.Scan() {
//     addr := scanner.Text()
//     con, e := net.Listen("tcp", addr)
//     if e == nil {
//       if free_addr == "" {
//         free_addr = addr
//       }
//       con.Close()
//     } else {
//       if taken_addr == "" {
//         taken_addr = addr
//       }
//     }
//   }
//   return free_addr, taken_addr
// }