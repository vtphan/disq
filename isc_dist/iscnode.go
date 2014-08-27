package main

import (
   "github.com/vtphan/disq"
   "os"
   "strconv"   
   "github.com/namsyvo/ISC"
   "strings"
   "path"
   "runtime"
   "encoding/gob"
   "fmt"
   "bytes"
)

type MyWorker struct { 
   snpcaller *isc.SNPProf
   align_info []isc.AlignInfo
   match_pos [][]int
}

// var idx *fmi.Index

func format_result(snp []isc.SNP) string{
   var network bytes.Buffer
   enc := gob.NewEncoder(&network)
   _ = enc.Encode(snp)
   return string(network.Bytes())
}

func GenerateReads(read1 string, read2 string) isc.ReadInfo {
   read_info := isc.ReadInfo{}

   if len(read1) > 0 && len(read2) > 0 {
      read_info.AssignReads([]byte(read1), []byte(read2))
      read_info.CalcRevComp()
   }
   return read_info
}

func (m *MyWorker) ProcessQuery(query string, i int) string {
   fmt.Println(i)
   var items []string
   items = strings.Split(query, ":")
   fmt.Println(items)

   read_info := GenerateReads(items[0], items[1])
   SNPs := (m.snpcaller).FindSNP(read_info, m.align_info[i], m.match_pos[i])
   if len(SNPs) > 0 {
      fmt.Println(SNPs)
      return string(format_result(SNPs))
   }
   fmt.Println("return empty")
   return "empty"
}

func (m *MyWorker) New(input_file string) disq.Worker {
   var items []string
   items = strings.Split(input_file, ":")

   _, genome_file_name := path.Split(items[0])
   multigenome_file := path.Join(items[2], genome_file_name) + ".mgf"
   rev_multigenome_file := path.Join(items[2], genome_file_name) + "_rev.mgf"
   _, dbsnp_file_name := path.Split(items[1])
   snp_prof_file := path.Join(items[2], dbsnp_file_name) + ".idx"


   input_info := isc.InputInfo{}
   input_info.Genome_file = multigenome_file
   input_info.SNP_file = snp_prof_file
   input_info.Index_file = multigenome_file + ".index/"
   input_info.Rev_index_file = rev_multigenome_file + ".index/"
   input_info.Read_file_1 = items[3]
   input_info.Read_file_2 = items[4]
   input_info.SNP_call_file = items[5]
   input_info.Search_mode,_ = strconv.Atoi(items[8])
   input_info.Start_pos,_ = strconv.Atoi(items[9])
   input_info.Search_step,_ = strconv.Atoi(items[10])
   input_info.Proc_num,_ = strconv.Atoi(items[11])
   input_info.Routine_num,_ = strconv.Atoi(items[12])
   if input_info.Proc_num == 0 || input_info.Routine_num == 0 {
      input_info.Proc_num = runtime.NumCPU()
      input_info.Routine_num = runtime.NumCPU()
   }

   para_info := isc.ParaInfo{}
   para_info.Max_match = 32
   para_info.Err_var_factor = 4
   para_info.Iter_num_factor = 1
   seq_err,_ := strconv.ParseFloat(items[7], 64)
   read_len,_ := strconv.Atoi(items[6])
   para_info.Seq_err = float32(seq_err)
   para_info.Read_len = read_len

   fmt.Println(input_info.Routine_num)
   align_info := make([]isc.AlignInfo, input_info.Routine_num)
   for i := 0; i < input_info.Routine_num; i++ {
      align_info[i].InitAlignInfo(read_len)
   }
   match_pos := make([][]int, input_info.Routine_num)
   for i := 0; i < input_info.Routine_num; i++ {
      match_pos[i] = make([]int, isc.MAXIMUM_MATCH)
   }

   var snpcaller isc.SNPProf
   snpcaller.Init(input_info, para_info)

   w := new(MyWorker)
   w.snpcaller = &snpcaller
   w.align_info = align_info
   w.match_pos = match_pos
   return w
}

func main() {
   m := new(isc.SNPProf)
   var n []isc.AlignInfo
   var g [][]int
   node := disq.NewNode(os.Args[1], &MyWorker{m,n,g})
   node.Start()
}