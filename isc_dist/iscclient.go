package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/namsyvo/ISC"
	"os"
	"runtime"
	"sync"
	"path"
	"github.com/vtphan/disq"
	"strconv"
    "encoding/gob"
    "bytes"
)

type Collector struct {
	results chan []isc.SNP
}

func (c *Collector) ProcessResult(qid int, result string) {
	var SNPs []isc.SNP
	decOCC := gob.NewDecoder(bytes.NewBuffer([]byte(result)))
	_ = decOCC.Decode(&SNPs)

    fmt.Println("Client::ProcessResult	", qid, SNPs)
    c.results <- SNPs
}

func main() {
	fmt.Println("ISC - Integrated SNP Calling based on Read-Multigenome Alignment")

	fmt.Println("Initializing indexes and parameters...")

	var configure_file = flag.String("c", "", "")
	var genome_file = flag.String("g", "", "reference genome file")
	var dbsnp_file = flag.String("s", "", "snp profile file")
	var idx_dir = flag.String("i", "", "index directory")
	var read_file_1 = flag.String("1", "", "pairend read file, first end")
	var read_file_2 = flag.String("2", "", "pairend read file, second end")
	var snp_call_file = flag.String("o", "", "snp calling file")
	var read_len = flag.Int("l", 100, "read length")
	var seq_err = flag.Float64("e", 0.01, "sequencing error")
	var search_mode = flag.Int("m", 2, "searching mode for finding seeds (1: random, 2: deterministic)")
	var start_pos = flag.Int("p", 0, "starting position on reads for finding seeds")
	var search_step = flag.Int("j", 5, "step for searching in deterministic mode")
	var proc_num = flag.Int("w", 0, "maximum number of CPUs using by Go")
	var routine_num = flag.Int("t", 0, "number of goroutines")
	flag.Parse()

	_, genome_file_name := path.Split(*genome_file)
	multigenome_file := path.Join(*idx_dir, genome_file_name) + ".mgf"
	rev_multigenome_file := path.Join(*idx_dir, genome_file_name) + "_rev.mgf"
	_, dbsnp_file_name := path.Split(*dbsnp_file)
	snp_prof_file := path.Join(*idx_dir, dbsnp_file_name) + ".idx"

	input_info := isc.InputInfo{}
	input_info.Genome_file = multigenome_file
	input_info.SNP_file = snp_prof_file
	input_info.Index_file = multigenome_file + ".index/"
	input_info.Rev_index_file = rev_multigenome_file + ".index/"
	input_info.Read_file_1 = *read_file_1
	input_info.Read_file_2 = *read_file_2
	input_info.SNP_call_file = *snp_call_file
	input_info.Search_mode = *search_mode
	input_info.Start_pos = *start_pos
	input_info.Search_step = *search_step
	input_info.Proc_num = *proc_num
	input_info.Routine_num = *routine_num
	if *proc_num == 0 || *routine_num == 0 {
		input_info.Proc_num = runtime.NumCPU()
		input_info.Routine_num = runtime.NumCPU()
	}

	para_info := isc.ParaInfo{}
	para_info.Max_match = 32
	para_info.Err_var_factor = 4
	para_info.Iter_num_factor = 1
	para_info.Seq_err = float32(*seq_err)
	para_info.Read_len = *read_len

	runtime.GOMAXPROCS(input_info.Proc_num)

	input_info2 := *genome_file+":"+*dbsnp_file+":"+*idx_dir+":"+*read_file_1+":"+*read_file_2+":"+*snp_call_file+":"+strconv.Itoa(*read_len)+":"+strconv.FormatFloat(*seq_err, 'f', 6, 64)+":"+strconv.Itoa(*search_mode)+":"+strconv.Itoa(*start_pos)+":"+strconv.Itoa(*search_step)+":"+strconv.Itoa(*proc_num)+":"+strconv.Itoa(*routine_num)
	var snpcaller isc.SNPProf
	snpcaller.Init(input_info, para_info)
    c := disq.NewClient(*configure_file)

	fmt.Println("Aligning reads to the mutigenome...")

	// data := make(chan isc.ReadInfo, input_info.Routine_num)
	data := make(chan disq.Query, input_info.Routine_num)

	results := make(chan []isc.SNP)

	go GetReads(*read_file_1, *read_file_2, data)

	collect := Collector{results}
    c.Start(input_info2, data, &collect)

	/*var wg sync.WaitGroup
	for i := 0; i < input_info.Routine_num; i++ {
		go ProcessReads(&snpcaller, data, results, &wg, align_info[i], match_pos[i])
	}
	go func() {
		wg.Wait()
		close(results)
	}()
*/
	//Collect SNPS from results channel and update SNPs
	
/*	fmt.Println("start")
	snp_aligned_read_num := 0
	var snp isc.SNP
	for SNPs := range collect.results {
		snp_aligned_read_num++
		for _, snp = range SNPs {
			snpcaller.SNP_Prof[snp.SNP_Idx] = append(snpcaller.SNP_Prof[snp.SNP_Idx], snp.SNP_Val)
		}
	}

	fmt.Println("Calling SNPs from alignment results...")

	snpcaller.CallSNP(input_info.Routine_num)
	snpcaller.SNPCall_tofile(input_info.SNP_call_file)

	fmt.Println("Finish, check the file", input_info.SNP_call_file, "for results")*/
}

//--------------------------------------------------------------------------------------------------
//Read input FASTQ files and put data into data channel
//--------------------------------------------------------------------------------------------------
func GetReads(fn1 string, fn2 string, data chan disq.Query) {
	f1, err_f1 := os.Open(fn1)
	if err_f1 != nil {
		panic("Error opening input read file " + fn1)
	}
	defer f1.Close()
	f2, err_f2 := os.Open(fn2)
	if err_f2 != nil {
		panic("Error opening input read file " + fn2)
	}
	defer f2.Close()

	scanner1 := bufio.NewScanner(f1)
	scanner2 := bufio.NewScanner(f2)
	count := 0
	var line_f1, line_f2 []byte
	for scanner1.Scan() && scanner2.Scan() {
		//ignore 1st lines in input FASTQ files
		scanner1.Scan() //use 2nd line in input FASTQ file 1
		scanner2.Scan() //use 2nd line in input FASTQ file 2
		line_f1 = scanner1.Bytes()
		line_f2 = scanner2.Bytes()
		if len(line_f1) > 0 && len(line_f2) > 0 {
			read_info2 := string(line_f1)+":"+string(line_f2)
			query := disq.Query{count, read_info2}
			data <- query
			count++
		}

		scanner1.Scan() //ignore 3rd line in 1st input FASTQ file 1
		scanner2.Scan() //ignore 3rd line in 2nd input FASTQ file 2
		scanner1.Scan() //ignore 4th line in 1st input FASTQ file 1
		scanner2.Scan() //ignore 4th line in 2nd input FASTQ file 2
	}
	close(data)
}

//--------------------------------------------------------------------------------------------------
//Take data from data channel, process them (find SNPs) and put results (SNPs) into results channel
//--------------------------------------------------------------------------------------------------
func ProcessReads(snpcaller *isc.SNPProf, data chan isc.ReadInfo, results chan []isc.SNP,
	wg *sync.WaitGroup, align_info isc.AlignInfo, match_pos []int) {
	wg.Add(1)
	defer wg.Done()
	var read_info isc.ReadInfo
	var SNPs []isc.SNP
	for read_info = range data {
		SNPs = (*snpcaller).FindSNP(read_info, align_info, match_pos)
		if len(SNPs) > 0 {
			results <- SNPs
		}
	}
}