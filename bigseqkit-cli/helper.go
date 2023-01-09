package main

import (
	"bigseqkit"
	"bufio"
	"fmt"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/spf13/cobra"
	"ignis/driver/api"
	"os"
	"strings"
)

var commands = make([]func(*cobra.Command), 0, 30)
var jobWorker *api.IWorker
var jobInput []*api.IDataFrame[string]
var jobOuput *api.IDataFrame[string]
var fOuput func()

func addCommand(f func(*cobra.Command)) {
	commands = append(commands, f)
}

func check[T any](e T, err error) T {
	if err != nil {
		panic(err)
	}
	return e
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func extension(s string, es []string) bool {
	for _, e := range es {
		if strings.HasSuffix(strings.ToLower(s), e) {
			return true
		}
	}
	return false
}

func readSeqs(cmd *cobra.Command, args []string, pipe bool) []*api.IDataFrame[string] {
	flag := true
	if cmd.Use == "faidx" {
		flag = false
		if pipe {
			return make([]*api.IDataFrame[string], 0)
		}
	}

	files := getFileListFromArgsAndFile(cmd, args, flag, "infile-list", flag)
	input := make([]*api.IDataFrame[string], len(files))

	for i, file := range files {
		fileInfo := check(os.Stat(file))
		if fileInfo.IsDir() {
			input[i] = check(jobWorker.PartitionTextFile(file))
		} else if extension(file, []string{".fa", ".fna", ".ffn", ".faa", ".frn"}) {
			input[i] = check(bigseqkit.ReadFASTA(file, jobWorker))
		} else if extension(file, []string{".fq", ".fastq"}) {
			input[i] = check(bigseqkit.ReadFASTQ(file, jobWorker))
		} else {
			f := check(os.Open(file))
			buff := []byte{0}
			check(f.Read(buff))
			checkError(f.Close())
			if buff[0] == '>' {
				input[i] = check(bigseqkit.ReadFASTA(file, jobWorker))
			} else if buff[0] == '@' {
				input[i] = check(bigseqkit.ReadFASTQ(file, jobWorker))
			} else {
				panic(fmt.Errorf(" <file> must be fasta or fastq"))
			}
		}
		if !flag {
			break
		}
	}
	return input
}

func ignisDriver(cmd *cobra.Command, args []string,
	f func(input []*api.IDataFrame[string], cmd *cobra.Command, args []string, pipe bool) *api.IDataFrame[string]) {
	if jobInput != nil {
		jobInput = append(jobInput, readSeqs(cmd, args, true)...)
		jobOuput = f(jobInput, cmd, args, true)
		if getFlagInt(cmd, "partitions") > 0 {
			jobOuput = check(jobOuput.Repartition(int64(getFlagInt(cmd, "partitions")), true, false))
		}
		return
	}

	check(0, api.Ignis.Start())
	defer api.Ignis.Stop()

	cluster := check(api.NewIClusterDefault())
	jobWorker = check(api.NewIWorkerDefault(cluster, "go"))

	output := f(readSeqs(cmd, args, false), cmd, args, false)
	if getFlagInt(cmd, "partitions") > 0 {
		output = check(output.Repartition(int64(getFlagInt(cmd, "partitions")), true, false))
	}
	if output != nil {
		if getFlagBool(cmd, "merge") {
			checkError(bigseqkit.StoreFASTX(output, getFlagString(cmd, "out-file")))
		} else {
			checkError(bigseqkit.StoreFASTXN(output, getFlagString(cmd, "out-file")))
		}
	}
	if fOuput != nil {
		fOuput()
	}
}

func union(cmd *cobra.Command, input ...*api.IDataFrame[string]) *api.IDataFrame[string] {
	result := input[0]
	order := getFlagBool(cmd, "order")
	for i := 1; i < len(input); i++ {
		result = check(result.Union(input[i], order, nil))
	}
	return result
}

func parseSeqKitConfig(cmd *cobra.Command) *bigseqkit.SeqKitConfig {
	return (&bigseqkit.SeqKitConfig{}).
		SeqType(getFlagString(cmd, "seq-type")).
		LineWidth(getFlagNonNegativeInt(cmd, "line-width")).
		IDRegexp(getIDRegexp(cmd, "id-regexp")).
		IDNCBI(getFlagBool(cmd, "id-ncbi")).
		Quiet(getFlagBool(cmd, "quiet")).
		AlphabetGuessSeqLength(getFlagAlphabetGuessSeqLength(cmd, "alphabet-guess-seq-length"))
}

func Parser() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bigseqkit",
		Short: "BigSeqKit: a Big Data approach to process FASTA/FASTQ files at scale",
		Long: `BigSeqKit: a Big Data approach to process FASTA/FASTQ files at scale
`,
	}

	cmd.PersistentFlags().StringP("seq-type", "t", "auto", "sequence type (dna|rna|protein|unlimit|auto) (for auto, it automatically detect by the first sequence)")
	//cmd.PersistentFlags().IntP("threads", "j", defaultThreads, "number of CPUs. can also set with environment variable SEQKIT_THREADS)")
	cmd.PersistentFlags().IntP("line-width", "w", 60, "line width when outputting FASTA format (0 for no wrap)")
	cmd.PersistentFlags().StringP("id-regexp", "", fastx.DefaultIDRegexp, "regular expression for parsing ID")
	cmd.PersistentFlags().BoolP("id-ncbi", "", false, "FASTA head is NCBI-style, e.g. >gi|110645304|ref|NC_002516.2| Pseud...")
	cmd.PersistentFlags().StringP("out-file", "o", "", `out file`)
	cmd.PersistentFlags().BoolP("quiet", "", false, "be quiet and do not show extra information")
	cmd.PersistentFlags().IntP("alphabet-guess-seq-length", "", 10000, "length of sequence prefix of the first FASTA record based on which seqkit guesses the sequence type (0 for whole seq)")
	cmd.PersistentFlags().StringP("infile-list", "", "", "file of input files list (one file per line), if given, they are appended to files from cli arguments")

	cmd.PersistentFlags().BoolP("merge", "", false, "store all results in a single file. (default false, faster)")
	cmd.PersistentFlags().IntP("partitions", "", 0, "set number of partitions to store the output (0 is auto)")
	cmd.PersistentFlags().BoolP("order", "", false, "preserve the order of the sequences when there is more than one input file. (default false, faster)")

	cmd.CompletionOptions.DisableDefaultCmd = true
	cmd.SetHelpCommand(&cobra.Command{Hidden: true})

	for i := range commands {
		commands[i](cmd)
	}

	return cmd
}

func getFileList(args []string, checkFile bool) []string {
	files := make([]string, 0, 1000)
	for _, file := range args {

		if !checkFile {
			continue
		}
		if _, err := os.Stat(file); os.IsNotExist(err) {
			checkError(err)
		}
	}
	files = args

	return files
}

func getFileListFromFile(file string, checkFile bool) ([]string, error) {
	fh, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("read file list from '%s': %s", file, err)
	}

	var _file string
	lists := make([]string, 0, 1000)
	scanner := bufio.NewScanner(fh)
	for scanner.Scan() {
		_file = scanner.Text()
		if strings.TrimSpace(_file) == "" {
			continue
		}
		if checkFile {
			if _, err = os.Stat(_file); os.IsNotExist(err) {
				return lists, fmt.Errorf("check file '%s': %s", _file, err)
			}
		}
		lists = append(lists, _file)
	}
	if err = scanner.Err(); err != nil {
		return nil, fmt.Errorf("read file list from '%s': %s", file, err)
	}

	return lists, nil
}

func getFileListFromArgsAndFile(cmd *cobra.Command, args []string, checkFileFromArgs bool, flag string, checkFileFromFile bool) []string {
	infileList := getFlagString(cmd, flag)
	files := getFileList(args, checkFileFromArgs)
	if infileList != "" {
		_files, err := getFileListFromFile(infileList, checkFileFromFile)
		checkError(err)
		if len(_files) == 0 {
			return files
		}

		files = append(files, _files...)
	}
	return files
}

func getFlagInt(cmd *cobra.Command, flag string) int {
	value, err := cmd.Flags().GetInt(flag)
	checkError(err)
	return value
}

func getFlagPositiveInt(cmd *cobra.Command, flag string) int {
	value, err := cmd.Flags().GetInt(flag)
	checkError(err)
	if value <= 0 {
		checkError(fmt.Errorf("value of flag --%s should be greater than 0", flag))
	}
	return value
}

func getFlagNonNegativeInt(cmd *cobra.Command, flag string) int {
	value, err := cmd.Flags().GetInt(flag)
	checkError(err)
	if value < 0 {
		checkError(fmt.Errorf("value of flag --%s should be greater than 0", flag))
	}
	return value
}

func getFlagBool(cmd *cobra.Command, flag string) bool {
	value, err := cmd.Flags().GetBool(flag)
	checkError(err)
	return value
}

func getFlagString(cmd *cobra.Command, flag string) string {
	value, err := cmd.Flags().GetString(flag)
	checkError(err)
	return value
}

func getFlagFloat64(cmd *cobra.Command, flag string) float64 {
	value, err := cmd.Flags().GetFloat64(flag)
	checkError(err)
	return value
}

func getFlagInt64(cmd *cobra.Command, flag string) int64 {
	value, err := cmd.Flags().GetInt64(flag)
	checkError(err)
	return value
}

func getFlagStringSlice(cmd *cobra.Command, flag string) []string {
	value, err := cmd.Flags().GetStringSlice(flag)
	checkError(err)
	return value
}

func getIDRegexp(cmd *cobra.Command, flag string) string {
	var idRegexp string
	f := getFlagBool(cmd, "id-ncbi")
	if f {
		// e.g. >gi|110645304|ref|NC_002516.2| Pseudomonas aeruginosa PAO1 chromosome, complete genome
		// NC_002516.2 is ID
		idRegexp = `\|([^\|]+)\| `
	} else {
		idRegexp = getFlagString(cmd, "id-regexp")
	}
	return idRegexp
}

func getAlphabet(cmd *cobra.Command, flag string) *seq.Alphabet {
	value, err := cmd.Flags().GetString(flag)
	checkError(err)

	switch strings.ToLower(value) {
	case "dna":
		return seq.DNAredundant
	case "rna":
		return seq.RNAredundant
	case "protein":
		return seq.Protein
	case "unlimit":
		return seq.Unlimit
	case "auto":
		return nil
	default:
		checkError(fmt.Errorf("invalid sequence type: %s, available value: dna|rna|protein|unlimit|auto", value))
		return nil
	}
}

func getFlagAlphabetGuessSeqLength(cmd *cobra.Command, flag string) int {
	alphabetGuessSeqLength := getFlagNonNegativeInt(cmd, flag)
	if alphabetGuessSeqLength > 0 && alphabetGuessSeqLength < 1000 {
		checkError(fmt.Errorf("value of flag --%s too small, should >= 1000", flag))
	}
	return alphabetGuessSeqLength
}

func getFlagValidateSeqLength(cmd *cobra.Command, flag string) int {
	validateSeqLength := getFlagNonNegativeInt(cmd, flag)
	if validateSeqLength > 0 && validateSeqLength < 1000 {
		checkError(fmt.Errorf("value of flag --%s too small, should >= 1000", flag))
	}
	return validateSeqLength
}

var regionExample = `
 1-based index    1 2 3 4 5 6 7 8 9 10
negative index    0-9-8-7-6-5-4-3-2-1
           seq    A C G T N a c g t n
           1:1    A
           2:4      C G T
         -4:-2                c g t
         -4:-1                c g t n
         -1:-1                      n
          2:-2      C G T N a c g t
          1:-1    A C G T N a c g t n
          1:12    A C G T N a c g t n
        -12:-1    A C G T N a c g t n
`
