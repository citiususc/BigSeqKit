package main

import (
	"bigseqkit"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runShuffle(input []*api.IDataFrame[string], cmd *cobra.Command, args []string, pipe bool) *api.IDataFrame[string] {
	opts := parseSeqKitShuffleOptions(cmd)
	results := make([]*api.IDataFrame[string], len(input))
	for i := range input {
		results[i] = check(bigseqkit.Shuffle(input[i], opts))
	}
	return union(cmd, results...)
}

func parseSeqKitShuffleOptions(cmd *cobra.Command) *bigseqkit.SeqKitShuffleOptions {
	return (&bigseqkit.SeqKitShuffleOptions{}).
		Config(parseSeqKitConfig(cmd)).
		Seed(int(getFlagInt64(cmd, "rand-seed")))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "shuffle",
			Short: "shuffle sequences",
			Long: `shuffle sequences.
By default, all records will be readed into memory.
For FASTA format, use flag -2 (--two-pass) to reduce memory usage. FASTQ not
supported.
Firstly, seqkit reads the sequence IDs. If the file is not plain FASTA file,
seqkit will write the sequences to temporary files, and create FASTA index.
Secondly, seqkit shuffles sequence IDs and extract sequences by FASTA index.
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runShuffle)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().Int64P("rand-seed", "s", 23, "rand seed for shuffle")
		cmd.Flags().BoolP("two-pass", "2", false, "two-pass mode read files twice to lower memory usage. (only for FASTA format)")
		cmd.Flags().BoolP("keep-temp", "k", false, "keep temporary FASTA and .fai file when using 2-pass mode")
	})
}
