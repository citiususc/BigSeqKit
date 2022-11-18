package main

import (
	"bigseqkit"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runFa2Fq(input []*api.IDataFrame[string], cmd *cobra.Command, args []string, pipe bool) *api.IDataFrame[string] {
	opts := parseSeqKitFa2FqOptions(cmd)
	results := make([]*api.IDataFrame[string], len(input))
	for i := range input {
		results[i] = check(bigseqkit.Fa2Fq(input[i], opts))
	}
	return union(cmd, results...)
}

func parseSeqKitFa2FqOptions(cmd *cobra.Command) *bigseqkit.SeqKitFa2FqOptions {
	return (&bigseqkit.SeqKitFa2FqOptions{}).
		Config(parseSeqKitConfig(cmd)).
		FastaFile(getFlagString(cmd, "fasta-file")).
		OnlyPositiveStrand(getFlagBool(cmd, "only-positive-strand"))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "fa2fq",
			Short: "retrieve corresponding FASTQ records by a FASTA file",
			Long: `retrieve corresponding FASTQ records by a FASTA file
Attention:
  1. We assume the FASTA file comes from the FASTQ file,
     so they share sequence IDs, and sequences in FASTA
     should be subseq of sequences in FASTQ file.
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runFa2Fq)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().StringP("fasta-file", "f", "", "FASTA file)")
		cmd.Flags().BoolP("only-positive-strand", "P", false, "only search on positive strand")
	})
}
