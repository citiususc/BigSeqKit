package main

import (
	"bigseqkit"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runFq2Fa(input []*api.IDataFrame[string], cmd *cobra.Command, args []string, pipe bool) *api.IDataFrame[string] {
	opts := parseSeqKitFq2FaOptions(cmd)
	results := make([]*api.IDataFrame[string], len(input))
	for i := range input {
		results[i] = check(bigseqkit.Fq2Fa(input[i], opts))
	}
	return union(cmd, results...)
}

func parseSeqKitFq2FaOptions(cmd *cobra.Command) *bigseqkit.SeqKitFq2FaOptions {
	return (&bigseqkit.SeqKitFq2FaOptions{}).
		Config(parseSeqKitConfig(cmd))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "fq2fa",
			Short: "convert FASTQ to FASTA",
			Long: `convert FASTQ to FASTA
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runFq2Fa)
			},
		}
		parent.AddCommand(cmd)
	})
}
