package main

import (
	"bigseqkit"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runHead(input []*api.IDataFrame[string], cmd *cobra.Command, args []string, pipe bool) *api.IDataFrame[string] {
	opts := parseSeqKitHeadOptions(cmd)
	results := make([]*api.IDataFrame[string], len(input))
	for i := range input {
		results[i] = check(bigseqkit.Head(input[i], opts))
	}
	return union(cmd, results...)
}

func parseSeqKitHeadOptions(cmd *cobra.Command) *bigseqkit.SeqKitHeadOptions {
	return (&bigseqkit.SeqKitHeadOptions{}).
		Config(parseSeqKitConfig(cmd)).
		N(int64(getFlagPositiveInt(cmd, "number")))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "head",
			Short: "print first N FASTA/Q records",
			Long: `print first N FASTA/Q records
For returning the last N records, use:
    seqkit range -N:-1 seqs.fasta
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runHead)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().IntP("number", "n", 10, "print first N FASTA/Q records")
	})
}
