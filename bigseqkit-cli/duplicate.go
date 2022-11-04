package main

import (
	"bigseqkit"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runDuplicate(input []*api.IDataFrame[string], cmd *cobra.Command, args []string) *api.IDataFrame[string] {
	opts := parseSeqKitDuplicateOptions(cmd)
	results := make([]*api.IDataFrame[string], len(input))
	for i := range input {
		results[i] = check(bigseqkit.Duplicate(input[i], opts))
	}
	return union(cmd, results...)
}

func parseSeqKitDuplicateOptions(cmd *cobra.Command) *bigseqkit.SeqKitDuplicateOptions {
	return (&bigseqkit.SeqKitDuplicateOptions{}).
		Config(parseSeqKitConfig(cmd)).
		Times(int64(getFlagPositiveInt(cmd, "times")))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:     "duplicate",
			Aliases: []string{"dup"},
			Short:   "duplicate sequences N times",
			Long: `duplicate sequences N times
You may need "seqkit rename" to make the the sequence IDs unique.
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runDuplicate)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().IntP("times", "n", 1, "duplication number")
	})
}
