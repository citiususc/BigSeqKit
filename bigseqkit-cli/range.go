package main

import (
	"bigseqkit"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runRange(input []*api.IDataFrame[string], cmd *cobra.Command, args []string) *api.IDataFrame[string] {
	opts := parseSeqKitRangeOptions(cmd)
	results := make([]*api.IDataFrame[string], len(input))
	for i := range input {
		results[i] = check(bigseqkit.Range(input[i], opts))
	}
	return union(cmd, results...)
}

func parseSeqKitRangeOptions(cmd *cobra.Command) *bigseqkit.SeqKitRangeOptions {
	return (&bigseqkit.SeqKitRangeOptions{}).
		Config(parseSeqKitConfig(cmd)).
		Range(getFlagString(cmd, "range"))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "range",
			Short: "print FASTA/Q records in a range (start:end)",
			Long: `print FASTA/Q records in a range (start:end)
Examples:
  1. leading 100 records (head -n 100)
      seqkit range -r 1:100
  2. last 100 records (tail -n 100)
      seqkit range -r -100:-1
  3. remove leading 100 records (tail -n +101)
      seqkit range -r 101:-1
  4. other ranges:
      seqkit range -r 10:100
      seqkit range -r -100:-10
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runRange)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().StringP("range", "r", "", `range. e.g., 1:12 for first 12 records (head -n 12), -12:-1 for last 12 records (tail -n 12)`)
	})
}
