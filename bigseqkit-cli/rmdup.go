package main

import (
	"bigseqkit"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runRmDup(input []*api.IDataFrame[string], cmd *cobra.Command, args []string) *api.IDataFrame[string] {
	opts := parseSeqKitRmDupOptions(cmd)
	return check(bigseqkit.RmDup(union(cmd, input...), opts))
}

func parseSeqKitRmDupOptions(cmd *cobra.Command) *bigseqkit.SeqKitRmDupOptions {
	return (&bigseqkit.SeqKitRmDupOptions{}).
		Config(parseSeqKitConfig(cmd)).
		BySeq(getFlagBool(cmd, "by-seq")).
		ByName(getFlagBool(cmd, "by-name")).
		IgnoreCase(getFlagBool(cmd, "ignore-case")).
		DupSeqsFile(getFlagString(cmd, "dup-seqs-file")).
		DupNumFile(getFlagString(cmd, "dup-num-file")).
		OnlyPositiveStrand(getFlagBool(cmd, "only-positive-strand"))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "rmdup",
			Short: "remove duplicated sequences by ID/name/sequence",
			Long: `remove duplicated sequences by ID/name/sequence
Attentions:
  1. When comparing by sequences, both positive and negative strands are
     compared. Switch on -P/--only-positive-strand for considering the
     positive strand only.
  2. Only the first record is saved for duplicates.
     
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runRmDup)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().BoolP("by-name", "n", false, "by full name instead of just id")
		cmd.Flags().BoolP("by-seq", "s", false, "by seq")
		cmd.Flags().BoolP("ignore-case", "i", false, "ignore case")
		cmd.Flags().StringP("dup-seqs-file", "d", "", "file to save duplicated seqs")
		cmd.Flags().StringP("dup-num-file", "D", "", "file to save number and list of duplicated seqs")
		// cmd.Flags().BoolP("consider-revcom", "r", false, "considering the reverse compelment sequence")
		cmd.Flags().BoolP("only-positive-strand", "P", false, "only considering positive strand when comparing by sequence")
	})
}
