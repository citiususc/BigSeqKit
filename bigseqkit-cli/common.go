package main

import (
	"bigseqkit"
	"fmt"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runCommon(input []*api.IDataFrame[string], cmd *cobra.Command, args []string) *api.IDataFrame[string] {
	if len(input) < 2 {
		checkError(fmt.Errorf("at least 2 files needed"))
	}
	opts := parseSeqKitCommonOptions(cmd)
	return check(bigseqkit.Common(input[0], input[1], opts, input[2:]...))
}

func parseSeqKitCommonOptions(cmd *cobra.Command) *bigseqkit.SeqKitCommonOptions {
	return (&bigseqkit.SeqKitCommonOptions{}).
		Config(parseSeqKitConfig(cmd)).
		BySeq(getFlagBool(cmd, "by-seq")).
		ByName(getFlagBool(cmd, "by-name")).
		IgnoreCase(getFlagBool(cmd, "ignore-case")).
		OnlyPositiveStrand(getFlagBool(cmd, "only-positive-strand"))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "common",
			Short: "find common sequences of multiple files by id/name/sequence",
			Long: `find common sequences of multiple files by id/name/sequence
Note:
  1. 'seqkit common' is designed to support 2 and MORE files.
  2. When comparing by sequences, both positive and negative strands are
     compared. Switch on -P/--only-positive-strand for considering the
     positive strand only.
  3. For 2 files, 'seqkit grep' is much faster and consumes lesser memory:
     seqkit grep -f <(seqkit seq -n -i small.fq.gz) big.fq.gz # by seq ID
     seqkit grep -s -f <(seqkit seq -s small.fq.gz) big.fq.gz # by seq
  4. Some records in one file may have same sequences/IDs. They will ALL be
     retrieved if the sequence/ID was shared in multiple files.
     So the records number may be larger than that of the smallest file.
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runCommon)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().BoolP("by-name", "n", false, "match by full name instead of just id")
		cmd.Flags().BoolP("by-seq", "s", false, "match by sequence")
		cmd.Flags().BoolP("ignore-case", "i", false, "ignore case")
		// commonCmd.Flags().BoolP("consider-revcom", "r", false, "considering the reverse compelment sequence")
		cmd.Flags().BoolP("only-positive-strand", "P", false, "only considering positive strand when comparing by sequence")
	})
}
