package main

import (
	"bigseqkit"
	"fmt"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runConcat(input []*api.IDataFrame[string], cmd *cobra.Command, args []string, pipe bool) *api.IDataFrame[string] {
	if len(input) < 2 {
		checkError(fmt.Errorf("at least 2 files needed"))
	}
	opts := parseSeqKitConcatOptions(cmd)
	result := input[0]
	for i := 1; i < len(input); i++ {
		result = check(bigseqkit.Concat(result, input[i], opts))
	}
	return result
}

func parseSeqKitConcatOptions(cmd *cobra.Command) *bigseqkit.SeqKitConcatOptions {
	return (&bigseqkit.SeqKitConcatOptions{}).
		Config(parseSeqKitConfig(cmd)).
		Separator(getFlagString(cmd, "separator")).
		Full(getFlagBool(cmd, "full"))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:     "concat",
			Aliases: []string{"concate"},
			Short:   "concatenate sequences with the same ID from multiple files",
			Long: `concatenate sequences with same ID from multiple files
Attentions:
   1. By default, only sequences with IDs that appear in all files are outputted.
      use -f/--full to output all sequences.
   2. If there are more than one sequences of the same ID, we output the Cartesian
      product of sequences.
   3. Description are also concatenated with a separator (-s/--separator).
   4. Order of sequences with different IDs are random.
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runConcat)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().BoolP("full", "f", false, "keep all sequences, like full/outer join")
		cmd.Flags().StringP("separator", "s", "|", "separator for descriptions of records with the same ID")
	})
}
