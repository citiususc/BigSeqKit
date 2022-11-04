package main

import (
	"bigseqkit"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runRename(input []*api.IDataFrame[string], cmd *cobra.Command, args []string) *api.IDataFrame[string] {
	opts := parseSeqKitRenameOptions(cmd)
	results := make([]*api.IDataFrame[string], len(input))
	for i := range input {
		results[i] = check(bigseqkit.Rename(input[i], opts))
	}
	return union(cmd, results...)
}

func parseSeqKitRenameOptions(cmd *cobra.Command) *bigseqkit.SeqKitRenameOptions {
	return (&bigseqkit.SeqKitRenameOptions{}).
		Config(parseSeqKitConfig(cmd)).
		ByName(getFlagBool(cmd, "by-name"))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "rename",
			Short: "rename duplicated IDs",
			Long: `rename duplicated IDs
Attention:
  1. This command only appends "_N" to duplicated sequence IDs to make them unique.
  2. Use "seqkit replace" for editing sequence IDs/headers using regular expression.
Example:
    $ seqkit seq seqs.fasta 
    >id comment
    actg
    >id description
    ACTG
    $ seqkit rename seqs.fasta
    >id comment
    actg
    >id_2 description
    ACTG
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runRename)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().StringSliceP("chr", "", []string{}, "select limited sequence with sequence IDs when using --gtf or --bed (multiple value supported, case ignored)")
		cmd.Flags().StringP("region", "r", "", "by region. "+
			"e.g 1:12 for first 12 bases, -12:-1 for last 12 bases,"+
			` 13:-1 for cutting first 12 bases. type "seqkit Rename -h" for more examples`)

		cmd.Flags().BoolP("by-name", "n", false, "check duplication by full name instead of just id")
		//cmd.Flags().BoolP("multiple-outfiles", "m", false, "write results into separated files for multiple input files")
		//cmd.Flags().StringP("out-dir", "O", "renamed", "output directory")
		//cmd.Flags().BoolP("force", "f", false, "overwrite output directory")
		// renameCmd.Flags().BoolP("inplace", "i", false, "rename ID in-place")
	})
}
