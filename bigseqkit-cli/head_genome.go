package main

import (
	"bigseqkit"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runHeadGenome(input []*api.IDataFrame[string], cmd *cobra.Command, args []string, pipe bool) *api.IDataFrame[string] {
	opts := parseSeqKitHeadGenomeOptions(cmd)
	results := make([]*api.IDataFrame[string], len(input))
	for i := range input {
		results[i] = check(bigseqkit.HeadGenome(input[i], opts))
	}
	return union(cmd, results...)
}

func parseSeqKitHeadGenomeOptions(cmd *cobra.Command) *bigseqkit.SeqKitHeadGenomeOptions {
	return (&bigseqkit.SeqKitHeadGenomeOptions{}).
		Config(parseSeqKitConfig(cmd)).
		MiniCommonWords(int64(getFlagPositiveInt(cmd, "mini-common-words")))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "head-genome",
			Short: "print sequences of the first genome with common prefixes in name",
			Long: `print sequences of the first genome with common prefixes in name
For a FASTA file containing multiple contigs of strains (see example below),
these's no list of IDs available for retrieving sequences of a certain strain,
while descriptions of each strain share the same prefix.
This command is used to restrieve sequences of the first strain,
i.e., "Vibrio cholerae strain M29".
>NZ_JFGR01000001.1 Vibrio cholerae strain M29 Contig_1, whole genome shotgun sequence
>NZ_JFGR01000002.1 Vibrio cholerae strain M29 Contig_2, whole genome shotgun sequence
>NZ_JFGR01000003.1 Vibrio cholerae strain M29 Contig_3, whole genome shotgun sequence
>NZ_JSTP01000001.1 Vibrio cholerae strain 2012HC-12 NODE_79, whole genome shotgun sequence
>NZ_JSTP01000002.1 Vibrio cholerae strain 2012HC-12 NODE_78, whole genome shotgun sequence
Attention:
  1. Sequences in file should be well organized.
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runHeadGenome)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().IntP("mini-common-words", "m", 1, "minimal shared prefix words")
	})
}
