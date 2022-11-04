package main

import (
	"bigseqkit"
	"fmt"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runSubseq(input []*api.IDataFrame[string], cmd *cobra.Command, args []string) *api.IDataFrame[string] {
	opts := parseSeqKitSubseqOptions(cmd)
	results := make([]*api.IDataFrame[string], len(input))
	for i := range input {
		results[i] = check(bigseqkit.Subseq(input[i], opts))
	}
	return union(cmd, results...)
}

func parseSeqKitSubseqOptions(cmd *cobra.Command) *bigseqkit.SeqKitSubseqOptions {
	return (&bigseqkit.SeqKitSubseqOptions{}).
		Config(parseSeqKitConfig(cmd)).
		Chr(getFlagStringSlice(cmd, "chr")).
		Region(getFlagString(cmd, "region")).
		Gtf(getFlagString(cmd, "gtf")).
		Feature(getFlagStringSlice(cmd, "feature")).
		UpStream(getFlagNonNegativeInt(cmd, "up-stream")).
		DownStream(getFlagNonNegativeInt(cmd, "down-stream")).
		OnlyFlank(getFlagBool(cmd, "only-flank")).
		Bed(getFlagString(cmd, "bed")).
		GtfTag(getFlagString(cmd, "gtf-tag"))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "subseq",
			Short: "get subsequences by region/gtf/bed, including flanking sequences",
			Long: fmt.Sprintf(`get subsequences by region/gtf/bed, including flanking sequences.
Attentions:
  1. Use "seqkit grep" for extract subsets of sequences.
     "seqtk subseq seqs.fasta id.txt" equals to
     "seqkit grep -f id.txt seqs.fasta"
Recommendation:
  1. use plain FASTA file, so seqkit could utilize FASTA index.
The definition of region is 1-based and with some custom design.
Examples:
%s
`, regionExample),
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runSubseq)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().StringSliceP("chr", "", []string{}, "select limited sequence with sequence IDs when using --gtf or --bed (multiple value supported, case ignored)")
		cmd.Flags().StringP("region", "r", "", "by region. "+
			"e.g 1:12 for first 12 bases, -12:-1 for last 12 bases,"+
			` 13:-1 for cutting first 12 bases. type "seqkit subseq -h" for more examples`)

		cmd.Flags().StringP("gtf", "", "", "by GTF (version 2.2) file")
		cmd.Flags().StringSliceP("feature", "", []string{}, `select limited feature types (multiple value supported, case ignored, only works with GTF)`)
		cmd.Flags().IntP("up-stream", "u", 0, "up stream length")
		cmd.Flags().IntP("down-stream", "d", 0, "down stream length")
		cmd.Flags().BoolP("only-flank", "f", false, "only return up/down stream sequence")
		cmd.Flags().StringP("bed", "", "", "by tab-delimited BED file")
		cmd.Flags().StringP("gtf-tag", "", "gene_id", `output this tag as sequence comment`)
	})
}
