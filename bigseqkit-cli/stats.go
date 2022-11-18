package main

import (
	"bigseqkit"
	"fmt"
	"github.com/spf13/cobra"
	"ignis/driver/api"
	"strings"
)

func runStats(input []*api.IDataFrame[string], cmd *cobra.Command, args []string, pipe bool) *api.IDataFrame[string] {
	opts := parseSeqKitStatsOptions(cmd)
	head := ""
	body := ""

	for i := range input {
		table := check(bigseqkit.StatsString(fmt.Sprintf("input%d", i), "N/A", input[i], opts))
		lines := strings.Split(table, "\n")
		head = lines[0] + "\n"
		body += lines[0] + "\n"
	}

	fOuput = func() {
		print(head + body)
	}

	return nil
}

func parseSeqKitStatsOptions(cmd *cobra.Command) *bigseqkit.SeqKitStatsOptions {
	return (&bigseqkit.SeqKitStatsOptions{}).
		Config(parseSeqKitConfig(cmd)).
		All(getFlagBool(cmd, "all")).
		Tabular(getFlagBool(cmd, "tabular")).
		SkipErr(getFlagBool(cmd, "skip-err"))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "stats",
			Short: "get Statsuences by region/gtf/bed, including flanking sequences",
			Long: fmt.Sprintf(`get Statsuences by region/gtf/bed, including flanking sequences.
Attentions:
  1. Use "seqkit Stats" for extract subsets of sequences.
     "seqtk Stats seqs.fasta id.txt" equals to
     "seqkit Stats -f id.txt seqs.fasta"
Recommendation:
  1. use plain FASTA file, so seqkit could utilize FASTA index.
The definition of region is 1-based and with some custom design.
Examples:
%s
`, regionExample),
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runStats)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().BoolP("tabular", "T", false, "output in machine-friendly tabular format")
		cmd.Flags().StringP("gap-letters", "G", "- .", "gap letters")
		cmd.Flags().BoolP("all", "a", false, "all statistics, including quartiles of seq length, sum_gap, N50")
		cmd.Flags().BoolP("skip-err", "e", false, "skip error, only show warning message")
		cmd.Flags().StringP("fq-encoding", "E", "sanger", `fastq quality encoding. available values: 'sanger', 'solexa', 'illumina-1.3+', 'illumina-1.5+', 'illumina-1.8+'.`)
		//cmd.Flags().BoolP("basename", "b", false, "only output basename of files")
		//cmd.Flags().StringP("stdin-label", "i", "-", `label for replacing default "-" for stdin`)
	})
}
