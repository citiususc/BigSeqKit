package main

import (
	"bigseqkit"
	"fmt"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runSample(input []*api.IDataFrame[string], cmd *cobra.Command, args []string) *api.IDataFrame[string] {
	if len(input) != 1 {
		checkError(fmt.Errorf("only 1 file needed"))
	}
	opts := parseSeqKitSampleOptions(cmd)
	return check(bigseqkit.Sample(input[0], opts))
}

func parseSeqKitSampleOptions(cmd *cobra.Command) *bigseqkit.SeqKitSampleOptions {
	return (&bigseqkit.SeqKitSampleOptions{}).
		Config(parseSeqKitConfig(cmd)).
		Seed(int(getFlagInt64(cmd, "rand-seed"))).
		Number(int(getFlagInt64(cmd, "number"))).
		Proportion(float32(getFlagFloat64(cmd, "proportion")))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "sample",
			Short: "sample sequences by number or proportion",
			Long: `sample sequences by number or proportion.
Attention:
1. Do not use '-n' on large FASTQ files, it loads all seqs into memory!
   use 'seqkit sample -p 0.1 seqs.fq.gz | seqkit head -n N' instead!
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runSample)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().Int64P("rand-seed", "s", 11, "rand seed")
		cmd.Flags().Int64P("number", "n", 0, "sample by number (result may not exactly match), DO NOT use on large FASTQ files.")
		cmd.Flags().Float64P("proportion", "p", 0, "sample by proportion")
		cmd.Flags().BoolP("two-pass", "2", false, "2-pass mode read files twice to lower memory usage. Not allowed when reading from stdin")
	})
}
