package main

import (
	"bigseqkit"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runSort(input []*api.IDataFrame[string], cmd *cobra.Command, args []string) *api.IDataFrame[string] {
	opts := parseSeqKitSortOptions(cmd)
	return check(bigseqkit.Sort(union(cmd, input...), opts))
}

func parseSeqKitSortOptions(cmd *cobra.Command) *bigseqkit.SeqKitSortOptions {
	return (&bigseqkit.SeqKitSortOptions{}).
		Config(parseSeqKitConfig(cmd)).
		InNaturalOrder(getFlagBool(cmd, "natural-order")).
		BySeq(getFlagBool(cmd, "by-seq")).
		ByName(getFlagBool(cmd, "by-name")).
		ByLength(getFlagBool(cmd, "by-length")).
		ByBases(getFlagBool(cmd, "by-bases")).
		GapLetters(getFlagString(cmd, "gap-letters")).
		Reverse(getFlagBool(cmd, "reverse")).
		IgnoreCase(getFlagBool(cmd, "ignore-case")).
		SeqPrefixLength(uint(getFlagNonNegativeInt(cmd, "seq-prefix-length")))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "sort",
			Short: "sort sequences by id/name/sequence/length",
			Long: `sort sequences by id/name/sequence/length.
By default, all records will be readed into memory.
For FASTA format, use flag -2 (--two-pass) to reduce memory usage. FASTQ not
supported.
Firstly, seqkit reads the sequence head and length information.
If the file is not plain FASTA file,
seqkit will write the sequences to temporary files, and create FASTA index.
Secondly, seqkit sorts sequence by head and length information
and extracts sequences by FASTA index.
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runSort)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().BoolP("natural-order", "N", false, "sort in natural order, when sorting by IDs/full name")
		cmd.Flags().BoolP("by-name", "n", false, "by full name instead of just id")
		cmd.Flags().BoolP("by-seq", "s", false, "by sequence")
		cmd.Flags().BoolP("by-length", "l", false, "by sequence length")
		cmd.Flags().BoolP("by-bases", "b", false, "by non-gap bases")
		cmd.Flags().StringP("gap-letters", "G", "- 	.", "gap letters")
		cmd.Flags().BoolP("reverse", "r", false, "reverse the result")
		cmd.Flags().BoolP("ignore-case", "i", false, "ignore case")

		cmd.Flags().BoolP("two-pass", "2", false, "two-pass mode read files twice to lower memory usage. (only for FASTA format)")
		cmd.Flags().BoolP("keep-temp", "k", false, "keep temporary FASTA and .fai file when using 2-pass mode")
		cmd.Flags().IntP("seq-prefix-length", "L", 10000, "length of sequence prefix on which seqkit sorts by sequences (0 for whole sequence)")
	})
}
