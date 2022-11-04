package main

import (
	"bigseqkit"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runSeq(input []*api.IDataFrame[string], cmd *cobra.Command, args []string) *api.IDataFrame[string] {
	opts := parseSeqKitSeqOptions(cmd)
	results := make([]*api.IDataFrame[string], len(input))
	for i := range input {
		results[i] = check(bigseqkit.Seq(input[i], opts))
	}
	return union(cmd, results...)
}

func parseSeqKitSeqOptions(cmd *cobra.Command) *bigseqkit.SeqKitSeqOptions {
	return (&bigseqkit.SeqKitSeqOptions{}).
		Config(parseSeqKitConfig(cmd)).
		Reverse(getFlagBool(cmd, "reverse")).
		Complement(getFlagBool(cmd, "complement")).
		Name(getFlagBool(cmd, "name")).
		Seq(getFlagBool(cmd, "seq")).
		Qual(getFlagBool(cmd, "qual")).
		OnlyId(getFlagBool(cmd, "only-id")).
		RemoveGaps(getFlagBool(cmd, "remove-gaps")).
		GapLetters(getFlagString(cmd, "gap-letters")).
		LowerCase(getFlagBool(cmd, "lower-case")).
		UpperCase(getFlagBool(cmd, "upper-case")).
		Dna2rna(getFlagBool(cmd, "dna2rna")).
		Rna2dna(getFlagBool(cmd, "rna2dna")).
		ValidateSeq(getFlagBool(cmd, "validate-seq")).
		ValidateSeqLength(getFlagValidateSeqLength(cmd, "validate-seq-length")).
		MinLen(getFlagInt(cmd, "min-len")).
		MaxLen(getFlagInt(cmd, "max-len")).
		QualAsciiBase(getFlagPositiveInt(cmd, "qual-ascii-base")).
		MinQual(getFlagFloat64(cmd, "min-qual")).MaxQual(getFlagFloat64(cmd, "max-qual"))
}

func init() {
	addCommand(func(parent *cobra.Command) {
		cmd := &cobra.Command{
			Use:   "seq",
			Short: "transform sequences (extract ID, filter by length, remove gaps, reverse complement...)",
			Long:  `transform sequences (extract ID, filter by length, remove gaps, reverse complement...)`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runSeq)
			},
		}

		parent.AddCommand(cmd)

		cmd.Flags().BoolP("reverse", "r", false, "reverse sequence")
		cmd.Flags().BoolP("complement", "p", false, "complement sequence, flag '-v' is recommended to switch on")
		cmd.Flags().BoolP("name", "n", false, "only print names")
		cmd.Flags().BoolP("seq", "s", false, "only print sequences")
		cmd.Flags().BoolP("qual", "q", false, "only print qualities")
		cmd.Flags().BoolP("only-id", "i", false, "print ID instead of full head")
		cmd.Flags().BoolP("remove-gaps", "g", false, "remove gaps")
		cmd.Flags().StringP("gap-letters", "G", "- 	.", "gap letters")
		cmd.Flags().BoolP("lower-case", "l", false, "print sequences in lower case")
		cmd.Flags().BoolP("upper-case", "u", false, "print sequences in upper case")
		cmd.Flags().BoolP("dna2rna", "", false, "DNA to RNA")
		cmd.Flags().BoolP("rna2dna", "", false, "RNA to DNA")
		cmd.Flags().BoolP("color", "k", false, "colorize sequences - to be piped into \"less -R\"")
		cmd.Flags().BoolP("validate-seq", "v", false, "validate bases according to the alphabet")
		cmd.Flags().IntP("validate-seq-length", "V", 10000, "length of sequence to validate (0 for whole seq)")
		cmd.Flags().IntP("min-len", "m", -1, "only print sequences longer than the minimum length (-1 for no limit)")
		cmd.Flags().IntP("max-len", "M", -1, "only print sequences shorter than the maximum length (-1 for no limit)")
		cmd.Flags().IntP("qual-ascii-base", "b", 33, "ASCII BASE, 33 for Phred+33")
		cmd.Flags().Float64P("min-qual", "Q", -1, "only print sequences with average quality qreater or equal than this limit (-1 for no limit)")
		cmd.Flags().Float64P("max-qual", "R", -1, "only print sequences with average quality less than this limit (-1 for no limit)")

	})
}
