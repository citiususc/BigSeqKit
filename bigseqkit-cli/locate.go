package main

import (
	"bigseqkit"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runLocate(input []*api.IDataFrame[string], cmd *cobra.Command, args []string, pipe bool) *api.IDataFrame[string] {
	opts := parseSeqKitLocateOptions(cmd)
	return check(bigseqkit.Locate(union(cmd, input...), opts))
}

func parseSeqKitLocateOptions(cmd *cobra.Command) *bigseqkit.SeqKitLocateOptions {
	return (&bigseqkit.SeqKitLocateOptions{}).
		Config(parseSeqKitConfig(cmd)).
		Pattern(getFlagStringSlice(cmd, "pattern")).
		PatternFile(getFlagString(cmd, "pattern-file")).
		Degenerate(getFlagBool(cmd, "degenerate")).
		UseRegexp(getFlagBool(cmd, "use-regexp")).
		UseFmi(getFlagBool(cmd, "use-fmi")).
		IgnoreCase(getFlagBool(cmd, "ignore-case")).
		OnlyPositiveStrand(getFlagBool(cmd, "only-positive-strand")).
		NonGreedy(getFlagBool(cmd, "non-greedy")).
		Gtf(getFlagBool(cmd, "gtf")).
		Bed(getFlagBool(cmd, "bed")).
		MaxMismatch(getFlagNonNegativeInt(cmd, "max-mismatch")).
		HideMatched(getFlagBool(cmd, "hide-matched")).
		Circular(getFlagBool(cmd, "circular"))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "locate",
			Short: "locate subsequences/motifs, mismatch allowed",
			Long: `locate subsequences/motifs, mismatch allowed
Attentions:
  1. Motifs could be EITHER plain sequence containing "ACTGN" OR regular
     expression like "A[TU]G(?:.{3})+?[TU](?:AG|AA|GA)" for ORFs.     
  2. Degenerate bases/residues like "RYMM.." are also supported by flag -d.
     But do not use degenerate bases/residues in regular expression, you need
     convert them to regular expression, e.g., change "N" or "X"  to ".".
  3. When providing search patterns (motifs) via flag '-p',
     please use double quotation marks for patterns containing comma, 
     e.g., -p '"A{2,}"' or -p "\"A{2,}\"". Because the command line argument
     parser accepts comma-separated-values (CSV) for multiple values (motifs).
     Patterns in file do not follow this rule.     
  4. Mismatch is allowed using flag "-m/--max-mismatch",
     you can increase the value of "-j/--threads" to accelerate processing.
  5. When using flag --circular, end position of matched subsequence that 
     crossing genome sequence end would be greater than sequence length.
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runLocate)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().StringSliceP("pattern", "p", []string{""}, `pattern/motif (multiple values supported. Attention: use double quotation marks for patterns containing comma, e.g., -p '"A{2,}"')`)
		cmd.Flags().StringP("pattern-file", "f", "", "pattern/motif file (FASTA format)")
		cmd.Flags().BoolP("degenerate", "d", false, "pattern/motif contains degenerate base")
		cmd.Flags().BoolP("use-regexp", "r", false, "patterns/motifs are regular expression")
		cmd.Flags().BoolP("use-fmi", "F", false, "use FM-index for much faster search of lots of sequence patterns")
		cmd.Flags().BoolP("ignore-case", "i", false, "ignore case")
		cmd.Flags().BoolP("only-positive-strand", "P", false, "only search on positive strand")
		cmd.Flags().IntP("validate-seq-length", "V", 10000, "length of sequence to validate (0 for whole seq)")
		cmd.Flags().BoolP("non-greedy", "G", false, "non-greedy mode, faster but may miss motifs overlapping with others")
		cmd.Flags().BoolP("gtf", "", false, "output in GTF format")
		cmd.Flags().BoolP("bed", "", false, "output in BED6 format")
		cmd.Flags().IntP("max-mismatch", "m", 0, "max mismatch when matching by seq. For large genomes like human genome, using mapping/alignment tools would be faster")
		cmd.Flags().BoolP("hide-matched", "M", false, "do not show matched sequences")
		cmd.Flags().BoolP("circular", "c", false, `circular genome. type "seqkit locate -h" for details`)
		//cmd.Flags().BoolP("immediate-output", "I", false, "print output immediately, do not use write buffer")
	})
}
