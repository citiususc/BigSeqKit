package main

import (
	"bigseqkit"
	"fmt"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runGrep(input []*api.IDataFrame[string], cmd *cobra.Command, args []string, pipe bool) *api.IDataFrame[string] {
	opts := parseSeqKitGrepOptions(cmd)
	if getFlagBool(cmd, "count") {
		fOuput = func() {
			print(check(bigseqkit.GrepCount(union(cmd, input...), opts)))
		}
		return nil
	}
	return check(bigseqkit.Grep(union(cmd, input...), opts))
}

func parseSeqKitGrepOptions(cmd *cobra.Command) *bigseqkit.SeqKitGrepOptions {
	return (&bigseqkit.SeqKitGrepOptions{}).
		Config(parseSeqKitConfig(cmd)).
		Pattern(getFlagStringSlice(cmd, "pattern")).
		PatternFile(getFlagString(cmd, "pattern-file")).
		UseRegexp(getFlagBool(cmd, "use-regexp")).
		DeleteMatched(getFlagBool(cmd, "delete-matched")).
		InvertMatch(getFlagBool(cmd, "invert-match")).
		BySeq(getFlagBool(cmd, "by-seq")).
		OnlyPositiveStrand(getFlagBool(cmd, "only-positive-strand")).
		MaxMismatch(getFlagNonNegativeInt(cmd, "max-mismatch")).
		ByName(getFlagBool(cmd, "by-name")).
		IgnoreCase(getFlagBool(cmd, "ignore-case")).
		Degenerate(getFlagBool(cmd, "degenerate")).
		Region(getFlagString(cmd, "region")).
		Circular(getFlagBool(cmd, "circular"))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "grep",
			Short: "search sequences by ID/name/sequence/sequence motifs, mismatch allowed",
			Long: fmt.Sprintf(`search sequences by ID/name/sequence/sequence motifs, mismatch allowed
Attentions:
  0. By default, we match sequence ID with patterns, use "-n/--by-name"
     for matching full name instead of just ID.
  1. Unlike POSIX/GNU grep, we compare the pattern to the whole target
     (ID/full header) by default. Please switch "-r/--use-regexp" on
     for partly matching.
  2. When searching by sequences, it's partly matching, and both positive
     and negative strands are searched.
     Mismatch is allowed using flag "-m/--max-mismatch", you can increase
     the value of "-j/--threads" to accelerate processing.
  3. Degenerate bases/residues like "RYMM.." are also supported by flag -d.
     But do not use degenerate bases/residues in regular expression, you need
     convert them to regular expression, e.g., change "N" or "X"  to ".".
  4. When providing search patterns (motifs) via flag '-p',
     please use double quotation marks for patterns containing comma, 
     e.g., -p '"A{2,}"' or -p "\"A{2,}\"". Because the command line argument
     parser accepts comma-separated-values (CSV) for multiple values (motifs).
     Patterns in file do not follow this rule.
  5. The order of sequences in result is consistent with that in original
     file, not the order of the query patterns. 
     But for FASTA file, you can use:
        seqkit faidx seqs.fasta --infile-list IDs.txt
  6. For multiple patterns, you can either set "-p" multiple times, i.e.,
     -p pattern1 -p pattern2, or give a file of patterns via "-f/--pattern-file".
You can specify the sequence region for searching with the flag -R (--region).
The definition of region is 1-based and with some custom design.
Examples:
%s
`, regionExample),
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runGrep)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().StringSliceP("pattern", "p", []string{""}, `search pattern (multiple values supported. Attention: use double quotation marks for patterns containing comma, e.g., -p '"A{2,}"'))`)
		cmd.Flags().StringP("pattern-file", "f", "", "pattern file (one record per line)")
		cmd.Flags().BoolP("use-regexp", "r", false, "patterns are regular expression")
		cmd.Flags().BoolP("delete-matched", "", false, "delete a pattern right after being matched, this keeps the firstly matched data and speedups when using regular expressions")
		cmd.Flags().BoolP("invert-match", "v", false, "invert the sense of matching, to select non-matching records")
		cmd.Flags().BoolP("by-name", "n", false, "match by full name instead of just ID")
		cmd.Flags().BoolP("by-seq", "s", false, "search subseq on seq, both positive and negative strand are searched, and mismatch allowed using flag -m/--max-mismatch")
		cmd.Flags().BoolP("only-positive-strand", "P", false, "only search on positive strand")
		cmd.Flags().IntP("max-mismatch", "m", 0, "max mismatch when matching by seq. For large genomes like human genome, using mapping/alignment tools would be faster")
		cmd.Flags().BoolP("ignore-case", "i", false, "ignore case")
		cmd.Flags().BoolP("degenerate", "d", false, "pattern/motif contains degenerate base")
		cmd.Flags().StringP("region", "R", "", "specify sequence region for searching. "+
			"e.g 1:12 for first 12 bases, -12:-1 for last 12 bases")
		cmd.Flags().BoolP("circular", "c", false, "circular genome")
		cmd.Flags().BoolP("immediate-output", "I", false, "print output immediately, do not use write buffer")
		cmd.Flags().BoolP("count", "C", false, "just print a count of matching records. with the -v/--invert-match flag, count non-matching records")
	})
}
