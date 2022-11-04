package main

import (
	"bigseqkit"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runReplace(input []*api.IDataFrame[string], cmd *cobra.Command, args []string) *api.IDataFrame[string] {
	opts := parseSeqKitReplaceOptions(cmd)
	results := make([]*api.IDataFrame[string], len(input))
	for i := range input {
		results[i] = check(bigseqkit.Replace(input[i], opts))
	}
	return union(cmd, results...)
}

func parseSeqKitReplaceOptions(cmd *cobra.Command) *bigseqkit.SeqKitReplaceOptions {
	return (&bigseqkit.SeqKitReplaceOptions{}).
		Config(parseSeqKitConfig(cmd)).
		Pattern(getFlagString(cmd, "pattern")).
		Replacement(getFlagString(cmd, "replacement")).
		NrWidth(getFlagPositiveInt(cmd, "nr-width")).
		KvFile(getFlagString(cmd, "kv-file")).
		KeepKey(getFlagBool(cmd, "keep-key")).
		KeepUntouch(getFlagBool(cmd, "keep-untouch")).
		KeyCaptIdx(getFlagPositiveInt(cmd, "key-capt-idx")).
		KeyMissRepl(getFlagString(cmd, "key-miss-repl")).
		BySeq(getFlagBool(cmd, "by-seq")).
		IgnoreCase(getFlagBool(cmd, "ignore-case"))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "replace",
			Short: "replace name/sequence by regular expression",
			Long: `replace name/sequence by regular expression.
Note that the replacement supports capture variables.
e.g. $1 represents the text of the first submatch.
ATTENTION: use SINGLE quote NOT double quotes in *nix OS.
Examples: Adding space to all bases.
    seqkit replace -p "(.)" -r '$1 ' -s
Or use the \ escape character.
    seqkit replace -p "(.)" -r "\$1 " -s
more on: http://bioinf.shenwei.me/seqkit/usage/#replace
Special replacement symbols (only for replacing name not sequence):
    {nr}    Record number, starting from 1
    {kv}    Corresponding value of the key (captured variable $n) by key-value file,
            n can be specified by flag -I (--key-capt-idx) (default: 1)
            
Special cases:
  1. If replacements contain '$', 
    a). If using '{kv}', you need use '$$$$' instead of a single '$':
            -r '{kv}' -k <(sed 's/\$/$$$$/' kv.txt)
    b). If not, use '$$':
            -r 'xxx$$xx'
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runReplace)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().StringP("pattern", "p", "", "search regular expression")
		cmd.Flags().StringP("replacement", "r", "",
			"replacement. supporting capture variables. "+
				" e.g. $1 represents the text of the first submatch. "+
				"ATTENTION: for *nix OS, use SINGLE quote NOT double quotes or "+
				`use the \ escape character. Record number is also supported by "{nr}".`+
				`use ${1} instead of $1 when {kv} given!`)
		cmd.Flags().IntP("nr-width", "", 1, `minimum width for {nr} in flag -r/--replacement. e.g., formatting "1" to "001" by --nr-width 3`)
		// cmd.Flags().BoolP("by-name", "n", false, "replace full name instead of just id")
		cmd.Flags().BoolP("by-seq", "s", false, "replace seq (only FASTA)")
		cmd.Flags().BoolP("ignore-case", "i", false, "ignore case")
		cmd.Flags().StringP("kv-file", "k", "",
			`tab-delimited key-value file for replacing key with value when using "{kv}" in -r (--replacement) (only for sequence name)`)
		cmd.Flags().BoolP("keep-untouch", "U", false, "do not change anything when no value found for the key (only for sequence name)")
		cmd.Flags().BoolP("keep-key", "K", false, "keep the key as value when no value found for the key (only for sequence name)")
		cmd.Flags().IntP("key-capt-idx", "I", 1, "capture variable index of key (1-based)")
		cmd.Flags().StringP("key-miss-repl", "m", "", "replacement for key with no corresponding value")
	})
}
