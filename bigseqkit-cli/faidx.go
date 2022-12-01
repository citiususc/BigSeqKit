package main

import (
	"bigseqkit"
	"fmt"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runFaidx(input []*api.IDataFrame[string], cmd *cobra.Command, args []string, pipe bool) *api.IDataFrame[string] {
	if len(input) != 1 {
		checkError(fmt.Errorf("only 1 file needed"))
	}
	opts := parseSeqKitFaidxOptions(cmd)
	files := getFileListFromArgsAndFile(cmd, args, false, "infile-list", false)
	if !pipe {
		files = files[1:]
	}
	opts.Regions(files)
	checkError(input[0].Cache())

	idx, queries, err := bigseqkit.Faidx(input[0], opts)
	checkError(err)

	if queries == nil {
		return idx
	}

	idxFile := getFlagString(cmd, "index-file")
	if idxFile != "" {
		checkError(bigseqkit.StoreFASTX(idx, idxFile))
	}

	return queries
}

func parseSeqKitFaidxOptions(cmd *cobra.Command) *bigseqkit.SeqKitFaidxOptions {
	return (&bigseqkit.SeqKitFaidxOptions{}).
		Config(parseSeqKitConfig(cmd)).
		FullHead(getFlagBool(cmd, "full-head")).
		IgnoreCase(getFlagBool(cmd, "ignore-case")).
		UseRegexp(getFlagBool(cmd, "use-regexp")).
		RegionFile(getFlagString(cmd, "region-file"))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "faidx",
			Short: "create FASTA index file and extract subsequence",
			Long: fmt.Sprintf(`create FASTA index file and extract subsequence
This command is similar with "samtools faidx" but has some extra features:
  1. output full header line with the flag -f
  2. support regular expression as sequence ID with the flag -r
  3. if you have large number of IDs, you can use:
        seqkit faidx seqs.fasta -l IDs.txt
The definition of region is 1-based and with some custom design.
Examples:
%s
`, regionExample),
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runFaidx)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().BoolP("use-regexp", "r", false, "IDs are regular expression. But subseq region is not supported here.")
		cmd.Flags().BoolP("ignore-case", "i", false, "ignore case")
		cmd.Flags().BoolP("full-head", "f", false, "print full header line instead of just ID. New fasta index file ending with .seqkit.fai will be created")
		cmd.Flags().StringP("region-file", "l", "", "file containing a list of regions")
		cmd.Flags().StringP("index-file", "d", "", "FASTA index file only to extract subsequence without re-indexing")

		cmd.SetUsageTemplate(`Usage:{{if .Runnable}}
  {{if .HasAvailableFlags}}{{appendIfNotPresent .UseLine "[flags]"}}{{else}}{{.UseLine}}{{end}}{{end}}{{if .HasAvailableSubCommands}}
  {{ .CommandPath}} [command]{{end}} <fasta-file> [regions...]{{if gt .Aliases 0}}
Aliases:
  {{.NameAndAliases}}
{{end}}{{if .HasExample}}
Examples:
{{ .Example }}{{end}}{{ if .HasAvailableSubCommands}}
Available Commands:{{range .Commands}}{{if .IsAvailableCommand}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{ if .HasAvailableLocalFlags}}
Flags:
{{.LocalFlags.FlagUsages | trimRightSpace}}{{end}}{{ if .HasAvailableInheritedFlags}}
Global Flags:
{{.InheritedFlags.FlagUsages | trimRightSpace}}{{end}}{{if .HasHelpSubCommands}}
Additional help topics:{{range .Commands}}{{if .IsHelpCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{ if .HasAvailableSubCommands }}
Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`)
	})
}
