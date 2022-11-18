package main

import (
	"bigseqkit"
	"github.com/spf13/cobra"
	"ignis/driver/api"
)

func runTranslate(input []*api.IDataFrame[string], cmd *cobra.Command, args []string, pipe bool) *api.IDataFrame[string] {
	opts := parseSeqKitTranslateOptions(cmd)
	results := make([]*api.IDataFrame[string], len(input))
	for i := range input {
		results[i] = check(bigseqkit.Translate(input[i], opts))
	}
	return union(cmd, results...)
}

func parseSeqKitTranslateOptions(cmd *cobra.Command) *bigseqkit.SeqKitTranslateOptions {
	return (&bigseqkit.SeqKitTranslateOptions{}).
		Config(parseSeqKitConfig(cmd)).
		TranslTable(getFlagPositiveInt(cmd, "transl-table")).
		Frame(getFlagStringSlice(cmd, "frame")).
		Trim(getFlagBool(cmd, "trim")).
		Clean(getFlagBool(cmd, "clean")).
		AllowUnknownCodon(getFlagBool(cmd, "allow-unknown-codon")).
		InitCodonAsM(getFlagBool(cmd, "init-codon-as-M")).
		ListTranslTable(getFlagInt(cmd, "list-transl-table")).
		ListTranslTableWithAmbCodons(getFlagInt(cmd, "list-transl-table-with-amb-codons")).
		AppendFrame(getFlagBool(cmd, "append-frame"))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "translate",
			Short: "translate DNA/RNA to protein sequence (supporting ambiguous bases)",
			Long: `translate DNA/RNA to protein sequence (supporting ambiguous bases)
Note:
  1. This command supports codons containing any ambiguous base.
     Please switch on flag -L INT for details. e.g., for standard table:
        ACN -> T
        CCN -> P
        CGN -> R
        CTN -> L
        GCN -> A
        GGN -> G
        GTN -> V
        TCN -> S
        
        MGR -> R
        YTR -> L
Translate Tables/Genetic Codes:
    # https://www.ncbi.nlm.nih.gov/Taxonomy/taxonomyhome.html/index.cgi?chapter=tgencodes
     1: The Standard Code
     2: The Vertebrate Mitochondrial Code
     3: The Yeast Mitochondrial Code
     4: The Mold, Protozoan, and Coelenterate Mitochondrial Code and the Mycoplasma/Spiroplasma Code
     5: The Invertebrate Mitochondrial Code
     6: The Ciliate, Dasycladacean and Hexamita Nuclear Code
     9: The Echinoderm and Flatworm Mitochondrial Code
    10: The Euplotid Nuclear Code
    11: The Bacterial, Archaeal and Plant Plastid Code
    12: The Alternative Yeast Nuclear Code
    13: The Ascidian Mitochondrial Code
    14: The Alternative Flatworm Mitochondrial Code
    16: Chlorophycean Mitochondrial Code
    21: Trematode Mitochondrial Code
    22: Scenedesmus obliquus Mitochondrial Code
    23: Thraustochytrium Mitochondrial Code
    24: Pterobranchia Mitochondrial Code
    25: Candidate Division SR1 and Gracilibacteria Code
    26: Pachysolen tannophilus Nuclear Code
    27: Karyorelict Nuclear
    28: Condylostoma Nuclear
    29: Mesodinium Nuclear
    30: Peritrich Nuclear
    31: Blastocrithidia Nuclear
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runTranslate)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().IntP("transl-table", "T", 1, `translate table/genetic code, type 'seqkit translate --help' for more details`)
		cmd.Flags().StringSliceP("frame", "f", []string{"1"}, "frame(s) to translate, available value: 1, 2, 3, -1, -2, -3, and 6 for all six frames")
		cmd.Flags().BoolP("trim", "", false, "remove all 'X' and '*' characters from the right end of the translation")
		cmd.Flags().BoolP("clean", "", false, "change all STOP codon positions from the '*' character to 'X' (an unknown residue)")
		cmd.Flags().BoolP("allow-unknown-codon", "x", false, "translate unknown code to 'X'. And you may not use flag --trim which removes 'X'")
		cmd.Flags().BoolP("init-codon-as-M", "M", false, "translate initial codon at beginning to 'M'")
		cmd.Flags().IntP("list-transl-table", "l", -1, "show details of translate table N, 0 for all")
		cmd.Flags().IntP("list-transl-table-with-amb-codons", "L", -1, "show details of translate table N (including ambigugous codons), 0 for all. ")
		cmd.Flags().BoolP("append-frame", "F", false, "append frame information to sequence ID")
	})
}
