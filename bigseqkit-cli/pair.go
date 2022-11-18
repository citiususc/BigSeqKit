package main

import (
	"bigseqkit"
	"fmt"
	"github.com/shenwei356/util/pathutil"
	"github.com/spf13/cobra"
	"ignis/driver/api"
	"os"
	"path/filepath"
)

func runPair(input []*api.IDataFrame[string], cmd *cobra.Command, args []string, pipe bool) *api.IDataFrame[string] {
	if len(input) < 2 {
		checkError(fmt.Errorf("2 files needed"))
	}
	opts := parseSeqKitPairOptions(cmd)

	outdir := getFlagString(cmd, "out-dir")
	if outdir == "" {
		checkError(fmt.Errorf("out-dir required"))
	}

	if outdir != "./" && outdir != "." {
		existed, err := pathutil.DirExists(outdir)
		checkError(err)
		if existed {
			empty, err := pathutil.IsEmpty(outdir)
			checkError(err)
			if !empty {
				if getFlagBool(cmd, "force") {
					checkError(os.RemoveAll(outdir))
					checkError(os.MkdirAll(outdir, 0755))
				} else {
					checkError(fmt.Errorf("outdir not empty: %s, you can use --force to overwrite", outdir))
				}
			}
		} else {
			checkError(os.MkdirAll(outdir, 0755))
		}
	}

	pair, unpaired, cache, err := bigseqkit.Pair(input[0], input[1], opts)
	checkError(err)

	fOuput = func() {
		checkError(cache.Cache())
		defer cache.Cache()
		checkError(pair.Cache())
		defer pair.Uncache()

		pair1 := check(bigseqkit.PairIndex(pair, 0))
		pair2 := check(bigseqkit.PairIndex(pair, 1))

		check(0, pair1.SaveAsTextFile(getFlagString(cmd, filepath.Join(outdir, "paired.1"))))
		check(0, pair2.SaveAsTextFile(getFlagString(cmd, filepath.Join(outdir, "paired.2"))))

		if getFlagBool(cmd, "save-unpaired") {
			checkError(unpaired.Cache())
			defer unpaired.Uncache()
			unpaired1 := check(bigseqkit.UnpairedId(pair, "1"))
			unpaired2 := check(bigseqkit.UnpairedId(pair, "2"))
			check(0, unpaired1.SaveAsTextFile(getFlagString(cmd, filepath.Join(outdir, "unpaired.1"))))
			check(0, unpaired2.SaveAsTextFile(getFlagString(cmd, filepath.Join(outdir, "unpaired.2"))))
		}
	}

	return nil
}

func parseSeqKitPairOptions(cmd *cobra.Command) *bigseqkit.SeqKitPairOptions {
	if getFlagString(cmd, "read1") != "" {
		checkError(fmt.Errorf("Don't use  --read1"))
	}

	if getFlagString(cmd, "read2") != "" {
		checkError(fmt.Errorf("Don't use  --read2"))
	}

	if getFlagString(cmd, "out-dir") != "" {
		checkError(fmt.Errorf("Don't use  --out-dir"))
	}

	return (&bigseqkit.SeqKitPairOptions{}).
		Config(parseSeqKitConfig(cmd)).
		SaveUnpaired(getFlagBool(cmd, "save-unpaired"))
}

func init() {
	addCommand(func(parent *cobra.Command) {

		cmd := &cobra.Command{
			Use:   "pair",
			Short: "match up paired-end reads from two fastq files",
			Long: `match up paired-end reads from two fastq files
Attentions:
1. Orders of headers in the two files better be the same (not shuffled),
   otherwise, it consumes a huge number of memory for buffering reads in memory.
2. Unpaired reads are optional outputted with the flag -u/--save-unpaired.
3. If the flag -O/--out-dir is not given, the output will be saved in the same directory
   of input, with the suffix "paired", e.g., read_1.paired.fq.gz.
   Otherwise, names are kept untouched in the given output directory.
4. Paired gzipped files may be slightly larger than original files, because
   of using a different gzip package/library, don't worry.
`,
			Run: func(cmd *cobra.Command, args []string) {
				ignisDriver(cmd, args, runPair)
			},
		}
		parent.AddCommand(cmd)

		cmd.Flags().StringP("read1", "1", "", "(gzipped) read1 file. (DEPRECATED use normal input with 2 files)")
		cmd.Flags().StringP("read2", "2", "", "(gzipped) read2 file. (DEPRECATED use normal input with 2 files)")
		cmd.Flags().StringP("out-dir", "O", "", "output directory.")
		cmd.Flags().BoolP("force", "f", false, "overwrite output directory")
		cmd.Flags().BoolP("save-unpaired", "u", false, "save unpaired reads if there are")
	})
}
