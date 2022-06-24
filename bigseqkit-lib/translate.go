package main

import (
	"bigseqkit"
	"bytes"
	"fmt"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/shenwei356/util/byteutil"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	log "ignis/executor/core/logger"
	"io"
	"sort"
	"strconv"
	"strings"
)

func NewTranslate() any {
	return &Translate{}
}

type Translate struct {
	base.IMapPartitions[string, string]
	function.IAfterNone
	opts     bigseqkit.TranslateOptions
	alphabet *seq.Alphabet
	frames   []int
}

func (this *Translate) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.TranslateOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	if err != nil {
		return err
	}
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidSeqThreads = 1
	seq.ComplementThreads = 1

	if _, ok := seq.CodonTables[*this.opts.TranslTable]; !ok {
		return fmt.Errorf("invalid translate table: %d", *this.opts.TranslTable)
	}
	_frames := *this.opts.Frame
	this.frames = make([]int, 0, len(_frames))
	for _, _frame := range _frames {
		frame, err := strconv.Atoi(_frame)
		if err != nil {
			return fmt.Errorf("invalid frame(s): %s. available: 1, 2, 3, -1, -2, -3, and 6 for all. multiple frames should be separated by comma", _frame)
		}
		if !(frame == 1 || frame == 2 || frame == 3 || frame == -1 || frame == -2 || frame == -3 || frame == 6) {
			return fmt.Errorf("invalid frame: %d. available: 1, 2, 3, -1, -2, -3, and 6 for all", frame)
		}
		if frame == 6 {
			this.frames = []int{1, 2, 3, -1, -2, -3}
			break
		}
		this.frames = append(this.frames, frame)
	}

	return nil
}

func (this *Translate) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	reader := NewIteratorReader(v1)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}
	var record *fastx.Record

	result := make([]string, 0, 100)

	listTableAmb := *this.opts.ListTranslTableWithAmbCodons
	listTable := *this.opts.ListTranslTable

	if listTableAmb == 0 || listTable == 0 {
		ks := make([]int, len(seq.CodonTables))
		i := 0
		for k := range seq.CodonTables {
			ks[i] = k
			i++
		}
		sort.Ints(ks)
		for _, i = range ks {
			result = append(result, fmt.Sprintf("%d\t%s", seq.CodonTables[i].ID, seq.CodonTables[i].Name))
		}
		return result, nil
	} else if listTableAmb > 0 {
		if table, ok := seq.CodonTables[listTableAmb]; ok {
			return strings.Split(table.StringWithAmbiguousCodons(), "\n"), nil
		}
		return []string{}, nil
	} else if listTable > 0 {
		if table, ok := seq.CodonTables[listTable]; ok {
			return strings.Split(table.String(), "\n"), nil
		}
		return []string{}, nil
	}

	var outfh bytes.Buffer
	var _seq *seq.Seq
	var frame int
	once := true
	for {
		outfh.Reset()
		record, err = fastxReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if once {
			if !(record.Seq.Alphabet == seq.DNA || record.Seq.Alphabet == seq.DNAredundant ||
				record.Seq.Alphabet == seq.RNA || record.Seq.Alphabet == seq.RNAredundant) {
				return nil, fmt.Errorf(`command 'seqkit translate' only apply to DNA/RNA sequences`)
			}
			once = false
		}

		for _, frame = range this.frames {
			_seq, err = record.Seq.Translate(*this.opts.TranslTable, frame, *this.opts.Trim, *this.opts.Clean, *this.opts.AllowUnknownCodon, *this.opts.InitCodonAsM)
			if err != nil {
				if err == seq.ErrUnknownCodon {
					log.Error("unknown codon detected, you can use flag -x/--allow-unknown-codon to translate it to 'X'.")
				}
				return nil, err
			}

			if *this.opts.AppendFrame {
				outfh.WriteString(fmt.Sprintf(">%s_frame=%d %s\n", record.ID, frame, record.Desc))
			} else {
				outfh.WriteString(">" + string(record.Name) + "\n")
			}
			outfh.Write(byteutil.WrapByteSlice(_seq.Seq, *this.opts.Config.LineWidth))
			result = append(result, outfh.String())
		}

	}

	return result, nil
}
