package main

import (
	"bigseqkit"
	"bytes"
	"fmt"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/shenwei356/util/stringutil"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	"io"
)

func NewHeadGenome() any {
	return &HeadGenome{}
}

type HeadGenome struct {
	base.IMapPartitionsWithIndex[string, string]
	function.IAfterNone
	opts     bigseqkit.HeadGenomeOptions
	alphabet *seq.Alphabet
	prefixes []string
}

func (this *HeadGenome) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.HeadGenomeOptions](context.Vars()["opts"].(string))
	this.prefixes = context.Vars()["prefixes"].([]string)
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false

	return nil
}

func (this *HeadGenome) Call(pid int64, v1 iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	reader := NewIteratorReader(v1)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}

	var record *fastx.Record
	var prefixes, words []string
	var i, N int
	var nSharedWords, pNSharedWords int64
	result := make([]string, 0, 100)
	minWords := *this.opts.MiniCommonWords

	var outbw bytes.Buffer
	for {
		outbw.Reset()
		record, err = fastxReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if fastxReader.IsFastq {
			*this.opts.Config.LineWidth = 0
			fastx.ForcelyOutputFastq = true
		}

		if len(record.Desc) == 0 {
			return nil, fmt.Errorf("no description: %s", record.ID)
		}

		if prefixes == nil { // first record
			if pid == 0 {
				result = append(result, string(record.Format(*this.opts.Config.LineWidth)))

				prefixes = stringutil.Split(string(record.Desc), "\t ")
				continue
			}
			prefixes = this.prefixes
		}

		words = stringutil.Split(string(record.Desc), "\t ")
		if len(words) < len(prefixes) {
			N = len(words)
		} else {
			N = len(prefixes)
		}

		nSharedWords = 0
		for i = 0; i < N; i++ {
			if words[i] != prefixes[i] {
				break
			}
			nSharedWords++
		}

		if nSharedWords < minWords {
			return result, nil
		}

		if pNSharedWords == 0 { // 2nd sequence
			pNSharedWords = nSharedWords
		} else if nSharedWords != pNSharedWords { // number of shared words changed
			return result, nil
		}

		result = append(result, string(record.Format(*this.opts.Config.LineWidth)))
	}

	return result, nil
}
