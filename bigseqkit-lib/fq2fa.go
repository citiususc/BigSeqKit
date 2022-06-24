package main

import (
	"bigseqkit"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fai"
	"github.com/shenwei356/bio/seqio/fastx"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	"io"
)

func NewFq2Fa() any {
	return &Fq2Fa{}
}

type Fq2Fa struct {
	base.IMapPartitions[string, string]
	function.IAfterNone
	opts     bigseqkit.Fq2FaOptions
	alphabet *seq.Alphabet
}

func (this *Fq2Fa) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.Fq2FaOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	fai.MapWholeFile = false
	return err
}

func (this *Fq2Fa) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	reader := NewIteratorReader(v1)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}
	var record *fastx.Record

	result := make([]string, 0, 100)

	for {
		record, err = fastxReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		record.Seq.Qual = []byte{}
		bb := record.Format(0)

		result = append(result, string(bb[:len(bb)-1]))
	}
	return result, nil
}
