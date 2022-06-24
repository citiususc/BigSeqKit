package main

import (
	"bigseqkit"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fastx"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/ipair"
	"ignis/executor/api/iterator"
	"io"
)

func NewPairPrepare() any {
	return &ConcatPrepare{}
}

type PairPrepare struct {
	base.IMapPartitions[string, ipair.IPair[string, string]]
	function.IAfterNone
	opts     bigseqkit.PairOptions
	alphabet *seq.Alphabet
	id       string
}

func (this *PairPrepare) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.PairOptions](context.Vars()["opts"].(string))
	this.id = context.Vars()["id"].(string)
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	//fai.MapWholeFile = false ¿?
	return err
}

func (this *PairPrepare) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]ipair.IPair[string, string], error) {
	result := make([]ipair.IPair[string, string], 0, 100)

	var record *fastx.Record
	var fastxReader *fastx.Reader

	reader := NewIteratorReader(v1)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}

	for {
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

		id := string(record.ID)
		result = append(result, *ipair.New(id, this.id+string(record.Format(*this.opts.Config.LineWidth))))
	}

	return result, nil
}

func NewPair() any {
	return &Pair{}
}

type Pair struct {
	base.IFlatmap[ipair.IPair[string, []string], ipair.IPair[string, string]]
	function.IOnlyCall
}

func (this *Pair) Call(v ipair.IPair[string, []string], context api.IContext) ([]ipair.IPair[string, string], error) {
	result := make([]ipair.IPair[string, string], 0, len(v.Second))
	file1 := make([]int, 0)
	file2 := make([]int, 0)
	for i := 0; i < len(v.Second); i++ {
		if v.Second[i][0] == '1' {
			file1 = append(file1, i)
		} else {
			file2 = append(file2, i)
		}
	}
	if len(file1) > len(file2) {
		file1 = file1[:len(file2)]
	} else {
		file2 = file2[:len(file1)]
	}

	for i := 0; i < len(file1); i++ {
		result = append(result, ipair.IPair[string, string]{v.Second[file1[i]], v.Second[file2[i]]})
	}

	return result, nil
}
