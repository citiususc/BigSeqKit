package main

import (
	"bigseqkit"
	"bytes"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fastx"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/ipair"
	"ignis/executor/api/iterator"
	"io"
	"strconv"
)

func NewConcatPrepare() any {
	return &ConcatPrepare{}
}

type ConcatPrepare struct {
	base.IMapPartitions[string, ipair.IPair[string, string]]
	function.IAfterNone
	opts     bigseqkit.ConcatOptions
	alphabet *seq.Alphabet
	id       string
}

func (this *ConcatPrepare) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.ConcatOptions](context.Vars()["opts"].(string))
	this.id = context.Vars()["id"].(string)
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	//fai.MapWholeFile = false ¿?
	return err
}

func (this *ConcatPrepare) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]ipair.IPair[string, string], error) {
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
		record.ID = append(record.ID, []byte("_"+this.id)...)

		result = append(result, *ipair.New(id, string(record.Format(*this.opts.Config.LineWidth))))
	}

	return result, nil
}

func NewConcatJoin() any {
	return &ConcatJoin{}
}

type ConcatJoin struct {
	base.IFlatmap[ipair.IPair[string, []string], string]
	function.IAfterNone
	opts     bigseqkit.ConcatOptions
	alphabet *seq.Alphabet
}

func (this *ConcatJoin) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.ConcatOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	//fai.MapWholeFile = false ¿?

	return err
}

func (this *ConcatJoin) Call(v ipair.IPair[string, []string], context api.IContext) ([]string, error) {
	if len(v.Second) == 1 && *this.opts.Full {
		return v.Second, nil
	}

	result := make([]string, 0, len(v.Second))

	var fastxReader *fastx.Reader

	reader := NewArrayReader(v.Second)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}

	seqs := [][]*fastx.Record{make([]*fastx.Record, 0, len(v.Second)), make([]*fastx.Record, 0, len(v.Second))}
	separator := []byte(*this.opts.Separator)

	for {
		record, err := fastxReader.Read()
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

		sep := bytes.LastIndexByte(record.ID, '_')
		i, err := strconv.Atoi(string(record.ID[sep+1:]))
		if err != nil {
			return nil, err
		}
		record.ID = record.ID[:sep]

		seqs[i] = append(seqs[i], record)
	}

	if (len(seqs[0]) == 0 || len(seqs[1]) == 0) && *this.opts.Full {
		var ifull int
		if len(seqs[0]) == 0 {
			ifull = 1
		} else {
			ifull = 0
		}

		for _, record := range seqs[ifull] {
			seqBB := record.Format(*this.opts.Config.LineWidth)
			result = append(result, string(seqBB[:len(seqBB)-1]))
		}
		return result, nil
	}

	for _, recordA := range seqs[0] {
		for _, recordB := range seqs[1] {
			record := &fastx.Record{
				ID:   recordA.ID,
				Name: recordA.ID,
				Desc: mergeBytes(recordA.Desc, separator, recordB.Desc),
				Seq: &seq.Seq{
					Alphabet: recordA.Seq.Alphabet,
					Seq:      mergeBytes(recordA.Seq.Seq, recordB.Seq.Seq),
					Qual:     mergeBytes(recordA.Seq.Qual, recordB.Seq.Qual),
				},
			}

			seqBB := record.Format(*this.opts.Config.LineWidth)
			result = append(result, string(seqBB[:len(seqBB)-1]))
		}
	}

	return result, nil
}
