package main

import (
	"bigseqkit"
	"fmt"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fai"
	"github.com/shenwei356/bio/seqio/fastx"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/ipair"
	"ignis/executor/api/iterator"
	"io"
)

func NewRenamePrepare() any {
	return &RenamePrepare{}
}

type RenamePrepare struct {
	base.IMapPartitions[string, ipair.IPair[string, string]]
	function.IAfterNone
	opts     bigseqkit.RenameOptions
	alphabet *seq.Alphabet
}

func (this *RenamePrepare) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.RenameOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	fai.MapWholeFile = false
	return err
}

func (this *RenamePrepare) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]ipair.IPair[string, string], error) {
	fastxReader, err := NewSeqParser(this.alphabet, v1, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}
	var record *fastx.Record
	var k string

	result := make([]ipair.IPair[string, string], 0, 100)

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

		if *this.opts.ByName {
			k = string(record.Name)
		} else {
			k = string(record.ID)
		}

		result = append(result, *ipair.New(k, string(record.Format(*this.opts.Config.LineWidth))))
	}
	return result, nil
}

func NewRename() any {
	return &Rename{}
}

type Rename struct {
	base.IFlatmap[ipair.IPair[string, []string], string]
	function.IAfterNone
	opts     bigseqkit.RenameOptions
	alphabet *seq.Alphabet
}

func (this *Rename) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.RenameOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	fai.MapWholeFile = false
	return err
}

func (this *Rename) Call(v1 ipair.IPair[string, []string], context api.IContext) ([]string, error) {
	if len(v1.Second) == 1 {
		return v1.Second, nil
	}

	reader := NewArrayIterator(v1.Second)
	fastxReader, err := NewSeqParser(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}
	var record *fastx.Record
	var newID string

	result := make([]string, 0, len(v1.Second))
	numbers := int64(0)

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

		if numbers > 0 {
			newID = fmt.Sprintf("%s_%d", record.ID, numbers)
			record.Name = []byte(fmt.Sprintf("%s %s", newID, record.Desc))
		}

		numbers++
		bb := record.Format(*this.opts.Config.LineWidth)
		result = append(result, string(bb[:len(bb)-1]))
	}

	return result, nil
}
