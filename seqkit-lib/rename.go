package main

import (
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
	"seqkit"
)

func NewRenamePrepare() any {
	return &RenamePrepare{}
}

type RenamePrepare struct {
	base.IMapPartitions[string, ipair.IPair[string, string]]
	function.IAfterNone
	opts     seqkit.RenameOptions
	alphabet *seq.Alphabet
}

func (this *RenamePrepare) Before(context api.IContext) (err error) {
	this.opts = seqkit.StringToOptions[seqkit.RenameOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	fai.MapWholeFile = false
	return err
}

func (this *RenamePrepare) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]ipair.IPair[string, string], error) {
	reader := NewIteratorReader(v1)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
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

		bb := record.Format(*this.opts.Config.LineWidth)
		result = append(result, *ipair.New(k, string(bb[:len(bb)-1])))
	}
	return result, nil
}

func NewRename() any {
	return &Rename{}
}

type Rename struct {
	base.IFlatmap[ipair.IPair[string, []string], string]
	function.IAfterNone
	opts     seqkit.RenameOptions
	alphabet *seq.Alphabet
}

func (this *Rename) Before(context api.IContext) (err error) {
	this.opts = seqkit.StringToOptions[seqkit.RenameOptions](context.Vars()["opts"].(string))
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

	reader := NewArrayReader(v1.Second)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
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
