package main

import (
	"bigseqkit"
	"bytes"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fai"
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/shenwei356/natsort"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/ipair"
	"ignis/executor/api/iterator"
	"io"
)

func NewSortParseInputString() any {
	return &SortParseInputString{}
}

type SortParseInputString struct {
	base.IMapPartitions[string, ipair.IPair[string, string]]
	function.IAfterNone
	opts     bigseqkit.SortOptions
	alphabet *seq.Alphabet
}

func (this *SortParseInputString) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.SortOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	fai.MapWholeFile = false
	return err
}

func (this *SortParseInputString) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]ipair.IPair[string, string], error) {
	reader := NewIteratorReader(v1)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}

	byName := *this.opts.ByName
	bySeq := *this.opts.BySeq
	seqPrefixLength := *this.opts.SeqPrefixLength
	ignoreCase := *this.opts.IgnoreCase

	result := make([]ipair.IPair[string, string], 0, 100)
	var first string
	var seqString string
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
		seqBB := record.Format(*this.opts.Config.LineWidth)
		if seqBB[len(seqBB)-1] == '\n' {
			seqString = string(seqBB[:len(seqBB)-1])
		} else {
			seqString = string(seqBB)
		}

		if byName {
			if ignoreCase {
				first = string(bytes.ToLower(record.Name))
			} else {
				first = string(record.Name)
			}
		} else if bySeq {
			if seqPrefixLength == 0 || uint(len(record.Seq.Seq)) <= seqPrefixLength {
				if ignoreCase {
					first = string(bytes.ToLower(record.Seq.Seq))
				} else {
					first = string(record.Seq.Seq)
				}
			} else {
				if ignoreCase {
					first = string(bytes.ToLower(record.Seq.Seq[0:seqPrefixLength]))
				} else {
					first = string(record.Seq.Seq[0:seqPrefixLength])
				}
			}
		} else {
			if ignoreCase {
				first = string(bytes.ToLower(record.ID))
			} else {
				first = string(record.ID)
			}
		}
		result = append(result, *ipair.New(first, seqString))
	}
	return result, nil
}

func NewSortParseInputInt() any {
	return &SortParseInputInt{}
}

type SortParseInputInt struct {
	base.IMapPartitions[string, ipair.IPair[int32, string]]
	function.IAfterNone
	opts     bigseqkit.SortOptions
	alphabet *seq.Alphabet
}

func (this *SortParseInputInt) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.SortOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	fai.MapWholeFile = false
	return err
}

func (this *SortParseInputInt) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]ipair.IPair[int32, string], error) {
	reader := NewIteratorReader(v1)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}

	byBases := *this.opts.ByBases
	gapLetters := *this.opts.GapLetters

	result := make([]ipair.IPair[int32, string], 0, 100)
	var length int32
	var second string
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
		seqBB := record.Format(*this.opts.Config.LineWidth)
		if seqBB[len(seqBB)-1] == '\n' {
			second = string(seqBB[:len(seqBB)-1])
		} else {
			second = string(seqBB)
		}

		if byBases {
			length = int32(record.Seq.Bases(gapLetters))
		} else {
			length = int32(len(record.Seq.Seq))
		}

		result = append(result, *ipair.New(length, second))
	}
	return result, nil
}

func NewSortString() any {
	return &SortString{}
}

type SortString struct {
	base.ISortByKey[string, string]
	function.IOnlyCall
}

func (this *SortString) Call(v1 string, v2 string, context api.IContext) (bool, error) {
	return v1 < v2, nil
}

func NewSortNatural() any {
	return &SortNatural{}
}

type SortNatural struct {
	base.ISortByKey[string, string]
	function.IOnlyCall
}

func (this *SortNatural) Call(v1 string, v2 string, context api.IContext) (bool, error) {
	return natsort.Compare(v1, v2, false), nil
}

func NewSortInt() any {
	return &SortInt{}
}

type SortInt struct {
	base.ISortByKey[int32, string]
	function.IOnlyCall
}

func (this *SortInt) Call(v1 int32, v2 int32, context api.IContext) (bool, error) {
	return v1 < v2, nil
}

func NewValuesStringString() any {
	return &ValuesStringString{}
}

type ValuesStringString struct {
	base.IMap[ipair.IPair[string, string], string]
	function.IOnlyCall
}

func (this *ValuesStringString) Call(v1 ipair.IPair[string, string], context api.IContext) (string, error) {
	return v1.Second, nil
}

func NewValuesIntString() any {
	return &ValuesIntString{}
}

type ValuesIntString struct {
	base.IMap[ipair.IPair[string, string], string]
	function.IOnlyCall
}

func (this *ValuesIntString) Call(v1 ipair.IPair[int, string], context api.IContext) (string, error) {
	return v1.Second, nil
}
