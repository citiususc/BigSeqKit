package main

import (
	"fmt"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/shenwei356/util/byteutil"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	"io"
	"seqkit"
)

func NewStats() any {
	return &Stats{}
}

type Stats struct {
	base.IMapPartitions[string, map[int64]int64]
	function.IAfterNone
	opts     seqkit.StatsOptions
	alphabet *seq.Alphabet
}

func (this *Stats) Before(context api.IContext) (err error) {
	this.opts = seqkit.StringToOptions[seqkit.StatsOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	if err != nil {
		return err
	}
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false

	if len(*this.opts.GapLetters) == 0 {
		return fmt.Errorf("value of flag -G (--gap-letters) should not be empty")
	}
	for _, c := range *this.opts.GapLetters {
		if c > 127 {
			return fmt.Errorf("value of -G (--gap-letters) contains non-ASCII characters")
		}
	}

	return err
}

func (this *Stats) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]map[int64]int64, error) {
	reader := NewIteratorReader(v1)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}
	var record *fastx.Record

	gapLettersBytes := []byte(*this.opts.GapLetters)

	var q byte
	encode, err := parseQualityEncoding(*this.opts.FqEncoding)
	if err != nil {
		return nil, err
	}
	encodeOffset := encode.Offset()
	seqFormat := ""

	result := make(map[int64]int64)
	Q20 := int64(-1)
	Q30 := int64(-2)
	GAP_SUM := int64(-3)
	T := int64(-4)

	for {
		record, err = fastxReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if seqFormat == "" {
			if len(record.Seq.Qual) > 0 {
				seqFormat = "FASTQ"
			} else {
				seqFormat = "FASTA"
			}
		}

		result[int64(len(record.Seq.Seq))]++

		if *this.opts.All {
			if fastxReader.IsFastq {
				for _, q = range record.Seq.Qual {
					if int(q)-encodeOffset >= 20 {
						result[Q20]++
						if int(q)-encodeOffset >= 30 {
							result[Q30]++
						}
					}
				}
			}

			result[GAP_SUM] += int64(byteutil.CountBytes(record.Seq.Seq, gapLettersBytes))
		}
	}

	if fastxReader.Alphabet() == seq.DNAredundant {
		result[T] = int64('D') //DNA
	} else if fastxReader.Alphabet() == seq.RNAredundant {
		result[T] = int64('R') //RNA
	} else if seqFormat == "" && fastxReader.Alphabet() == seq.Unlimit {
		result[T] = int64('U') //Unlimit
	} else {
		result[T] = int64('F') //Check in file
	}

	return []map[int64]int64{result}, nil
}

func NewStatsReduce() any {
	return &StatsReduce{}
}

type StatsReduce struct {
	base.IReduce[map[int64]int64]
	function.IOnlyCall
}

func (this *StatsReduce) Call(v1 map[int64]int64, v2 map[int64]int64, context api.IContext) (map[int64]int64, error) {
	result := make(map[int64]int64)
	for k, v := range v1 {
		result[k] = v
	}
	for k, v := range v2 {
		result[k] = v
	}
	return result, nil
}
