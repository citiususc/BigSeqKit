package main

import (
	"bytes"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fastx"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/ipair"
	"ignis/executor/api/iterator"
	"io"
	"seqkit"
	"strconv"
)

func NewCommonPrepare() any {
	return &commonPrepare{}
}

type commonPrepare struct {
	base.IMapPartitions[string, ipair.IPair[int64, string]]
	function.IAfterNone
	opts     seqkit.CommonOptions
	alphabet *seq.Alphabet
	id       string
}

func (this *commonPrepare) Before(context api.IContext) (err error) {
	this.opts = seqkit.StringToOptions[seqkit.CommonOptions](context.Vars()["opts"].(string))
	this.id = context.Vars()["id"].(string)
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false

	if *this.opts.BySeq && *this.opts.ByName {
		return fmt.Errorf("only one/none of the flags -s (--by-seq) and -n (--by-name) is allowed")
	}

	revcom := !*this.opts.OnlyPositiveStrand

	if !revcom && !*this.opts.BySeq {
		return fmt.Errorf("flag -s (--by-seq) needed when using -P (--only-positive-strand)")
	}

	return err
}

func (this *commonPrepare) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]ipair.IPair[int64, string], error) {
	result := make([]ipair.IPair[int64, string], 0, 100)

	var record *fastx.Record
	var fastxReader *fastx.Reader

	reader := NewIteratorReader(v1)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}

	revcom := !*this.opts.OnlyPositiveStrand

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

		var subject uint64
		if *this.opts.BySeq {
			if revcom {
				if *this.opts.IgnoreCase {
					subject = xxhash.Sum64(bytes.ToLower(record.Seq.Seq))
				} else {
					subject = xxhash.Sum64(record.Seq.Seq)
				}
				if *this.opts.IgnoreCase {
					subject = xxhash.Sum64(bytes.ToLower(record.Seq.RevComInplace().Seq))
				} else {
					subject = xxhash.Sum64(record.Seq.RevComInplace().Seq)
				}
			}
		} else if *this.opts.ByName {
			if *this.opts.IgnoreCase {
				subject = xxhash.Sum64(bytes.ToLower(record.Name))
			} else {
				subject = xxhash.Sum64(record.Name)
			}
		} else { // byID
			if *this.opts.IgnoreCase {
				subject = xxhash.Sum64(bytes.ToLower(record.ID))
			} else {
				subject = xxhash.Sum64(record.ID)
			}
		}

		record.ID = append(record.ID, []byte("_"+this.id)...)

		result = append(result, *ipair.New(int64(subject), string(record.Format(*this.opts.Config.LineWidth))))
	}

	return result, nil
}

func NewCommonJoin() any {
	return &CommonJoin{}
}

type CommonJoin struct {
	base.IFlatmap[ipair.IPair[int64, []string], string]
	opts     seqkit.CommonOptions
	alphabet *seq.Alphabet
	ids      int
}

func (this *CommonJoin) Before(context api.IContext) (err error) {
	this.opts = seqkit.StringToOptions[seqkit.CommonOptions](context.Vars()["opts"].(string))
	this.ids = context.Vars()["ids"].(int)
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false

	return err
}

func (this *CommonJoin) Call(v ipair.IPair[int64, []string], context api.IContext) ([]string, error) {
	if len(v.Second) < this.ids {
		return []string{}, nil
	}

	result := make([]string, 0, len(v.Second))

	var fastxReader *fastx.Reader

	reader := NewArrayReader(v.Second)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}

	counter := make(map[string][]int)

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
		revcom := !*this.opts.OnlyPositiveStrand
		if err != nil {
			return nil, err
		}
		record.ID = record.ID[:sep]

		var subject string
		if *this.opts.BySeq {
			if revcom {
				if *this.opts.IgnoreCase {
					subject = string(bytes.ToLower(record.Seq.Seq))
				} else {
					subject = string(record.Seq.Seq)
				}
				if *this.opts.IgnoreCase {
					subject = string(bytes.ToLower(record.Seq.RevComInplace().Seq))
				} else {
					subject = string(record.Seq.RevComInplace().Seq)
				}
			}
		} else if *this.opts.ByName {
			if *this.opts.IgnoreCase {
				subject = string(bytes.ToLower(record.Name))
			} else {
				subject = string(record.Name)
			}
		} else { // byID
			if *this.opts.IgnoreCase {
				subject = string(bytes.ToLower(record.ID))
			} else {
				subject = string(record.ID)
			}
		}

		if n, ok := counter[subject]; ok {
			if n[0] == this.ids {
				result = append(result, string(record.Format(*this.opts.Config.LineWidth)))
				delete(counter, subject)
			}
			counter[subject][0]++
			counter[subject][i] = 1
		} else {
			counter[subject] = make([]int, this.ids+1)
			counter[subject][0] = 1
			counter[subject][i] = 1
		}
	}

	return result, nil
}
