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
	"os"
	"path"
	"seqkit"
	"strconv"
	"strings"
	"sync"
)

func NewRmDupPrepare() any {
	return &RmDupPrepare{}
}

type RmDupPrepare struct {
	base.IMapPartitions[string, ipair.IPair[int64, string]]
	function.IAfterNone
	opts     seqkit.RmDupOptions
	alphabet *seq.Alphabet
}

func (this *RmDupPrepare) Before(context api.IContext) (err error) {
	this.opts = seqkit.StringToOptions[seqkit.RmDupOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	//fai.MapWholeFile = false ¿?
	return err
}

func (this *RmDupPrepare) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]ipair.IPair[int64, string], error) {
	result := make([]ipair.IPair[int64, string], 0, 100)

	var record *fastx.Record
	var fastxReader *fastx.Reader
	var subject uint64

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

		if *this.opts.BySeq {
			if *this.opts.IgnoreCase {
				subject = xxhash.Sum64(bytes.ToLower(record.Seq.Seq))
			} else {
				subject = xxhash.Sum64(record.Seq.Seq)
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
		result = append(result, *ipair.New(int64(subject), string(record.Format(*this.opts.Config.LineWidth))))
	}

	return result, nil
}

func NewRmDupCheck() any {
	return &RmDupCheck{}
}

type RmDupCheck struct {
	base.IFlatmap[ipair.IPair[int64, []string], string]
	opts     seqkit.RmDupOptions
	alphabet *seq.Alphabet
	mu       sync.Mutex
	data     []string
	dups     []string
	removed  int64
}

func (this *RmDupCheck) Before(context api.IContext) (err error) {
	this.opts = seqkit.StringToOptions[seqkit.RmDupOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	//fai.MapWholeFile = false ¿?
	this.data = make([]string, 0)
	this.dups = make([]string, 0)
	this.removed = 0
	return err
}

func (this *RmDupCheck) Call(v ipair.IPair[int64, []string], context api.IContext) ([]string, error) {
	if len(v.Second) == 1 {
		return v.Second, nil
	}

	var record *fastx.Record
	var fastxReader *fastx.Reader
	var subject string
	removed := int64(0)

	revcom := !*this.opts.OnlyPositiveStrand
	result := make([]string, 0, 100)
	counter := make(map[string]int)
	var names map[string][]string
	var dups []string

	if len(*this.opts.DupSeqsFile) > 0 {
		dups = make([]string, 0)
	}

	if len(*this.opts.DupNumFile) > 0 {
		names = make(map[string][]string)
	}

	reader := NewArrayReader(v.Second)
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

		if *this.opts.BySeq {
			if *this.opts.IgnoreCase {
				subject = string(bytes.ToLower(record.Seq.Seq))
			} else {
				subject = string(record.Seq.Seq)
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

		if _, ok := counter[subject]; ok { // duplicated
			counter[subject]++
			removed++
			if len(*this.opts.DupSeqsFile) > 0 {
				dups = append(dups, string(record.Format(*this.opts.Config.LineWidth)))
			}
			if len(*this.opts.DupNumFile) > 0 {
				names[subject] = append(names[subject], string(record.ID))
			}

			continue
		}

		if *this.opts.BySeq && revcom {
			if *this.opts.IgnoreCase {
				subject = string(bytes.ToLower(record.Seq.RevCom().Seq))
			} else {
				subject = string(record.Seq.RevCom().Seq)
			}

			if _, ok := counter[subject]; ok { // duplicated
				counter[subject]++
				removed++
				if len(*this.opts.DupSeqsFile) > 0 {
					dups = append(dups, string(record.Format(*this.opts.Config.LineWidth)))
				}
				if len(*this.opts.DupNumFile) > 0 {
					names[subject] = append(names[subject], string(record.ID))
				}
				continue
			}
		}
		counter[subject]++

		bb := record.Format(*this.opts.Config.LineWidth)
		result = append(result, string(bb[:len(bb)-1]))

		if len(*this.opts.DupNumFile) > 0 {
			names[subject] = []string{string(record.ID)}
		}

	}

	if len(*this.opts.DupSeqsFile) > 0 || len(*this.opts.DupNumFile) > 0 {
		this.mu.Lock()
		this.removed += removed

		if len(*this.opts.DupSeqsFile) > 0 {
			this.dups = append(this.dups, dups...)
		}

		if len(*this.opts.DupNumFile) > 0 {
			for _, l := range names {
				if len(l) > 1 {
					this.data = append(this.data, fmt.Sprintf("%d\t%s\n", len(l), strings.Join(l, ", ")))
				}
			}
		}
		this.mu.Unlock()
	}

	return result, nil
}

func (this *RmDupCheck) After(context api.IContext) (err error) {
	if this.removed > 0 {
		if len(*this.opts.DupSeqsFile) > 0 {
			if err := os.MkdirAll(*this.opts.DupNumFile, os.ModePerm); err != nil {
				return err
			}
			id := strconv.Itoa(context.ExecutorId())
			dupFile, err := os.Create(path.Join(*this.opts.DupNumFile, id))
			if err != nil {
				return err
			}
			for _, e := range this.dups {
				if _, err := dupFile.WriteString(e); err != nil {
					return err
				}
			}
		}

		if len(*this.opts.DupNumFile) > 0 {
			if err := os.MkdirAll(*this.opts.DupSeqsFile, os.ModePerm); err != nil {
				return err
			}
			id := strconv.Itoa(context.ExecutorId())
			sepFile, err := os.Create(path.Join(*this.opts.DupSeqsFile, id))
			if err != nil {
				return err
			}
			for _, e := range this.data {
				if _, err := sepFile.WriteString(e); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
