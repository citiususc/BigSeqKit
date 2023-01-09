package main

import (
	"bigseqkit"
	"bytes"
	"fmt"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fastx"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	log "ignis/executor/core/logger"
	"io"
)

func NewFa2Fq() any {
	return &Fa2Fq{}
}

type Fa2Fq struct {
	base.IMapPartitions[string, string]
	function.IAfterNone
	opts     bigseqkit.Fa2FqOptions
	alphabet *seq.Alphabet
	records  map[string]*fastx.Record
}

func (this *Fa2Fq) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.Fa2FqOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	if err != nil {
		return err
	}
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	//fai.MapWholeFile = false
	fileFasta := *this.opts.FastaFile
	if fileFasta == "" {
		return fmt.Errorf("flag -f (--fasta-file) needed")
	}

	this.records, err = fastx.GetSeqsMap(fileFasta, seq.Unlimit, context.Threads(), 10, "")
	if err != nil {
		return err
	}
	if len(this.records) == 0 {
		return fmt.Errorf("no sequences found in fasta file: %s", fileFasta)
	} else {
		log.Info(fmt.Sprintf("%d sequences loaded", len(this.records)))
	}

	return err
}

func (this *Fa2Fq) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	fastxReader, err := NewSeqParser(this.alphabet, v1, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}
	var record *fastx.Record

	result := make([]string, 0, 100)
	checkingFastq := true
	var buffer bytes.Buffer

	for {
		buffer.Reset()
		record, err = fastxReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if checkingFastq && !fastxReader.IsFastq {
			return nil, fmt.Errorf("this command only works for FASTQ format")
		}

		fa, ok := this.records[string(record.ID)]
		if !ok {
			continue
		}

		i := bytes.Index(record.Seq.Seq, fa.Seq.Seq)
		if i >= 0 {
			j := i + len(fa.Seq.Seq)
			buffer.Write(_mark_fastq)
			buffer.Write(record.ID)
			buffer.Write(_mark_newline)
			buffer.Write(record.Seq.Seq[i:j])
			buffer.Write(_mark_newline)
			buffer.Write(_mark_plus_newline)
			buffer.Write(record.Seq.Qual[i:j])
			continue
		}

		if *this.opts.OnlyPositiveStrand {
			continue
		}

		record.Seq.RevComInplace()

		i = bytes.Index(record.Seq.Seq, fa.Seq.Seq)
		if i >= 0 {
			j := i + len(fa.Seq.Seq)
			buffer.Write(_mark_fastq)
			buffer.Write(record.ID)
			buffer.Write(_mark_newline)
			buffer.Write(record.Seq.Seq[i:j])
			buffer.Write(_mark_newline)
			buffer.Write(_mark_plus_newline)
			buffer.Write(record.Seq.Qual[i:j])
		}

		result = append(result, buffer.String())
	}
	return result, nil
}
