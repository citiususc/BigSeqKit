package main

import (
	"fmt"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fai"
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/shenwei356/breader"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	log "ignis/executor/core/logger"
	"io"
	"regexp"
	"seqkit"
	"strings"
)

func NewReplace() any {
	return &Replace{}
}

type Replace struct {
	base.IMapPartitions[string, string]
	function.IAfterNone
	opts          seqkit.ReplaceOptions
	alphabet      *seq.Alphabet
	patternRegexp *regexp.Regexp
	replacement   []byte
	replaceWithNR bool
	replaceWithKV bool
	kvs           map[string]string
}

func (this *Replace) Before(context api.IContext) (err error) {
	this.opts = seqkit.StringToOptions[seqkit.ReplaceOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	fai.MapWholeFile = false
	this.replacement = []byte(*this.opts.Replacement)

	if *this.opts.Pattern == "" {
		return fmt.Errorf("flags -p (--pattern) needed")
	}
	p := *this.opts.Pattern
	if *this.opts.IgnoreCase {
		p = "(?i)" + p
	}
	this.patternRegexp, err = regexp.Compile(p)
	if err != nil {
		return err
	}

	if *this.opts.KvFile != "" {
		if len(*this.opts.Replacement) == 0 {
			return fmt.Errorf("flag -r (--replacement) needed when given flag -k (--kv-file)")
		}
		if !reKV.Match(this.replacement) {
			return fmt.Errorf(`replacement symbol "{kv}"/"{KV}" not found in value of flag -r (--replacement) when flag -k (--kv-file) given`)
		}
	}

	if reNR.Match(this.replacement) {
		this.replaceWithNR = true
	}

	if reKV.Match(this.replacement) {
		this.replaceWithKV = true
		if !regexp.MustCompile(`\(.+\)`).MatchString(*this.opts.Pattern) {
			return fmt.Errorf(`value of -p (--pattern) must contains "(" and ")" to capture data which is used specify the KEY`)
		}
		if *this.opts.BySeq {
			return fmt.Errorf(`replaceing with key-value pairs was not supported for sequence`)
		}
		if *this.opts.KvFile == "" {
			return fmt.Errorf(`since replacement symbol "{kv}"/"{KV}" found in value of flag -r (--replacement), tab-delimited key-value file should be given by flag -k (--kv-file)`)
		}
		if !*this.opts.Config.Quiet {
			log.Info(fmt.Sprintf("read key-value file: %s", *this.opts.KvFile))
		}
		this.kvs, err = readKVs(*this.opts.KvFile, *this.opts.IgnoreCase)
		if err != nil {
			return fmt.Errorf("read key-value file: %s", err)
		}
		if len(this.kvs) == 0 {
			return fmt.Errorf("no valid data in key-value file: %s", *this.opts.KvFile)
		}
		if !*this.opts.Config.Quiet {
			log.Info(fmt.Sprintf("%d pairs of key-value loaded", len(this.kvs)))
		}
	}

	return err
}

func (this *Replace) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	reader := NewIteratorReader(v1)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, 100)

	var r []byte
	var founds [][][]byte
	var found [][]byte
	var k, v string
	var ok bool
	var doNotChange bool
	var record *fastx.Record
	nrFormat := fmt.Sprintf("%%0%dd", *this.opts.NrWidth)

	nr := 0
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

		nr++
		if *this.opts.BySeq {
			if fastxReader.IsFastq {
				return nil, fmt.Errorf("editing FASTQ is not supported")
			}
			record.Seq.Seq = this.patternRegexp.ReplaceAll(record.Seq.Seq, this.replacement)
		} else {
			doNotChange = false

			r = this.replacement

			if this.replaceWithNR {
				r = reNR.ReplaceAll(r, []byte(fmt.Sprintf(nrFormat, nr)))
			}

			if this.replaceWithKV {
				founds = this.patternRegexp.FindAllSubmatch(record.Name, -1)
				if len(founds) > 1 {
					return nil, fmt.Errorf(`pattern "%s" matches multiple targets in "%s", this will cause chaos`, *this.opts.Pattern, record.Name)
				}

				if len(founds) > 0 {
					found = founds[0]
					if *this.opts.KeyCaptIdx > len(found)-1 {
						return nil, fmt.Errorf("value of flag -I (--key-capt-idx) overflows")
					}
					k = string(found[*this.opts.KeyCaptIdx])
					if *this.opts.IgnoreCase {
						k = strings.ToLower(k)
					}
					if v, ok = this.kvs[k]; ok {
						r = reKV.ReplaceAll(r, []byte(v))
					} else if *this.opts.KeepUntouch {
						doNotChange = true
					} else if *this.opts.KeepKey {
						r = reKV.ReplaceAll(r, found[*this.opts.KeyCaptIdx])
					} else {
						r = reKV.ReplaceAll(r, []byte(*this.opts.KeyMissRepl))
					}
				} else {
					doNotChange = true
				}
			}

			if !doNotChange {
				record.Name = this.patternRegexp.ReplaceAll(record.Name, r)
			}
		}

		result = append(result, string(record.Format(*this.opts.Config.LineWidth)))
	}

	return result, nil
}

var reNR = regexp.MustCompile(`\{(NR|nr)\}`)
var reKV = regexp.MustCompile(`\{(KV|kv)\}`)

func readKVs(file string, ignoreCase bool) (map[string]string, error) {
	type KV [2]string
	fn := func(line string) (interface{}, bool, error) {
		if len(line) == 0 {
			return nil, false, nil
		}
		items := strings.Split(strings.TrimRight(line, "\r\n"), "\t")
		if len(items) < 2 {
			return nil, false, nil
		}
		if ignoreCase {
			return KV([2]string{strings.ToLower(items[0]), items[1]}), true, nil
		}
		return KV([2]string{items[0], items[1]}), true, nil
	}
	kvs := make(map[string]string)
	reader, err := breader.NewBufferedReader(file, 2, 10, fn)
	if err != nil {
		return kvs, err
	}
	var items KV
	for chunk := range reader.Ch {
		if chunk.Err != nil {
			return kvs, err
		}
		for _, data := range chunk.Data {
			items = data.(KV)
			kvs[items[0]] = items[1]
		}
	}
	return kvs, nil
}
