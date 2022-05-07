package main

import (
	"bytes"
	"fmt"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/shenwei356/breader"
	"github.com/shenwei356/bwt"
	"github.com/shenwei356/bwt/fmi"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/ipair"
	"ignis/executor/api/iterator"
	log "ignis/executor/core/logger"
	"io"
	"regexp"
	"seqkit"
	"strconv"
	"strings"
)

func NewGrep() any {
	return &Grep{}
}

type Grep struct {
	base.IMapPartitionsWithIndex[string, string]
	function.IAfterNone
	opts        seqkit.GrepOptions
	alphabet    *seq.Alphabet
	limitRegion bool
	patterns    map[string]*regexp.Regexp
	start, end  int
}

var reUnquotedComma = regexp.MustCompile(`\{[^\}]*$|^[^\{]*\}`)
var helpUnquotedComma = `possible unquoted comma detected, please use double quotation marks for patterns containing comma, e.g., -p '"A{2,}"' or -p "\"A{2,}\""`

func (this *Grep) Before(context api.IContext) (err error) {
	this.opts = seqkit.StringToOptions[seqkit.GrepOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	//fai.MapWholeFile = false ¿?
	usingDefaultIDRegexp := *this.opts.Config.IDRegexp == fastx.DefaultIDRegexp
	bwt.CheckEndSymbol = false

	if len(*this.opts.Pattern) == 0 && *this.opts.PatternFile == "" {
		return fmt.Errorf("one of flags -p (--pattern) and -f (--pattern-file) needed")
	}

	// check pattern with unquoted comma
	hasUnquotedComma := false
	for _, _pattern := range *this.opts.Pattern {
		if reUnquotedComma.MatchString(_pattern) {
			hasUnquotedComma = true
			break
		}
	}
	if hasUnquotedComma {
		log.Warn(helpUnquotedComma)
	}

	if *this.opts.Degenerate && !*this.opts.BySeq {
		log.Info("when flag -d (--degenerate) given, flag -s (--by-seq) is automatically on")
		*this.opts.BySeq = true
	}

	//var sfmi *fmi.FMIndex
	if *this.opts.MaxMismatch > 0 {
		if *this.opts.UseRegexp || *this.opts.Degenerate {
			return fmt.Errorf("flag -r (--use-regexp) or -d (--degenerate) not allowed when giving flag -m (--max-mismatch)")
		}
		if !*this.opts.BySeq {
			log.Info("when value of flag -m (--max-mismatch) > 0, flag -s (--by-seq) is automatically on")
			*this.opts.BySeq = true
		}
		//sfmi = fmi.NewFMIndex()
		if *this.opts.MaxMismatch > 4 {
			log.Warn("large value flag -m/--max-mismatch will slow down the search")
		}
	}

	if *this.opts.UseRegexp && *this.opts.Degenerate {
		return fmt.Errorf("could not give both flags -d (--degenerate) and -r (--use-regexp)")
	}

	if *this.opts.Region != "" {
		this.limitRegion = true
		if !*this.opts.BySeq {
			log.Info("when flag -R (--region) given, flag -s (--by-seq) is automatically on")
			*this.opts.BySeq = true
		}
		region := *this.opts.Region
		if !reRegion.MatchString(region) {
			return fmt.Errorf(`invalid region: %s. type "seqkit grep -h" for more examples`, region)
		}
		r := strings.Split(region, ":")
		this.start, err = strconv.Atoi(r[0])
		if err != nil {
			return err
		}
		this.end, err = strconv.Atoi(r[1])
		if err != nil {
			return err
		}
		if this.start == 0 || this.end == 0 {
			return fmt.Errorf("both start and end should not be 0")
		}
		if this.start < 0 && this.end > 0 {
			return fmt.Errorf("when start < 0, end should not > 0")
		}
	}

	// prepare pattern
	this.patterns = make(map[string]*regexp.Regexp)
	var pattern2seq *seq.Seq
	var pbyte []byte
	if *this.opts.PatternFile != "" {
		var reader *breader.BufferedReader
		reader, err = breader.NewDefaultBufferedReader(*this.opts.PatternFile)
		if err != nil {
			return err
		}
		for chunk := range reader.Ch {
			if chunk.Err != nil {
				return chunk.Err
			}
			for _, data := range chunk.Data {
				p := data.(string)
				if p == "" {
					continue
				}

				if !*this.opts.Config.Quiet {
					if p[0] == '>' {
						log.Warn(`symbol ">" detected, it should not be a part of the sequence ID/name: %s`, p)
					} else if p[0] == '@' {
						log.Warn(`symbol "@" detected, it should not be a part of the sequence ID/name. %s`, p)
					} else if !*this.opts.ByName && usingDefaultIDRegexp && strings.ContainsAny(p, "\t ") {
						log.Warn("space found in pattern, you may need use -n/--by-name: %s", p)
					}
				}

				if *this.opts.Degenerate || *this.opts.UseRegexp {
					if *this.opts.Degenerate {
						pattern2seq, err = seq.NewSeq(this.alphabet, []byte(p))
						if err != nil {
							return fmt.Errorf("it seems that flag -d is given, but you provide regular expression instead of available %s sequence", this.alphabet.String())
						}
						p = pattern2seq.Degenerate2Regexp()
					}
					if *this.opts.IgnoreCase {
						p = "(?i)" + p
					}
					r, err := regexp.Compile(p)
					if err != nil {
						return err
					}
					this.patterns[p] = r
				} else if *this.opts.BySeq {
					pbyte = []byte(p)
					if *this.opts.MaxMismatch > 0 && *this.opts.MaxMismatch > len(p) {
						return fmt.Errorf("mismatch should be <= length of sequence: %s", p)
					}
					if seq.DNAredundant.IsValid(pbyte) == nil ||
						seq.RNAredundant.IsValid(pbyte) == nil ||
						seq.Protein.IsValid(pbyte) == nil { // legal sequence
						if *this.opts.IgnoreCase {
							this.patterns[strings.ToLower(p)] = nil
						} else {
							this.patterns[p] = nil
						}
					} else {
						return fmt.Errorf("illegal DNA/RNA/Protein sequence: %s", p)
					}
				} else {
					if *this.opts.IgnoreCase {
						this.patterns[strings.ToLower(p)] = nil
					} else {
						this.patterns[p] = nil
					}
				}
			}
		}
		if !*this.opts.Config.Quiet {
			if len(this.patterns) == 0 {
				log.Warn("%d patterns loaded from file", len(this.patterns))
			} else {
				log.Info("%d patterns loaded from file", len(this.patterns))
			}
		}
	} else {
		for _, p := range *this.opts.Pattern {
			if !*this.opts.Config.Quiet {
				if p[0] == '>' {
					log.Warn(`symbol ">" detected, it should not be a part of the sequence ID/name: %s`, p)
				} else if p[0] == '@' {
					log.Warn(`symbol "@" detected, it should not be a part of the sequence ID/name. %s`, p)
				} else if !*this.opts.ByName && usingDefaultIDRegexp && strings.ContainsAny(p, "\t ") {
					log.Warn("space found in pattern, you may need use -n/--by-name: %s", p)
				}
			}

			if *this.opts.Degenerate || *this.opts.UseRegexp {
				if *this.opts.Degenerate {
					pattern2seq, err = seq.NewSeq(this.alphabet, []byte(p))
					if err != nil {
						return fmt.Errorf("it seems that flag -d is given, but you provide regular expression instead of available %s sequence", this.alphabet.String())
					}
					p = pattern2seq.Degenerate2Regexp()
				}
				if *this.opts.IgnoreCase {
					p = "(?i)" + p
				}
				r, err := regexp.Compile(p)
				if err != nil {
					return err
				}
				this.patterns[p] = r
			} else if *this.opts.BySeq {
				pbyte = []byte(p)
				if *this.opts.MaxMismatch > 0 && *this.opts.MaxMismatch > len(p) {
					return fmt.Errorf("mismatch should be <= length of sequence: %s", p)
				}
				if seq.DNAredundant.IsValid(pbyte) == nil ||
					seq.RNAredundant.IsValid(pbyte) == nil ||
					seq.Protein.IsValid(pbyte) == nil { // legal sequence
					if *this.opts.IgnoreCase {
						this.patterns[strings.ToLower(p)] = nil
					} else {
						this.patterns[p] = nil
					}
				} else {
					return fmt.Errorf("illegal DNA/RNA/Protein sequence: %s", p)
				}
			} else {
				if *this.opts.IgnoreCase {
					this.patterns[strings.ToLower(p)] = nil
				} else {
					this.patterns[p] = nil
				}
			}
		}
	}

	return err
}

func (this *Grep) grepBySeqMismatches(pid int64, it iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	// only for searching with sequences and mismatch > 0, were FMI is very slow
	result := make([]string, 0, 100)
	var fastxReader *fastx.Reader
	var record *fastx.Record
	strands := []byte{'+', '-'}
	count := int64(0)

	justCount := *this.opts.Count

	var err error
	reader := NewIteratorReader(it)
	fastxReader, err = fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}

	checkAlphabet := true
	for {
		record, err = fastxReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if checkAlphabet {
			if fastxReader.Alphabet() == seq.Unlimit || fastxReader.Alphabet() == seq.Protein {
				*this.opts.OnlyPositiveStrand = true
			}
			checkAlphabet = false
		}

		if fastxReader.IsFastq {
			*this.opts.Config.LineWidth = 0
			fastx.ForcelyOutputFastq = true
		}

		var sequence *seq.Seq
		var target []byte
		var hit bool
		var k string

		sfmi := fmi.NewFMIndex()

		for _, strand := range strands {
			if hit {
				break
			}

			if strand == '-' && *this.opts.OnlyPositiveStrand {
				break
			}

			sequence = record.Seq
			if strand == '-' {
				sequence = record.Seq.RevCom()
			}
			if this.limitRegion {
				target = sequence.SubSeq(this.start, this.end).Seq
			} else if *this.opts.Circular {
				// concat two copies of sequence, and do not change orginal sequence
				target = make([]byte, len(sequence.Seq)*2)
				copy(target[0:len(sequence.Seq)], sequence.Seq)
				copy(target[len(sequence.Seq):], sequence.Seq)
			} else {
				target = sequence.Seq
			}

			if *this.opts.IgnoreCase {
				target = bytes.ToLower(target)
			}

			_, err = sfmi.Transform(target)
			if err != nil {
				return nil, fmt.Errorf("fail to build FMIndex for sequence: %s", record.Name)
			}
			for k = range this.patterns {
				hit, err = sfmi.Match([]byte(k), *this.opts.MaxMismatch)
				if err != nil {
					return nil, fmt.Errorf("fail to search pattern '%s' on seq '%s': %s", k, record.Name, err)
				}
				if hit {
					break
				}
			}

		}

		if *this.opts.InvertMatch {
			if hit {
				continue
			}
		} else {
			if !hit {
				continue
			}
		}

		if justCount {
			count++
		} else {
			result = append(result, string(record.Format(*this.opts.Config.LineWidth)))
		}

	}

	if justCount {
		result = append(result, fmt.Sprintf("%d", count))
	}
	return result, nil
}

func (this *Grep) grepGeneral(pid int64, it iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	result := make([]string, 0, 100)
	var record *fastx.Record
	var sequence *seq.Seq
	var target []byte
	var ok, hit bool
	var k string
	var re *regexp.Regexp
	var strand byte
	strands := []byte{'+', '-'}
	spid := strconv.FormatInt(pid, 10)
	count := int64(0)
	patterns := make(map[string]*regexp.Regexp, len(this.patterns))
	for a, b := range this.patterns {
		patterns[a] = b
	}

	justCount := *this.opts.Count

	reader := NewIteratorReader(it)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}

	checkAlphabet := true
	for {
		record, err = fastxReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			break
		}

		if checkAlphabet {
			if fastxReader.Alphabet() == seq.Unlimit || fastxReader.Alphabet() == seq.Protein {
				*this.opts.OnlyPositiveStrand = true
			}
			checkAlphabet = false
		}

		if fastxReader.IsFastq {
			*this.opts.Config.LineWidth = 0
			fastx.ForcelyOutputFastq = true
		}

		if *this.opts.ByName {
			target = record.Name
		} else if *this.opts.BySeq {

		} else {
			target = record.ID
		}

		hit = false
		sfmi := fmi.NewFMIndex()

		for _, strand = range strands {
			if hit {
				break
			}

			if strand == '-' {
				if *this.opts.BySeq {
					if *this.opts.OnlyPositiveStrand {
						break
					}
				} else {
					break
				}
			}

			if *this.opts.BySeq {
				sequence = record.Seq
				if strand == '-' {
					sequence = record.Seq.RevCom()
				}
				if this.limitRegion {
					target = sequence.SubSeq(this.start, this.end).Seq
				} else if *this.opts.Circular {
					// concat two copies of sequence, and do not change orginal sequence
					target = make([]byte, len(sequence.Seq)*2)
					copy(target[0:len(sequence.Seq)], sequence.Seq)
					copy(target[len(sequence.Seq):], sequence.Seq)
				} else {
					target = sequence.Seq
				}
			}

			if *this.opts.Degenerate || *this.opts.UseRegexp {
				for k, re = range patterns {
					if re.Match(target) {
						hit = true
						if *this.opts.DeleteMatched && !*this.opts.InvertMatch {
							delete(patterns, k)
						}
						break
					}
				}
			} else if *this.opts.BySeq {
				if *this.opts.IgnoreCase {
					target = bytes.ToLower(target)
				}
				if *this.opts.MaxMismatch == 0 {
					for k = range patterns {
						if bytes.Contains(target, []byte(k)) {
							hit = true
							if *this.opts.DeleteMatched && !*this.opts.InvertMatch {
								delete(patterns, k)
							}
							break
						}
					}
				} else {
					_, err = sfmi.Transform(target)
					if err != nil {
						return nil, fmt.Errorf("fail to build FMIndex for sequence: %s", record.Name)
					}
					for k = range patterns {
						hit, err = sfmi.Match([]byte(k), *this.opts.MaxMismatch)
						if err != nil {
							return nil, fmt.Errorf("fail to search pattern '%s' on seq '%s': %s", k, record.Name, err)
						}
						if hit {
							if *this.opts.DeleteMatched && !*this.opts.InvertMatch {
								delete(patterns, k)
							}
							break
						}
					}
				}
			} else {
				k = string(target)
				if *this.opts.IgnoreCase {
					k = strings.ToLower(k)
				}
				if _, ok = patterns[k]; ok {
					hit = true
					if *this.opts.DeleteMatched && !*this.opts.InvertMatch {
						delete(patterns, k)
					}
				}
			}

		}

		if *this.opts.InvertMatch {
			if hit {
				continue
			}
		} else {
			if !hit {
				continue
			}
		}

		if justCount {
			count++
		} else {
			bb := record.Format(*this.opts.Config.LineWidth)
			if *this.opts.DeleteMatched && !*this.opts.InvertMatch {
				result = append(result, k+"\000"+spid+"\000"+string(bb[:len(bb)-1]))
			} else {
				result = append(result, string(bb[:len(bb)-1]))
			}
		}
	}

	if justCount {
		result = append(result, fmt.Sprintf("%d", count))
	}
	return result, nil
}

func (this *Grep) Call(pid int64, it iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	if *this.opts.BySeq && *this.opts.MaxMismatch > 0 {
		return this.grepBySeqMismatches(pid, it, context)
	}
	return this.grepGeneral(pid, it, context)
}

func NewGrepPairMatched() any {
	return &GrepPairMatched{}
}

type GrepPairMatched struct {
	base.IMap[string, ipair.IPair[string, string]]
	function.IOnlyCall
}

func (this *GrepPairMatched) Call(v string, context api.IContext) (ipair.IPair[string, string], error) {
	sep := strings.IndexByte(v, '\000')
	return *ipair.New(v[0:sep], v[sep+1:]), nil
}

func NewGrepReducePairMatched() any {
	return &GrepReducePairMatched{}
}

type GrepReducePairMatched struct {
	base.IReduceByKey[string, string]
	function.IOnlyCall
}

func (this *GrepReducePairMatched) Call(v1 string, v2 string, context api.IContext) (string, error) {
	sep1 := strings.IndexByte(v1, '\000')
	sep2 := strings.IndexByte(v2, '\000')
	o1, _ := strconv.Atoi(v1[0:sep1])
	o2, _ := strconv.Atoi(v2[0:sep2])
	if o1 < o2 {
		return v1[sep1+1:], nil
	}
	return v2[sep2+1:], nil
}

func NewGrepValueMatched() any {
	return &GrepValueMatched{}
}

type GrepValueMatched struct {
	base.IMap[ipair.IPair[string, string], string]
	function.IOnlyCall
}

func (this *GrepValueMatched) Call(v ipair.IPair[string, string], context api.IContext) (string, error) {
	return v.Second, nil
}

func NewGrepReduceCount() any {
	return &GrepReduceCount{}
}

type GrepReduceCount struct {
	base.IReduce[string]
	function.IOnlyCall
}

func (this *GrepReduceCount) Call(v1 string, v2 string, context api.IContext) (string, error) {
	c1, _ := strconv.ParseInt(v1, 10, 64)
	c2, _ := strconv.ParseInt(v2, 10, 64)
	return strconv.FormatInt(c1+c2, 10), nil
}
