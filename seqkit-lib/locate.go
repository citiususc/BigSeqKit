package main

import (
	"bytes"
	"fmt"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/shenwei356/bwt/fmi"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	log "ignis/executor/core/logger"
	"io"
	"regexp"
	"seqkit"
)

func NewLocate() any {
	return &Locate{}
}

type Locate struct {
	base.IMapPartitionsWithIndex[string, string]
	function.IAfterNone
	opts               seqkit.LocateOptions
	alphabet           *seq.Alphabet
	onlyPositiveStrand bool
	regexps            map[string]*regexp.Regexp
	patterns           map[string][]byte
}

func (this *Locate) Before(context api.IContext) (err error) {
	this.opts = seqkit.StringToOptions[seqkit.LocateOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength
	seq.ValidateSeq = false
	seq.ValidateWholeSeq = false
	seq.ValidSeqLengthThreshold = *this.opts.ValidateSeqLength

	if this.alphabet == seq.Protein {
		this.onlyPositiveStrand = true
	}

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

	if *this.opts.MaxMismatch > 0 {
		if *this.opts.Degenerate {
			return fmt.Errorf("flag -d (--degenerate) not allowed when giving flag -m (--max-mismatch)")
		}
		if *this.opts.UseRegexp {
			return fmt.Errorf("flag -r (--use-regexp) not allowed when giving flag -m (--use-regexp)")
		}
		if *this.opts.NonGreedy && !*this.opts.Config.Quiet {
			log.Info("flag -G (--non-greedy) ignored when giving flag -m (--max-mismatch)")
		}

	}
	if *this.opts.UseFmi {
		if *this.opts.Degenerate {
			return fmt.Errorf("flag -d (--degenerate) ignored when giving flag -F (--use-fmi)")
		}
		if *this.opts.UseRegexp {
			return fmt.Errorf("flag -r (--use-regexp) ignored when giving flag -F (--use-fmi)")
		}
	}

	// prepare pattern
	this.regexps = make(map[string]*regexp.Regexp)
	this.patterns = make(map[string][]byte)
	var s string
	if *this.opts.PatternFile != "" {
		records, err := fastx.GetSeqsMap(*this.opts.PatternFile, seq.Unlimit, context.Threads(), 10, "")
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return fmt.Errorf("no FASTA sequences found in pattern file: %s", *this.opts.PatternFile)
		}
		for name, record := range records {
			this.patterns[name] = record.Seq.Seq
			if !*this.opts.Config.Quiet && bytes.Contains(record.Seq.Seq, []byte("\t ")) {
				log.Warn("space found in sequence: %s", name)
			}

			if *this.opts.Degenerate {
				s = record.Seq.Degenerate2Regexp()
			} else if *this.opts.UseRegexp {
				s = string(record.Seq.Seq)
			} else {
				if *this.opts.IgnoreCase {
					this.patterns[name] = bytes.ToLower(record.Seq.Seq)
				}
			}

			// check pattern
			if *this.opts.MaxMismatch > 0 {
				if *this.opts.MaxMismatch > len(record.Seq.Seq) {
					return fmt.Errorf("mismatch should be <= length of sequence: %s", record.Seq.Seq)
				}
				if seq.DNAredundant.IsValid(record.Seq.Seq) == nil ||
					seq.RNAredundant.IsValid(record.Seq.Seq) == nil ||
					seq.Protein.IsValid(record.Seq.Seq) == nil { // legal sequence
				} else {
					return fmt.Errorf("illegal DNA/RNA/Protein sequence: %s", record.Name)
				}
			} else {
				if *this.opts.Degenerate || *this.opts.UseRegexp {
					if *this.opts.IgnoreCase {
						s = "(?i)" + s
					}
					re, err := regexp.Compile(s)
					if err != nil {
						return err
					}
					this.regexps[name] = re
				} else if bytes.Index(record.Seq.Seq, []byte(".")) >= 0 ||
					!(seq.DNAredundant.IsValid(record.Seq.Seq) == nil ||
						seq.RNAredundant.IsValid(record.Seq.Seq) == nil ||
						seq.Protein.IsValid(record.Seq.Seq) == nil) {
					return fmt.Errorf("illegal DNA/RNA/Protein sequence: %s, you may switch on -d/--degenerate or -r/--use-regexp", record.Name)
				}
			}
		}
	} else {
		for _, p := range *this.opts.Pattern {
			this.patterns[p] = []byte(p)

			if !*this.opts.Config.Quiet && bytes.IndexAny(this.patterns[p], " \t") >= 0 {
				log.Warn("space found in sequence: '%s'", p)
			}

			if *this.opts.Degenerate {
				pattern2seq, err := seq.NewSeq(this.alphabet, []byte(p))
				if err != nil {
					return fmt.Errorf("it seems that flag -d is given, but you provide regular expression instead of available %s sequence", this.alphabet.String())
				}
				s = pattern2seq.Degenerate2Regexp()
			} else if *this.opts.UseRegexp {
				s = p
			} else {
				if *this.opts.IgnoreCase {
					this.patterns[p] = bytes.ToLower(this.patterns[p])
				}
			}

			// check pattern
			if *this.opts.MaxMismatch > 0 {
				if *this.opts.MaxMismatch > len(this.patterns[p]) {
					return fmt.Errorf("mismatch should be <= length of sequence: %s", p)
				}
				if seq.DNAredundant.IsValid(this.patterns[p]) == nil ||
					seq.RNAredundant.IsValid(this.patterns[p]) == nil ||
					seq.Protein.IsValid(this.patterns[p]) == nil { // legal sequence
				} else {
					return fmt.Errorf("illegal DNA/RNA/Protein sequence: %s", p)
				}
			} else {
				if *this.opts.Degenerate || *this.opts.UseRegexp {
					if *this.opts.IgnoreCase {
						s = "(?i)" + s
					}
					re, err := regexp.Compile(s)
					if err != nil {
						return err
					}
					this.regexps[p] = re
				} else if bytes.Index(this.patterns[p], []byte(".")) >= 0 ||
					!(seq.DNAredundant.IsValid(this.patterns[p]) == nil ||
						seq.RNAredundant.IsValid(this.patterns[p]) == nil ||
						seq.Protein.IsValid(this.patterns[p]) == nil) {
					return fmt.Errorf("illegal DNA/RNA/Protein sequence: %s, you may switch on -d/--degenerate or -r/--use-regexp", p)
				}
			}
		}
	}

	return err
}

func (this *Locate) Call(pid int64, it iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	result := make([]string, 0, 100)

	if !(*this.opts.Gtf || *this.opts.Bed) && pid == 0 {
		if *this.opts.HideMatched {
			result = append(result, "seqID\tpatternName\tpattern\tstrand\tstart\tend")
		} else {
			result = append(result, "seqID\tpatternName\tpattern\tstrand\tstart\tend\tmatched")
		}
	}

	_onlyPositiveStrand := *this.opts.OnlyPositiveStrand

	if *this.opts.MaxMismatch > 0 || *this.opts.UseFmi {

		reader := NewIteratorReader(it)
		fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
		if err != nil {
			return nil, err
		}

		checkAlphabet := true
	FOR_SEQ:
		for {
			record, err := fastxReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}

			if checkAlphabet {
				if fastxReader.Alphabet() == seq.Unlimit || fastxReader.Alphabet() == seq.Protein {
					_onlyPositiveStrand = true
				}
				checkAlphabet = false
			}

			var seqRP *seq.Seq
			var l int
			var sfmi *fmi.FMIndex
			sfmi = fmi.NewFMIndex()

			if !(*this.opts.Degenerate || *this.opts.UseRegexp) && *this.opts.IgnoreCase {
				record.Seq.Seq = bytes.ToLower(record.Seq.Seq)
			}

			l = len(record.Seq.Seq)

			if *this.opts.Circular { // concat two copies of sequence
				record.Seq.Seq = append(record.Seq.Seq, record.Seq.Seq...)
			}

			_, err = sfmi.Transform(record.Seq.Seq)
			if err != nil {
				return nil, fmt.Errorf("fail to build FMIndex for sequence: %s", record.Name)
			}

			for pName, pSeq := range this.patterns {
				loc, err := sfmi.Locate(pSeq, *this.opts.MaxMismatch)
				if err != nil {
					return nil, fmt.Errorf("fail to search pattern '%s' on seq '%s': %s", pName, record.Name, err)
				}
				var begin, end int
				for _, i := range loc {
					if *this.opts.Circular && i+1 > l { // 2nd clone of original part
						continue
					}

					begin = i + 1

					end = i + len(pSeq)
					if i+len(pSeq) > len(record.Seq.Seq) {
						continue
					}
					if *this.opts.Gtf {
						result = append(result,
							fmt.Sprintf("%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\tgene_id \"%s\"; \n",
								record.ID,
								"SeqKit",
								"location",
								begin,
								end,
								0,
								"+",
								".",
								pName))
					} else if *this.opts.Bed {
						result = append(result,
							fmt.Sprintf("%s\t%d\t%d\t%s\t%d\t%s\n",
								record.ID,
								begin-1,
								end,
								pName,
								0,
								"+"))
					} else {
						if *this.opts.HideMatched {
							result = append(result,
								fmt.Sprintf("%s\t%s\t%s\t%s\t%d\t%d\n",
									record.ID,
									pName,
									this.patterns[pName],
									"+",
									begin,
									end))
						} else {
							result = append(result, fmt.Sprintf("%s\t%s\t%s\t%s\t%d\t%d\t%s\n",
								record.ID,
								pName,
								this.patterns[pName],
								"+",
								begin,
								end,
								record.Seq.Seq[i:i+len(pSeq)]))
						}
					}
				}
			}

			if _onlyPositiveStrand {
				continue FOR_SEQ
			}

			seqRP = record.Seq.RevCom()

			_, err = sfmi.Transform(seqRP.Seq)
			if err != nil {
				return nil, fmt.Errorf("fail to build FMIndex for reverse complement sequence: %s", record.Name)
			}

			for pName, pSeq := range this.patterns {
				loc, err := sfmi.Locate(pSeq, *this.opts.MaxMismatch)
				if err != nil {
					return nil, fmt.Errorf("fail to search pattern '%s' on seq '%s': %s", pName, record.Name, err)
				}
				var begin, end int
				for _, i := range loc {
					if *this.opts.Circular && i+1 > l { // 2nd clone of original part
						continue
					}

					begin = l - i - len(pSeq) + 1
					end = l - i
					if i+len(pSeq) > len(record.Seq.Seq) {
						continue
					}
					if *this.opts.Gtf {
						result = append(result,
							fmt.Sprintf("%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\tgene_id \"%s\"; \n",
								record.ID,
								"SeqKit",
								"location",
								begin,
								end,
								0,
								"-",
								".",
								pName))
					} else if *this.opts.Bed {
						result = append(result,
							fmt.Sprintf("%s\t%d\t%d\t%s\t%d\t%s\n",
								record.ID,
								begin-1,
								end,
								pName,
								0,
								"-"))
					} else {
						if *this.opts.HideMatched {
							result = append(result,
								fmt.Sprintf("%s\t%s\t%s\t%s\t%d\t%d\n",
									record.ID,
									pName,
									this.patterns[pName],
									"-",
									begin,
									end))
						} else {
							result = append(result,
								fmt.Sprintf("%s\t%s\t%s\t%s\t%d\t%d\t%s\n",
									record.ID,
									pName,
									this.patterns[pName],
									"-",
									begin,
									end,
									seqRP.Seq[i:i+len(pSeq)]))
						}
					}
				}

			}
		}

		return result, nil
	}

	// -------------------------------------------------------------------

	var seqRP *seq.Seq
	var offset, l, lpatten int
	var loc []int
	var locs, locsNeg [][2]int
	var i, begin, end int
	var flag bool
	var pSeq, p []byte
	var pName string
	var re *regexp.Regexp
	var sfmi *fmi.FMIndex
	if *this.opts.MaxMismatch > 0 || *this.opts.UseFmi {
		sfmi = fmi.NewFMIndex()
	}

	reader := NewIteratorReader(it)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}

	checkAlphabet := true
	for {
		record, err := fastxReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if checkAlphabet {
			if fastxReader.Alphabet() == seq.Unlimit || fastxReader.Alphabet() == seq.Protein {
				_onlyPositiveStrand = true
			}
			checkAlphabet = false
		}

		if !(*this.opts.Degenerate || *this.opts.UseRegexp) && *this.opts.IgnoreCase {
			record.Seq.Seq = bytes.ToLower(record.Seq.Seq)
		}

		l = len(record.Seq.Seq)

		if *this.opts.Circular { // concat two copies of sequence
			record.Seq.Seq = append(record.Seq.Seq, record.Seq.Seq...)
		}

		if *this.opts.MaxMismatch > 0 || *this.opts.UseFmi {
			_, err = sfmi.Transform(record.Seq.Seq)
			if err != nil {
				return nil, fmt.Errorf("fail to build FMIndex for sequence: %s", record.Name)
			}

			for pName, pSeq = range this.patterns {
				loc, err = sfmi.Locate(pSeq, *this.opts.MaxMismatch)
				if err != nil {
					return nil, fmt.Errorf("fail to search pattern '%s' on seq '%s': %s", pName, record.Name, err)
				}
				for _, i = range loc {
					if *this.opts.Circular && i+1 > l { // 2nd clone of original part
						continue
					}

					begin = i + 1

					end = i + len(pSeq)
					if i+len(pSeq) > len(record.Seq.Seq) {
						continue
					}
					if *this.opts.Gtf {
						result = append(result, fmt.Sprintf("%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\tgene_id \"%s\"; \n",
							record.ID,
							"SeqKit",
							"location",
							begin,
							end,
							0,
							"+",
							".",
							pName))
					} else if *this.opts.Bed {
						result = append(result, fmt.Sprintf("%s\t%d\t%d\t%s\t%d\t%s\n",
							record.ID,
							begin-1,
							end,
							pName,
							0,
							"+"))
					} else {
						if *this.opts.HideMatched {
							result = append(result, fmt.Sprintf("%s\t%s\t%s\t%s\t%d\t%d\n",
								record.ID,
								pName,
								this.patterns[pName],
								"+",
								begin,
								end))
						} else {
							result = append(result, fmt.Sprintf("%s\t%s\t%s\t%s\t%d\t%d\t%s\n",
								record.ID,
								pName,
								this.patterns[pName],
								"+",
								begin,
								end,
								record.Seq.Seq[i:i+len(pSeq)]))
						}
					}
				}
			}

			if _onlyPositiveStrand {
				continue
			}

			seqRP = record.Seq.RevCom()

			_, err = sfmi.Transform(seqRP.Seq)
			if err != nil {
				return nil, fmt.Errorf("fail to build FMIndex for reverse complement sequence: %s", record.Name)
			}
			for pName, pSeq = range this.patterns {
				loc, err = sfmi.Locate(pSeq, *this.opts.MaxMismatch)
				if err != nil {
					return nil, fmt.Errorf("fail to search pattern '%s' on seq '%s': %s", pName, record.Name, err)
				}
				for _, i = range loc {
					if *this.opts.Circular && i+1 > l { // 2nd clone of original part
						continue
					}

					begin = l - i - len(pSeq) + 1
					end = l - i
					if i+len(pSeq) > len(record.Seq.Seq) {
						continue
					}
					if *this.opts.Gtf {
						result = append(result, fmt.Sprintf("%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\tgene_id \"%s\"; \n",
							record.ID,
							"SeqKit",
							"location",
							begin,
							end,
							0,
							"-",
							".",
							pName))
					} else if *this.opts.Bed {
						result = append(result, fmt.Sprintf("%s\t%d\t%d\t%s\t%d\t%s\n",
							record.ID,
							begin-1,
							end,
							pName,
							0,
							"-"))
					} else {
						if *this.opts.HideMatched {
							result = append(result, fmt.Sprintf("%s\t%s\t%s\t%s\t%d\t%d\n",
								record.ID,
								pName,
								this.patterns[pName],
								"-",
								begin,
								end))
						} else {
							result = append(result, fmt.Sprintf("%s\t%s\t%s\t%s\t%d\t%d\t%s\n",
								record.ID,
								pName,
								this.patterns[pName],
								"-",
								begin,
								end,
								seqRP.Seq[i:i+len(pSeq)]))
						}
					}
				}
			}

			continue
		}

		for pName = range this.patterns {
			locs = make([][2]int, 0, 1000)

			offset = 0
			if !(*this.opts.UseRegexp || *this.opts.Degenerate) {
				p = this.patterns[pName]
				lpatten = len(p)
			}
			for {
				if *this.opts.UseRegexp || *this.opts.Degenerate {
					re = this.regexps[pName]
					loc = re.FindSubmatchIndex(record.Seq.Seq[offset:])
					if loc == nil {
						break
					}

				} else {
					i = bytes.Index(record.Seq.Seq[offset:], p)
					if i < 0 {
						break
					}
					loc = []int{i, i + lpatten}
				}
				begin = offset + loc[0] + 1

				if *this.opts.Circular && begin > l { // 2nd clone of original part
					break
				}

				end = offset + loc[1]

				flag = true // check "duplicated" region
				if *this.opts.UseRegexp || *this.opts.Degenerate {
					for i = len(locs) - 1; i >= 0; i-- {
						if locs[i][0] <= begin && locs[i][1] >= end {
							flag = false
							break
						}
					}
				}

				if flag {
					if *this.opts.Gtf {
						result = append(result, fmt.Sprintf("%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\tgene_id \"%s\"; \n",
							record.ID,
							"SeqKit",
							"location",
							begin,
							end,
							0,
							"+",
							".",
							pName))
					} else if *this.opts.Bed {
						result = append(result, fmt.Sprintf("%s\t%d\t%d\t%s\t%d\t%s\n",
							record.ID,
							begin-1,
							end,
							pName,
							0,
							"+"))
					} else {
						if *this.opts.HideMatched {
							result = append(result, fmt.Sprintf("%s\t%s\t%s\t%s\t%d\t%d\n",
								record.ID,
								pName,
								this.patterns[pName],
								"+",
								begin,
								end))
						} else {
							result = append(result, fmt.Sprintf("%s\t%s\t%s\t%s\t%d\t%d\t%s\n",
								record.ID,
								pName,
								this.patterns[pName],
								"+",
								begin,
								end,
								record.Seq.Seq[begin-1:end]))
						}
					}
					locs = append(locs, [2]int{begin, end})
				}

				if *this.opts.NonGreedy {
					offset = offset + loc[1] + 1
				} else {
					offset = offset + loc[0] + 1
				}
				if offset >= len(record.Seq.Seq) {
					break
				}
			}

			if *this.opts.OnlyPositiveStrand {
				continue
			}

			seqRP = record.Seq.RevCom()

			locsNeg = make([][2]int, 0, 1000)

			offset = 0

			for {
				if *this.opts.UseRegexp || *this.opts.Degenerate {
					re = this.regexps[pName]
					loc = re.FindSubmatchIndex(seqRP.Seq[offset:])
					if loc == nil {
						break
					}
				} else {
					i = bytes.Index(seqRP.Seq[offset:], p)
					if i < 0 {
						break
					}
					loc = []int{i, i + lpatten}
				}

				if *this.opts.Circular && offset+loc[0]+1 > l { // 2nd clone of original part
					break
				}

				begin = l - offset - loc[1] + 1
				end = l - offset - loc[0]
				if offset+loc[1] > l {
					begin += l
					end += l
				}

				flag = true
				if *this.opts.UseRegexp || *this.opts.Degenerate {
					for i = len(locsNeg) - 1; i >= 0; i-- {
						if locsNeg[i][0] <= begin && locsNeg[i][1] >= end {
							flag = false
							break
						}
					}
				}

				if flag {
					if *this.opts.Gtf {
						result = append(result, fmt.Sprintf("%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\tgene_id \"%s\"; \n",
							record.ID,
							"SeqKit",
							"location",
							begin,
							end,
							0,
							"-",
							".",
							pName))
					} else if *this.opts.Bed {
						result = append(result, fmt.Sprintf("%s\t%d\t%d\t%s\t%d\t%s\n",
							record.ID,
							begin-1,
							end,
							pName,
							0,
							"-"))
					} else {
						if *this.opts.HideMatched {
							result = append(result, fmt.Sprintf("%s\t%s\t%s\t%s\t%d\t%d\n",
								record.ID,
								pName,
								this.patterns[pName],
								"-",
								begin,
								end))
						} else {
							result = append(result, fmt.Sprintf("%s\t%s\t%s\t%s\t%d\t%d\t%s\n",
								record.ID,
								pName,
								this.patterns[pName],
								"-",
								begin,
								end,
								seqRP.Seq[offset+loc[0]:offset+loc[1]]))
						}
					}
					locsNeg = append(locsNeg, [2]int{begin, end})
				}

				if *this.opts.NonGreedy {
					offset = offset + loc[1] + 1
				} else {
					offset = offset + loc[0] + 1
				}
				if offset >= len(record.Seq.Seq) {
					break
				}
			}
		}

	}

	return result, nil
}
