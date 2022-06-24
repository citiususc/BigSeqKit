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

func NewSeqTransform() any {
	return &SeqTransform{}
}

type SeqTransform struct {
	base.IMapPartitions[string, string]
	function.IAfterNone
	opts     bigseqkit.SeqOptions
	alphabet *seq.Alphabet
}

func (this *SeqTransform) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.SeqOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	if err != nil {
		return err
	}
	seq.AlphabetGuessSeqLengthThreshold = *this.opts.Config.AlphabetGuessSeqLength

	if *this.opts.GapLetters == "" {
		return fmt.Errorf("value of flag -G (--gap-letters) should not be empty")
	}
	for _, c := range *this.opts.GapLetters {
		if c > 127 {
			return fmt.Errorf("value of -G (--gap-letters) contains non-ASCII characters")
		}
	}

	if *this.opts.MinLen >= 0 && *this.opts.MaxLen >= 0 && *this.opts.MinLen > *this.opts.MaxLen {
		return fmt.Errorf("value of flag -m (--min-len) should be >= value of flag -M (--max-len)")
	}
	if *this.opts.MinQual >= 0 && *this.opts.MaxQual >= 0 && *this.opts.MinQual > *this.opts.MaxQual {
		return fmt.Errorf("value of flag -Q (--min-qual) should be <= value of flag -R (--max-qual)")
	}

	if (*this.opts.MinLen >= 0 || *this.opts.MaxLen >= 0) && !*this.opts.RemoveGaps {
		log.Warn("you may switch on flag -g/--remove-gaps to remove spaces")
	}

	seq.ValidateSeq = *this.opts.ValidateSeq
	seq.ValidateWholeSeq = false
	seq.ValidSeqLengthThreshold = *this.opts.ValidateSeqLength
	seq.ValidSeqThreads = 1
	seq.ComplementThreads = 1

	if *this.opts.Complement && (this.alphabet == nil || this.alphabet == seq.Protein) {
		log.Warn("flag -t (--seq-type) (DNA/RNA) is recommended for computing complement sequences")
	}

	if !*this.opts.ValidateSeq && !(this.alphabet == nil || this.alphabet == seq.Unlimit) {
		if !*this.opts.Config.Quiet {
			log.Info("when flag -t (--seq-type) given, flag -v (--validate-seq) is automatically switched on")
		}
		*this.opts.ValidateSeq = true
		seq.ValidateSeq = true
	}

	if *this.opts.LowerCase && *this.opts.UpperCase {
		return fmt.Errorf("could not give both flags -l (--lower-case) and -u (--upper-case)")
	}

	return nil
}

func (this *SeqTransform) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	reader := NewIteratorReader(v1)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}
	var record *fastx.Record

	filterMinLen := *this.opts.MinLen > 0
	filterMaxLen := *this.opts.MaxLen > 0
	filterMinQual := *this.opts.MinQual > 0
	filterMaxQual := *this.opts.MaxQual > 0

	var checkSeqType bool
	var isFastq bool
	var printName, printSeq, printQual bool
	var head []byte
	var sequence *seq.Seq
	var text []byte
	var buffer *bytes.Buffer

	result := make([]string, 0, 100)

	checkSeqType = true
	printQual = false
	once := true
	if *this.opts.Seq || *this.opts.Qual {
		*this.opts.Config.LineWidth = 0
	}
	var outbw bytes.Buffer
	for {
		outbw.Reset()
		record, err = fastxReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if checkSeqType {
			isFastq = fastxReader.IsFastq
			if isFastq {
				*this.opts.Config.LineWidth = 0
				printQual = true
			}
			checkSeqType = false
		}

		if *this.opts.RemoveGaps {
			record.Seq.RemoveGapsInplace(*this.opts.GapLetters)
		}

		if filterMinLen && len(record.Seq.Seq) < *this.opts.MinLen {
			continue
		}

		if filterMaxLen && len(record.Seq.Seq) > *this.opts.MaxLen {
			continue
		}

		if filterMinQual || filterMaxQual {
			avgQual := record.Seq.AvgQual(*this.opts.QualAsciiBase)
			if filterMinQual && avgQual < *this.opts.MinQual {
				continue
			}
			if filterMaxQual && avgQual >= *this.opts.MaxQual {
				continue
			}
		}

		printName, printSeq = true, true
		if *this.opts.Name && *this.opts.Seq {
			printName, printSeq = true, true
		} else if *this.opts.Name {
			printName, printSeq, printQual = true, false, false
		} else if *this.opts.Seq {
			printName, printSeq, printQual = false, true, false
		} else if *this.opts.Qual {
			if !isFastq {
				return nil, fmt.Errorf("FASTA format has no quality. So do not just use flag -q (--qual)")
			}
			printName, printSeq, printQual = false, false, true
		}
		if printName {
			if *this.opts.OnlyId {
				head = record.ID
			} else {
				head = record.Name
			}

			if printSeq {
				if isFastq {
					outbw.Write(_mark_fastq)
					outbw.Write(head)
					outbw.Write(_mark_newline)
				} else {
					outbw.Write(_mark_fasta)
					outbw.Write(head)
					outbw.Write(_mark_newline)
				}
			} else {
				outbw.Write(head)
				outbw.Write(_mark_newline)
			}
		}

		sequence = record.Seq
		if *this.opts.Reverse {
			sequence = sequence.ReverseInplace()
		}
		if *this.opts.Complement {
			if !*this.opts.Config.Quiet && record.Seq.Alphabet == seq.Protein || record.Seq.Alphabet == seq.Unlimit {
				log.Warn("complement does no take effect on protein/unlimit sequence")
			}
			sequence = sequence.ComplementInplace()
		}

		if printSeq {
			if *this.opts.Dna2rna {
				ab := fastxReader.Alphabet()
				if ab == seq.RNA || ab == seq.RNAredundant {
					if once {
						log.Warn("it's already RNA, no need to convert")
						once = false
					}
				} else {
					for i, b := range sequence.Seq {
						switch b {
						case 't':
							sequence.Seq[i] = 'u'
						case 'T':
							sequence.Seq[i] = 'U'
						}
					}
				}
			}
			if *this.opts.Rna2dna {
				ab := fastxReader.Alphabet()
				if ab == seq.DNA || ab == seq.DNAredundant {
					if once {
						log.Warn("it's already DNA, no need to convert")
						once = false
					}
				} else {
					for i, b := range sequence.Seq {
						switch b {
						case 'u':
							sequence.Seq[i] = 't'
						case 'U':
							sequence.Seq[i] = 'T'
						}
					}
				}
			}
			if *this.opts.LowerCase {
				sequence.Seq = bytes.ToLower(sequence.Seq)
			} else if *this.opts.UpperCase {
				sequence.Seq = bytes.ToUpper(sequence.Seq)
			}

			if isFastq {
				outbw.Write(sequence.Seq)
			} else {
				text, buffer = wrapByteSlice(sequence.Seq, *this.opts.Config.LineWidth, buffer)

				outbw.Write(text)
			}

			outbw.Write(_mark_newline)
		}

		if printQual {
			if !*this.opts.Qual {
				outbw.Write(_mark_plus_newline)
			}

			outbw.Write(sequence.Qual)
			outbw.Write(_mark_newline)
		}

		ss := outbw.String()
		if ss[len(ss)-1] == '\n' {
			ss = ss[:len(ss)-1]
		}
		result = append(result, ss)
	}

	return result, nil
}
