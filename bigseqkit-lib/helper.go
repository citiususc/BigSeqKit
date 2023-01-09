package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fastx"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	"ignis/executor/core/impi"
	"io"
	"os"
	"regexp"
	"strings"
)

var reRegion = regexp.MustCompile(`\-?\d+:\-?\d+`)
var _mark_fasta = []byte{'>'}
var _mark_fastq = []byte{'@'}
var _mark_plus_newline = []byte{'+', '\n'}
var _mark_newline = []byte{'\n'}

func NewReadFixer() any {
	return &ReadFixer{}
}

type ReadFixer struct {
	base.IMapPartitions[string, string]
	function.IAfterNone
	delim string
}

func (this *ReadFixer) Before(context api.IContext) (err error) {
	this.delim = context.Vars()["delim"].(string)
	return nil
}

func (this *ReadFixer) Call(it iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	result := make([]string, 0, 100)
	for it.HasNext() {
		v, err := it.Next()
		if err != nil {
			return nil, err
		}
		if len(v) == 0 {
			continue
		}
		if v[len(v)-1] == '\n' {
			if v[0] != this.delim[0] {
				result = append(result, this.delim+v[:len(v)-1])
			} else {
				result = append(result, v[:len(v)-1])
			}
		} else {
			if v[0] != this.delim[0] {
				result = append(result, this.delim+v)
			} else {
				result = append(result, v)
			}
		}
	}
	return result, nil
}

func mergeBytes(aa ...[]byte) []byte {
	n := 0
	for _, a := range aa {
		n += len(a)
	}
	merge := make([]byte, n)
	i := 0
	for _, a := range aa {
		i += copy(merge[i:], a)
	}
	return merge
}

func wrapByteSlice(s []byte, width int, buffer *bytes.Buffer) ([]byte, *bytes.Buffer) {
	if width < 1 {
		return s, buffer
	}
	l := len(s)
	if l == 0 {
		return s, buffer
	}

	var lines int
	if l%width == 0 {
		lines = l/width - 1
	} else {
		lines = int(l / width)
	}

	if buffer == nil {
		buffer = bytes.NewBuffer(make([]byte, 0, l+lines))
	} else {
		buffer.Reset()
	}

	var start, end int
	for i := 0; i <= lines; i++ {
		start = i * width
		end = (i + 1) * width
		if end > l {
			end = l
		}

		buffer.Write(s[start:end])
		if i < lines {
			buffer.Write(_mark_newline)
		}
	}
	return buffer.Bytes(), buffer
}

func parseQualityEncoding(s string) (seq.QualityEncoding, error) {
	switch strings.ToLower(s) {
	case "sanger":
		return seq.Sanger, nil
	case "solexa":
		return seq.Solexa, nil
	case "illumina-1.3+":
		return seq.Illumina1p3, nil
	case "illumina-1.5+":
		return seq.Illumina1p5, nil
	case "illumina-1.8+":
		return seq.Illumina1p8, nil
	case "":
		return seq.Unknown, nil
	default:
		return -1, fmt.Errorf("unsupported quality encoding: %s. available values: 'sanger', 'solexa', 'illumina-1.3+', 'illumina-1.5+', 'illumina-1.8+'", s)
	}
}

func NewArrayIterator(array []string) *ArrayIterator {
	return &ArrayIterator{array, 0}
}

type ArrayIterator struct {
	array []string
	i     int
}

func (this *ArrayIterator) HasNext() bool {
	return len(this.array) > this.i
}

func (this *ArrayIterator) Next() (string, error) {
	this.i += 1
	return this.array[this.i-1], nil
}

var reCheckIDregexpStr = regexp.MustCompile(`\(.+\)`)
var defaultIDRegexp = `^(\S+)\s?`
var IDRegexp = regexp.MustCompile(defaultIDRegexp)

// SeqParser parse both FASTA and FASTQ formats
type SeqParser struct {
	it iterator.IReadIterator[string]

	firstseq               bool // for guess alphabet by the first seq
	IsFastq                bool // if the file is fastq format
	isUsingDefaultIDRegexp bool

	t        *seq.Alphabet  // alphabet
	IDRegexp *regexp.Regexp // regexp for parsing record id

	head, seq, qual []byte
	seqBuffer       *bytes.Buffer
	qualBuffer      *bytes.Buffer
	record          *fastx.Record

	Err error // Current error
}

func NewSeqParser(t *seq.Alphabet, it iterator.IReadIterator[string], idRegexp string) (*SeqParser, error) {
	var r *regexp.Regexp
	var isUsingDefaultIDRegexp bool
	if idRegexp == "" {
		r = regexp.MustCompile(defaultIDRegexp)
		isUsingDefaultIDRegexp = true
	} else {
		if !reCheckIDregexpStr.MatchString(idRegexp) {
			return nil, fmt.Errorf(`fastx: regular expression must contain "(" and ")" to capture matched ID. default: %s`, defaultIDRegexp)
		}
		var err error
		r, err = regexp.Compile(idRegexp)
		if err != nil {
			return nil, fmt.Errorf("fastx: fail to compile regexp: %s", idRegexp)
		}
		if idRegexp == defaultIDRegexp {
			isUsingDefaultIDRegexp = true
		}
	}

	parser := &SeqParser{
		it:                     it,
		t:                      t,
		IDRegexp:               r,
		firstseq:               true,
		isUsingDefaultIDRegexp: isUsingDefaultIDRegexp,
	}
	parser.seqBuffer = bytes.NewBuffer(make([]byte, 0, 1<<20))
	parser.qualBuffer = bytes.NewBuffer(make([]byte, 0, 1<<20))

	parser.record = &fastx.Record{
		ID:   nil,
		Name: nil,
		Desc: nil,
		Seq:  &seq.Seq{},
	}

	return parser, nil
}

func (parser *SeqParser) Read() (*fastx.Record, error) {
	if !parser.it.HasNext() {
		return nil, io.EOF
	}
	p, err := parser.it.Next()
	if err != nil {
		return nil, err
	}

	if parser.firstseq {
		parser.IsFastq = p[0] == '@'
	}
	parser.seqBuffer.Reset()
	if parser.IsFastq {
		parser.qualBuffer.Reset()
	}

	if j := strings.IndexByte(p, '\n'); j > 0 {
		parser.head = []byte(p[0:j])
		r := j + 1

		if !parser.IsFastq { // FASTA
			for {
				if k := strings.IndexByte(p[r:], '\n'); k >= 0 {
					parser.seqBuffer.Write([]byte(p[r : r+k]))
					r += k + 1
					continue
				}
				parser.seqBuffer.Write([]byte(p[r:]))
				break
			}
			parser.seq = parser.seqBuffer.Bytes()
		} else { // FASTQ
			var isQual bool
			for {
				if k := strings.IndexByte(p[r:], '\n'); k >= 0 {
					if k > 0 && p[r] == '+' && !isQual {
						isQual = true
					} else if isQual {
						parser.qualBuffer.Write([]byte(p[r : r+k]))
					} else {
						parser.seqBuffer.Write([]byte(p[r : r+k]))
					}
					r += k + 1
					continue
				}
				if isQual {
					parser.qualBuffer.Write([]byte(p[r:]))
				}
				break
			}

			parser.seq = parser.seqBuffer.Bytes()
			parser.qual = parser.qualBuffer.Bytes()
		}

	} else {
		if len(p) > 0 && p[len(p)-1] == '\n' {
			parser.head = []byte(p[0 : len(p)-1])
		} else {
			parser.head = []byte(p)
		}
		parser.seq = []byte{}
		parser.qual = []byte{}
	}

	// guess alphabet
	if parser.firstseq {
		if parser.t == nil {
			parser.t = seq.GuessAlphabetLessConservatively(parser.seq)
		}
		parser.firstseq = false
	}

	if len(parser.head) == 0 && len(parser.seq) == 0 {
		return nil, io.EOF
	}
	// new record
	if parser.IsFastq {
		parser.record.Seq.Alphabet = parser.t
		parser.record.ID, parser.record.Desc = parser.parseHeadIDAndDesc(parser.IDRegexp, parser.head)
		parser.record.Name = parser.head
		parser.record.Seq.Seq = parser.seq
		parser.record.Seq.Qual = parser.qual

		if seq.ValidateSeq {
			parser.Err = parser.t.IsValid(parser.seq)
		}

		if len(parser.seq) != len(parser.qual) {
			parser.Err = fmt.Errorf("seq('%s'): unmatched length of sequence (%d) and quality (%d)",
				string(parser.record.Name), len(parser.seq), len(parser.qual))
		}

	} else {
		parser.record.Seq.Alphabet = parser.t
		parser.record.ID, parser.record.Desc = parser.parseHeadIDAndDesc(parser.IDRegexp, parser.head)
		parser.record.Name = parser.head
		parser.record.Seq.Seq = parser.seq

		if seq.ValidateSeq {
			parser.Err = parser.t.IsValid(parser.seq)
		}
	}

	return parser.record, parser.Err
}

var emptyByteSlice = []byte{}

func (parser *SeqParser) parseHeadIDAndDesc(idRegexp *regexp.Regexp, head []byte) ([]byte, []byte) {
	if parser.isUsingDefaultIDRegexp {
		if i := bytes.IndexByte(head, ' '); i > 0 {
			e := len(head)
			j := i + 1
			for ; j < e; j++ {
				if head[j] == ' ' || head[j] == '\t' {
					j++
				} else {
					break
				}
			}
			if j >= e {
				return head[0:i], emptyByteSlice
			}
			return head[0:i], head[j:]
		}
		if i := bytes.IndexByte(head, '\t'); i > 0 {
			e := len(head)
			j := i + 1
			for ; j < e; j++ {
				if head[j] == ' ' || head[j] == '\t' {
					j++
				} else {
					break
				}
			}
			if j >= e {
				return head[0:i], emptyByteSlice
			}
			return head[0:i], head[j:]
		}
		return head, emptyByteSlice
	}

	found := idRegexp.FindSubmatch(head)
	if found == nil { // not match
		return head, emptyByteSlice
	}
	return found[1], emptyByteSlice
}

func (parser *SeqParser) Alphabet() *seq.Alphabet {
	if parser.t == nil {
		return seq.Unlimit
	}
	return parser.t
}

func NewFileStore() any {
	return &FileStore{}
}

type FileStore struct {
	base.IMapPartitionsWithIndex[string, string]
	path  string
	execs int
	id    int
	f     *os.File
	buff  *bufio.Writer
	sync  []chan int
	part  int64
	err   error
}

func (this *FileStore) Before(context api.IContext) (err error) {
	this.err = nil
	this.path = context.Vars()["path"].(string)
	this.execs = context.Executors()
	this.id = context.ExecutorId()
	for i := 0; i < this.id; i++ {
		_ = impi.MPI_Barrier(context.MpiGroup())
	}
	this.f, err = os.OpenFile(this.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	this.buff = bufio.NewWriterSize(this.f, 64*1024*1024)
	this.sync = make([]chan int, 0)
	this.part = -1
	for i := 0; i < context.Threads(); i++ {
		this.sync = append(this.sync, make(chan int))
	}
	return
}

func (this *FileStore) After(context api.IContext) (err error) {
	if err = this.buff.Flush(); err != nil {
		return err
	}
	this.f.Close()
	for i := this.id; i < this.execs; i++ {
		_ = impi.MPI_Barrier(context.MpiGroup())
	}
	return this.err
}

func (this *FileStore) Call(pid int64, it iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	for true {
		<-this.sync[context.ThreadId()]
		if this.part == pid {
			break
		}
		if this.part == -1 && context.ThreadId() == 0 {
			this.part = pid
		}
	}
	for it.HasNext() {
		e, err := it.Next()
		if err != nil {
			this.err = err
			return []string{""}, nil
		}
		if _, err := this.buff.WriteString(e + "\n"); err != nil {
			this.err = err
			return []string{""}, nil
		}
	}

	this.part++
	for i := 0; i < context.Threads(); i++ {
		this.sync[i] <- 0
	}
	return []string{""}, nil
}
