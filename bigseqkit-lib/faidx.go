package main

import (
	"bigseqkit"
	"bytes"
	"fmt"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fai"
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/shenwei356/breader"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type Index map[string]fai.Record

type rangeQuery struct {
	ID     string
	Region [2]int
}

var reCheckIDregexpStr = regexp.MustCompile(`\(.+\)`)
var defaultIDRegexp = `^(\S+)\s?`
var IDRegexp = regexp.MustCompile(defaultIDRegexp)

func NewFaidxOffset() any {
	return &FaidxOffset{}
}

type FaidxOffset struct {
	base.IMapPartitions[string, int64]
	function.IOnlyCall
}

func (this *FaidxOffset) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]int64, error) {
	offset := int64(0)
	for v1.HasNext() {
		elem, err := v1.Next()
		if err != nil {
			return nil, err
		}
		offset += int64(len(elem)) + 1 //\n
	}
	return []int64{offset}, nil
}

func NewFaidx() any {
	return &Faidx{}
}

type Faidx struct {
	base.IMapPartitionsWithIndex[string, string]
	function.IAfterNone
	offsets                []int64
	opts                   bigseqkit.FaidxOptions
	IDRegexp               *regexp.Regexp
	isUsingDefaultIDRegexp bool
}

func (this *Faidx) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.FaidxOptions](context.Vars()["opts"].(string))
	this.offsets = context.Vars()["offsets"].([]int64)
	fai.MapWholeFile = true
	var idRegexp string

	if *this.opts.FullHead {
		idRegexp = "^(.+)$"
	} else {
		idRegexp = *this.opts.Config.IDRegexp
	}

	if idRegexp != defaultIDRegexp {
		if !reCheckIDregexpStr.MatchString(idRegexp) {
			return fmt.Errorf(`regular expression must contain "(" and ")" to capture matched ID. default: %s`, `^([^\s]+)\s?`)
		}
		this.IDRegexp, err = regexp.Compile(idRegexp)
		if err != nil {
			return fmt.Errorf("fail to Compile idRegexp: %s", err)
		}
		this.isUsingDefaultIDRegexp = false
	} else {
		this.IDRegexp = IDRegexp
	}

	return nil
}

func (this *Faidx) Call(pid int64, v1 iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	result := make([]string, 0, 100)

	seqLen := 0
	var hasSeq bool
	var lastName, thisName []byte
	var id string
	iqual := int64(-1)
	qline := false
	var lastStart int64
	thisStart := this.offsets[pid]
	var lineWidths, seqWidths []int
	var lastLineWidth, lineWidth, seqWidth int
	var chances int
	var line, lineDropCR []byte
	var seenSeqs bool
	for v1.HasNext() {
		seqBlock, err := v1.Next()
		if err != nil {
			return nil, err
		}
		for _, line = range bytes.Split([]byte(seqBlock), []byte("\n")) {
			if line[0] == '+' && !qline{
				thisStart += 2
				iqual = thisStart + 1
				qline = true
			} else if (line[0] == '>' || line[0] == '@') && !qline{
				hasSeq = true
				thisName = dropCR(line[1:])

				if lastName != nil { // not the first record
					id = string(parseHeadID(lastName, this.isUsingDefaultIDRegexp))

					// check lineWidths
					lastLineWidth, chances = -1, 2
					seenSeqs = false
					for i := len(lineWidths) - 1; i >= 0; i-- {
						if !seenSeqs && seqWidths[i] == 0 { // skip empty lines in the end
							continue
						}
						seenSeqs = true

						if lastLineWidth == -1 {
							lastLineWidth = lineWidths[i]
							continue
						}
						if lineWidths[i] != lastLineWidth {
							chances--
							if chances == 0 || lineWidths[i] < lastLineWidth {
								return nil, fmt.Errorf("different line length in sequence: %s. Please format the file with 'seqkit seq'", id)
							}
						}
						lastLineWidth = lineWidths[i]
					}
					// lineWidth = 0
					if len(lineWidths) > 0 {
						lineWidth = lineWidths[0]
					}
					// seqWidth = 0
					if len(seqWidths) > 0 {
						seqWidth = seqWidths[0]
					}

					if iqual > 0 {
						result = append(result, fmt.Sprintf("%s\t%d\t%d\t%d\t%d\t%d", id, seqLen, lastStart, seqWidth, lineWidth, iqual))
					} else {
						result = append(result, fmt.Sprintf("%s\t%d\t%d\t%d\t%d", id, seqLen, lastStart, seqWidth, lineWidth))
					}

					iqual = -1
					seqLen = 0
				}
				lineWidths = []int{}
				seqWidths = []int{}
				thisStart += int64(len(line))
				lastStart = thisStart + 1
				lastName = thisName
			} else if hasSeq {
				thisStart += int64(len(line) - 1)
				if iqual < 0 {
					lineDropCR = dropCR(line)
					seqLen += len(lineDropCR)
					lineWidths = append(lineWidths, len(line)+1)
					seqWidths = append(seqWidths, len(lineDropCR))
				} else {
					qline = false
				}
			}
			thisStart++ //\n
		}
		thisStart++ //\n
	}
	{ // end of file
		id = string(parseHeadID(lastName, this.isUsingDefaultIDRegexp))

		// check lineWidths
		lastLineWidth, chances = -2, 2
		seenSeqs = false
		for i := len(lineWidths) - 1; i >= 0; i-- {
			if !seenSeqs && seqWidths[i] == 0 { // skip empty lines in the end
				continue
			}
			seenSeqs = true

			if lastLineWidth == -2 {
				lastLineWidth = lineWidths[i]
				continue
			}
			if lineWidths[i] != lastLineWidth {
				chances--
				if chances == 0 || lineWidths[i] < lastLineWidth {
					return nil, fmt.Errorf("different line length in sequence: %s. Please format the file with 'seqkit seq'", id)
				}
			}
			lastLineWidth = lineWidths[i]
		}
		// lineWidth = 0
		if len(lineWidths) > 0 {
			lineWidth = lineWidths[0]
		}
		// seqWidth = 0
		if len(seqWidths) > 0 {
			seqWidth = seqWidths[0]
		}

		//if len(line) > 0 && line[len(line)-1] != '\n' {
		//	fmt.Fprintln(os.Stderr, `[WARNING]: newline character ('\n') not detected at end of file, truncated file?`)
		//}

		if iqual > 0 {
			result = append(result, fmt.Sprintf("%s\t%d\t%d\t%d\t%d\t%d", id, seqLen, lastStart, seqWidth, lineWidth, iqual))
		} else {
			result = append(result, fmt.Sprintf("%s\t%d\t%d\t%d\t%d", id, seqLen, lastStart, seqWidth, lineWidth))
		}

	}

	return result, nil
}

func NewFaidxQuery() any {
	return &FaidxQuery{}
}

type FaidxQuery struct {
	base.IMapPartitions[string, string]
	function.IAfterNone
	opts                   bigseqkit.FaidxOptions
	alphabet               *seq.Alphabet
	IDRegexp               *regexp.Regexp
	isUsingDefaultIDRegexp bool
	ranQueries             []*rangeQuery
	rexQueries             []*regexp.Regexp
}

func (this *FaidxQuery) Before(context api.IContext) (err error) {
	this.opts = bigseqkit.StringToOptions[bigseqkit.FaidxOptions](context.Vars()["opts"].(string))
	this.alphabet, err = this.opts.Config.GetAlphabet()
	fai.MapWholeFile = true
	var idRegexp string

	if *this.opts.FullHead {
		idRegexp = "^(.+)$"
	} else {
		idRegexp = *this.opts.Config.IDRegexp
	}

	if idRegexp != defaultIDRegexp {
		if !reCheckIDregexpStr.MatchString(idRegexp) {
			return fmt.Errorf(`regular expression must contain "(" and ")" to capture matched ID. default: %s`, `^([^\s]+)\s?`)
		}
		this.IDRegexp, err = regexp.Compile(idRegexp)
		if err != nil {
			return fmt.Errorf("fail to Compile idRegexp: %s", err)
		}
		this.isUsingDefaultIDRegexp = false
	} else {
		this.IDRegexp = IDRegexp
	}

	regions := make([]string, 0, 256)
	regionFile := *this.opts.RegionFile
	if regionFile != "" {
		var reader *breader.BufferedReader
		reader, err = breader.NewDefaultBufferedReader(regionFile)
		if err != nil {
			return err
		}
		var data interface{}
		var r string
		for chunk := range reader.Ch {
			if chunk.Err != nil {
				return err
			}
			for _, data = range chunk.Data {
				r = data.(string)
				if r == "" {
					continue
				}
				regions = append(regions, r)
			}
		}
		if !*this.opts.Config.Quiet {
			if len(regions) == 0 {
				fmt.Fprintf(os.Stderr, "%d patterns loaded from file", len(regions))
			} else {
				fmt.Fprintf(os.Stderr, "%d patterns loaded from file", len(regions))
			}
		}
	}

	// handle queries
	queries := make([]string, len(regions), len(regions)+len(*this.opts.Regions)-1)
	if len(regions) > 0 {
		copy(queries, regions)
	}
	queries = append(queries, *this.opts.Regions...)
	this.rexQueries = make([]*regexp.Regexp, len(queries))
	this.ranQueries = make([]*rangeQuery, len(queries))

	if *this.opts.UseRegexp {
		for i, query := range queries {
			this.rexQueries[i], err = regexp.Compile(query)
			if err != nil {
				return fmt.Errorf("invalid regular expression: %s", query)
			}
		}
	} else {
		for i := range queries {
			id, begin, end := parseRegion(queries[i])
			if *this.opts.IgnoreCase {
				id = strings.ToLower(id)
			}
			this.ranQueries[i] = &rangeQuery{ID: id, Region: [2]int{begin, end}}
		}
	}
	return nil
}

func (this *FaidxQuery) Call(v1 iterator.IReadIterator[string], context api.IContext) ([]string, error) {
	reader := NewIteratorReader(v1)
	fastxReader, err := fastx.NewReaderFromIO(this.alphabet, reader, *this.opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}
	var record *fastx.Record
	var alphabet *seq.Alphabet
	var re *regexp.Regexp
	var q *rangeQuery
	var id string
	var region [2]int
	var pstart, pend int
	var head string
	var ok bool
	var buffer bytes.Buffer
	var fbuffer *bytes.Buffer
	var text []byte
	var subseq []byte
	var _s *seq.Seq

	result := make([]string, 0, 100)

	for {
		buffer.Reset()
		record, err = fastxReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		id = string(record.ID)

		if *this.opts.UseRegexp {
			region[0] = 1
			region[1] = -1
			for _, re = range this.rexQueries {
				if re.MatchString(id) {
					ok = true
					break
				}
			}
		} else {
			if *this.opts.IgnoreCase {
				id = strings.ToLower(id)
			}
			for _, q = range this.ranQueries {
				if q.ID == id {
					region[0] = q.Region[0]
					region[1] = q.Region[1]
					ok = true
					break
				}
			}
		}

		if !ok {
			continue
		}

		if (region[0] == 1 && region[1] == -1) || (region[0] > 0 && region[1] < 0) { // full record or region like [5, -5].
			pstart, pend, ok = seq.SubLocation(len(record.Seq.Seq), region[0], region[1])
			head = string(parseHeadID(record.Name, this.isUsingDefaultIDRegexp))
			if !ok {
				continue
			}
			buffer.WriteString(fmt.Sprintf(">%s\n", head))
			subseq = record.Seq.Seq[pstart:pend]
		} else if region[0] <= region[1] {
			pstart, pend, ok = seq.SubLocation(len(record.Seq.Seq), region[0], region[1])
			head = string(parseHeadID(record.Name, this.isUsingDefaultIDRegexp))
			if !ok {
				continue
			}
			buffer.WriteString(fmt.Sprintf(">%s:%d-%d\n", head, region[0], region[1]))
			subseq = record.Seq.Seq[pstart:pend]
		} else { // reverse complement sequence
			pstart, pend, ok = seq.SubLocation(len(record.Seq.Seq), region[0], region[1])
			alphabet = this.alphabet
			if alphabet == nil {
				alphabet = seq.DNAredundant
				if bytes.ContainsAny(record.Seq.Seq[pstart:pend], "uU") {
					alphabet = seq.RNAredundant
				}
			}
			_s, err = seq.NewSeqWithoutValidation(alphabet, subseq)
			if err != nil {
				return nil, fmt.Errorf("fail to compute reverse complemente sequence for region: %s:%d-%d", head, region[0], region[1])
			}
			subseq = _s.RevComInplace().Seq

			buffer.WriteString(fmt.Sprintf(">%s:%d-%d\n", head, region[0], region[1]))
		}

		text, fbuffer = wrapByteSlice(subseq, *this.opts.Config.LineWidth, fbuffer)
		buffer.Write(text)

		result = append(result, buffer.String())
	}
	return result, nil
}

func parseHeadID(head []byte, isUsingDefaultIDRegexp bool) []byte {
	if isUsingDefaultIDRegexp {
		if i := bytes.IndexByte(head, ' '); i > 0 {
			return head[0:i]
		}
		if i := bytes.IndexByte(head, '\t'); i > 0 {
			return head[0:i]
		}
		return head
	}

	found := IDRegexp.FindSubmatch(head)
	if found == nil { // not match
		return head
	}
	return found[1]
}

func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}

func dropCRStr(data string) string {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}

func stringSplitNByByte(s string, sep byte, n int, a *[]string) {
	if a == nil {
		tmp := make([]string, n)
		a = &tmp
	}

	n--
	i := 0
	for i < n {
		m := strings.IndexByte(s, sep)
		if m < 0 {
			break
		}
		(*a)[i] = s[:m]
		s = s[m+1:]
		i++
	}
	(*a)[i] = s

	(*a) = (*a)[:i+1]
}

func Read(raw string) (index Index, err error) {
	index = make(map[string]fai.Record)

	items := make([]string, 5)
	var line, name string
	var length int
	var start int64
	var BasesPerLine, bytesPerLine int
	for _, line = range strings.Split(raw, "\n") {
		line = dropCRStr(line)
		stringSplitNByByte(line, '\t', 5, &items)
		if len(items) != 5 {
			return nil, fmt.Errorf("invalid fai records: %s", line)
		}
		name = items[0]

		length, err = strconv.Atoi(items[1])
		if err != nil {
			return nil, fmt.Errorf("invalid fai records: %s", line)
		}

		start, err = strconv.ParseInt(items[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid fai records: %s", line)
		}

		BasesPerLine, err = strconv.Atoi(items[3])
		if err != nil {
			return nil, fmt.Errorf("invalid fai records: %s", line)
		}

		bytesPerLine, err = strconv.Atoi(items[4])
		if err != nil {
			return nil, fmt.Errorf("invalid fai records: %s", line)
		}

		index[name] = fai.Record{
			Name:         name,
			Length:       length,
			Start:        start,
			BasesPerLine: BasesPerLine,
			BytesPerLine: bytesPerLine,
		}
	}

	return
}

var reRegionFull = regexp.MustCompile(`^(.+?):(\-?\d+)\-(\-?\d+)$`)
var reRegionOneBase = regexp.MustCompile(`^(.+?):(\d+)$`)
var reRegionOnlyBegin = regexp.MustCompile(`^(.+?):(\-?\d+)\-$`)
var reRegionOnlyEnd = regexp.MustCompile(`^(.+?):\-(\-?\d+)$`)

func parseRegion(region string) (id string, begin int, end int) {
	var found []string
	if reRegionFull.MatchString(region) {
		found = reRegionFull.FindStringSubmatch(region)
		id = found[1]
		begin, _ = strconv.Atoi(found[2])
		end, _ = strconv.Atoi(found[3])
	} else if reRegionOneBase.MatchString(region) {
		found = reRegionOneBase.FindStringSubmatch(region)
		id = found[1]
		begin, _ = strconv.Atoi(found[2])
		end = begin
	} else if reRegionOnlyBegin.MatchString(region) {
		found = reRegionOnlyBegin.FindStringSubmatch(region)
		id = found[1]
		begin, _ = strconv.Atoi(found[2])
		end = -1
	} else if reRegionOnlyEnd.MatchString(region) {
		found = reRegionOnlyEnd.FindStringSubmatch(region)
		id = found[1]
		begin = 1
		end, _ = strconv.Atoi(found[2])
	} else {
		id = region
		begin, end = 1, -1
	}
	return
}
