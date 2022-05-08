package main

import (
	"bytes"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	"io"
	"regexp"
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
	base.IMap[string, string]
	function.IAfterNone
	delim string
}

func (this *ReadFixer) Before(context api.IContext) (err error) {
	this.delim = context.Vars()["delim"].(string)
	return nil
}

func (this *ReadFixer) Call(v string, context api.IContext) (string, error) {
	return this.delim + v[:len(v)-1], nil
}

func NewIteratorReader(it iterator.IReadIterator[string]) io.Reader {
	return &IteratorReader{
		it: it,
	}
}

type IteratorReader struct {
	it     iterator.IReadIterator[string]
	buffer string
	pos    int
}

func (this *IteratorReader) Read(p []byte) (n int, err error) {
	if this.pos == len(this.buffer) {
		if this.it.HasNext() {
			this.buffer, err = this.it.Next()
			this.pos = 0
			if err != nil {
				return
			}
		} else {
			err = io.EOF
			return
		}
	}
	n = copy(p, this.buffer[this.pos:])
	this.pos += n

	return
}

func NewArrayReader(array []string) io.Reader {
	return &ArrayReader{array: array}
}

type ArrayReader struct {
	array []string
	i     int
	pos   int
}

func (this *ArrayReader) Read(p []byte) (n int, err error) {
	if this.pos == len(this.array[this.i]) {
		if this.i < len(this.array) {
			this.pos = 0
			this.i++
		} else {
			err = io.EOF
			return
		}
	}
	n = copy(p, this.array[this.i][this.pos:])
	this.pos += n

	return
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
