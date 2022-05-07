package main

import (
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	"io"
	"regexp"
)

var reRegion = regexp.MustCompile(`\-?\d+:\-?\d+`)

func NewReadFixer() any {
	return &GrepValueMatched{}
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
