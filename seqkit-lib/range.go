package main

import (
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
)

func NewRangePrepare() any {
	return &RangePrepare{}
}

type RangePrepare struct {
	base.IMapWithIndex[string, string]
	function.IAfterNone
	start, end int64
}

func (this *RangePrepare) Before(context api.IContext) (err error) {
	this.start = context.Vars()["start"].(int64)
	this.end = context.Vars()["end"].(int64)
	return nil
}

func (this *RangePrepare) Call(v1 int64, v2 string, context api.IContext) (string, error) {
	if this.start <= v1 && v1 < this.end {
		return v2, nil
	}
	return "", nil
}

func NewRangeFilter() any {
	return &RangeFilter{}
}

type RangeFilter struct {
	base.IFilter[string]
	function.IOnlyCall
}

func (this *RangeFilter) Call(v string, context api.IContext) (bool, error) {
	return len(v) > 0, nil
}
