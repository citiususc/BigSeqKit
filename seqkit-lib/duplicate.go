package main

import (
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
)

func NewDuplicate() any {
	return &Duplicate{}
}

type Duplicate struct {
	base.IFlatmap[string, string]
	function.IAfterNone
	times int64
}

func (this *Duplicate) Before(context api.IContext) (err error) {
	this.times = context.Vars()["times"].(int64)
	return err
}

func (this *Duplicate) Call(v string, context api.IContext) ([]string, error) {
	result := make([]string, this.times)
	for i := int64(0); i < this.times; i++ {
		result[i] = v
	}
	return result, nil
}
