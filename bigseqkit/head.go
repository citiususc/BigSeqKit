package bigseqkit

import (
	"ignis/driver/api"
	"strconv"
)

type SeqKitHeadOptions struct {
	inner HeadOptions
}

type HeadOptions struct {
	Config KitConfig
	N      *int64
}

func (this *HeadOptions) setDefaults() *HeadOptions {
	this.Config.setDefaults()
	setDefault(&this.N, 10)

	return this
}

func (this *SeqKitHeadOptions) Config(v *SeqKitConfig) *SeqKitHeadOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitHeadOptions) N(v int64) *SeqKitHeadOptions {
	this.inner.N = &v
	return this
}

func Head(input *api.IDataFrame[string], o *SeqKitHeadOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitHeadOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	oRange := (&SeqKitRangeOptions{}).Range("1:" + strconv.FormatInt(*opts.N, 10))
	oRange.inner.Config = opts.Config

	return Range(input, oRange)
}
