package seqkit

import "ignis/driver/api"

type SeqKitDuplicateOptions struct {
	inner DuplicateOptions
}

type DuplicateOptions struct {
	Config KitConfig
	Times  *int64
}

func (this *DuplicateOptions) setDefaults() *DuplicateOptions {
	this.Config.setDefaults()
	setDefault(&this.Times, 1)

	return this
}

func (this *SeqKitDuplicateOptions) Config(v *SeqKitConfig) *SeqKitDuplicateOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitDuplicateOptions) Times(v int64) *SeqKitDuplicateOptions {
	this.inner.Times = &v
	return this
}

func Duplicate(input *api.IDataFrame[string], o *SeqKitDuplicateOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitDuplicateOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	dup, err := api.AddParam[int64](libSource("Duplicate"), "times", *opts.Times)
	if err != nil {
		return nil, err
	}

	return api.Flatmap[string, string](input, dup)
}
