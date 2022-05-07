package seqkit

import (
	"ignis/driver/api"
)

type SeqKitShuffleOptions struct {
	inner ShuffleOptions
}

type ShuffleOptions struct {
	Config  KitConfig
	Seed    *int
	TwoPass *bool
}

func (this *ShuffleOptions) setDefaults() *ShuffleOptions {
	this.Config.setDefaults()
	setDefault(&this.Seed, 23)
	setDefault(&this.TwoPass, false)

	return this
}

func (this *SeqKitShuffleOptions) Config(v *SeqKitConfig) *SeqKitShuffleOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitShuffleOptions) Seed(v int) *SeqKitShuffleOptions {
	this.inner.Seed = &v
	return this
}

func (this *SeqKitShuffleOptions) TwoPass(v bool) *SeqKitShuffleOptions {
	this.inner.TwoPass = &v
	return this
}

func Shuffle(input *api.IDataFrame[string], o *SeqKitShuffleOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitShuffleOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	if !*opts.TwoPass {
		if err := input.Cache(api.PRESERVE); err != nil {
			return nil, err
		}
	}

	n, err := input.Partitions()
	if err != nil {
		return nil, err
	}

	return input.PartitionByRandom(n, *opts.Seed)
}
