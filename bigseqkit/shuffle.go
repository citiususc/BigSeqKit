package bigseqkit

import (
	"ignis/driver/api"
)

type SeqKitShuffleOptions struct {
	inner ShuffleOptions
}

type ShuffleOptions struct {
	Config KitConfig
	Seed   *int
}

func (this *ShuffleOptions) setDefaults() *ShuffleOptions {
	this.Config.setDefaults()
	setDefault(&this.Seed, 23)

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

func Shuffle(input *api.IDataFrame[string], o *SeqKitShuffleOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitShuffleOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	n, err := input.Partitions()
	if err != nil {
		return nil, err
	}

	return input.PartitionByRandom(n, *opts.Seed)
}
