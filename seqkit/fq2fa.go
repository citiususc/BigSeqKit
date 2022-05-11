package seqkit

import (
	"ignis/driver/api"
)

type SeqKitFq2FaOptions struct {
	inner Fq2FaOptions
}

type Fq2FaOptions struct {
	Config KitConfig
}

func (this *Fq2FaOptions) setDefaults() *Fq2FaOptions {
	this.Config.setDefaults()
	return this
}

func (this *SeqKitFq2FaOptions) Config(v *SeqKitConfig) *SeqKitFq2FaOptions {
	this.inner.Config = v.inner
	return this
}

func Fq2Fa(input *api.IDataFrame[string], o *SeqKitFq2FaOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitFq2FaOptions{}
	}
	opts := o.inner
	opts.setDefaults()
	libprepare, err := api.AddParam(libSource("Fq2Fa"), "opts", OptionsToString(o.inner))
	if err != nil {
		return nil, err
	}

	return api.MapPartitions[string, string](input, libprepare)
}
