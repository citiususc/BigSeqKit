package seqkit

import (
	"ignis/driver/api"
)

type SeqKitFa2FqOptions struct {
	inner Fa2FqOptions
}

type Fa2FqOptions struct {
	Config             KitConfig
	FastaFile          *string
	OnlyPositiveStrand *bool
}

func (this *Fa2FqOptions) setDefaults() *Fa2FqOptions {
	this.Config.setDefaults()
	setDefault(&this.FastaFile, "")
	setDefault(&this.OnlyPositiveStrand, false)

	return this
}

func (this *SeqKitFa2FqOptions) Config(v *SeqKitConfig) *SeqKitFa2FqOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitFa2FqOptions) FastaFile(v string) *SeqKitFa2FqOptions {
	this.inner.FastaFile = &v
	return this
}

func (this *SeqKitFa2FqOptions) OnlyPositiveStrand(v bool) *SeqKitFa2FqOptions {
	this.inner.OnlyPositiveStrand = &v
	return this
}

func Fa2Fq(input *api.IDataFrame[string], o *SeqKitFa2FqOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitFa2FqOptions{}
	}
	opts := o.inner
	opts.setDefaults()
	libprepare, err := api.AddParam(libSource("Fa2Fq"), "opts", OptionsToString(opts))
	if err != nil {
		return nil, err
	}

	return api.MapPartitions[string, string](input, libprepare)
}
