package seqkit

import (
	"ignis/driver/api"
	"ignis/executor/api/ipair"
)

type SeqKitConcatOptions struct {
	inner ConcatOptions
}

type ConcatOptions struct {
	Config    KitConfig
	Full      *bool
	Separator *string
}

func (this *ConcatOptions) setDefaults() *ConcatOptions {
	this.Config.setDefaults()
	setDefault(&this.Full, false)
	setDefault(&this.Separator, "|")

	return this
}

func (this *SeqKitConcatOptions) Config(v *SeqKitConfig) *SeqKitConcatOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitConcatOptions) Seed(v bool) *SeqKitConcatOptions {
	this.inner.Full = &v
	return this
}

func (this *SeqKitConcatOptions) TwoPass(v string) *SeqKitConcatOptions {
	this.inner.Separator = &v
	return this
}

func prepare(input *api.IDataFrame[string], o *SeqKitConcatOptions, id string) (*api.IDataFrame[ipair.IPair[string, string]], error) {
	libprepare, err := api.AddParam(libSource("ConcatPrepare"), "opts", OptionsToString(o.inner))
	if err != nil {
		return nil, err
	}
	libprepare, err = api.AddParam(libprepare, "id", id)
	if err != nil {
		return nil, err
	}
	return api.MapPartitions[string, ipair.IPair[string, string]](input, libprepare)
}

func Concat(inputA *api.IDataFrame[string], inputB *api.IDataFrame[string], o *SeqKitConcatOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitConcatOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	p1, err := prepare(inputA, o, "1")
	if err != nil {
		return nil, err
	}
	p2, err := prepare(inputB, o, "2")
	if err != nil {
		return nil, err
	}

	u, err := p1.Union(p2, false, nil)
	if err != nil {
		return nil, err
	}

	grouped, err := api.GroupByKey[string, string](api.ToPair[string, string](u), nil)
	if err != nil {
		return nil, err
	}

	join, err := api.AddParam(libSource("ConcatJoin"), "opts", OptionsToString(o.inner))
	if err != nil {
		return nil, err
	}

	return api.Flatmap[ipair.IPair[string, []string], string](grouped.FromPair(), join)
}
