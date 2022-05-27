package seqkit

import (
	"ignis/driver/api"
	"ignis/executor/api/ipair"
	"strconv"
)

type SeqKitCommonOptions struct {
	inner CommonOptions
}

type CommonOptions struct {
	Config             KitConfig
	ByName             *bool
	BySeq              *bool
	IgnoreCase         *bool
	OnlyPositiveStrand *bool
}

func (this *CommonOptions) setDefaults() *CommonOptions {
	this.Config.setDefaults()
	setDefault(&this.ByName, false)
	setDefault(&this.BySeq, false)
	setDefault(&this.IgnoreCase, false)
	setDefault(&this.OnlyPositiveStrand, false)

	return this
}

func (this *SeqKitCommonOptions) Config(v *SeqKitConfig) *SeqKitCommonOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitCommonOptions) ByName(v bool) *SeqKitCommonOptions {
	this.inner.ByName = &v
	return this
}

func (this *SeqKitCommonOptions) BySeq(v bool) *SeqKitCommonOptions {
	this.inner.BySeq = &v
	return this
}

func (this *SeqKitCommonOptions) IgnoreCase(v bool) *SeqKitCommonOptions {
	this.inner.IgnoreCase = &v
	return this
}

func (this *SeqKitCommonOptions) OnlyPositiveStrand(v bool) *SeqKitCommonOptions {
	this.inner.OnlyPositiveStrand = &v
	return this
}

func prepareCommon(input *api.IDataFrame[string], opts *CommonOptions, id string) (*api.IDataFrame[ipair.IPair[int64, string]], error) {
	libprepare, err := api.AddParam(libSource("CommonPrepare"), "opts", OptionsToString(*opts))
	if err != nil {
		return nil, err
	}
	libprepare, err = api.AddParam(libprepare, "id", id)
	if err != nil {
		return nil, err
	}
	return api.MapPartitions[string, ipair.IPair[int64, string]](input, libprepare)
}

func Common(inputA *api.IDataFrame[string], inputB *api.IDataFrame[string], o *SeqKitCommonOptions, inputN ...*api.IDataFrame[string]) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitCommonOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	inputs := make([]*api.IDataFrame[string], 0, len(inputN)+1)
	inputs = append(inputs, inputB)
	inputs = append(inputs, inputN...)

	u, err := prepareCommon(inputA, &opts, "1")
	if err != nil {
		return nil, err
	}

	for i, input := range inputs {

		pn, err := prepareCommon(input, &opts, strconv.Itoa(i+2))
		if err != nil {
			return nil, err
		}

		u, err = u.Union(pn, false, nil)
		if err != nil {
			return nil, err
		}
	}

	grouped, err := api.GroupByKey[int64, string](api.ToPair[int64, string](u), nil)
	if err != nil {
		return nil, err
	}

	join, err := api.AddParam(libSource("CommonJoin"), "opts", OptionsToString(opts))
	if err != nil {
		return nil, err
	}
	join, err = api.AddParam(join, "ids", len(inputN)+1)
	if err != nil {
		return nil, err
	}

	return api.Flatmap[ipair.IPair[int64, []string], string](grouped.FromPair(), join)
}
