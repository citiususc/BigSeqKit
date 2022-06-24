package bigseqkit

import (
	"ignis/driver/api"
	"ignis/executor/api/ipair"
)

type SeqKitPairOptions struct {
	inner PairOptions
}

type PairOptions struct {
	Config KitConfig
}

func (this *PairOptions) setDefaults() *PairOptions {
	this.Config.setDefaults()

	return this
}

func (this *SeqKitPairOptions) Config(v *SeqKitConfig) *SeqKitPairOptions {
	this.inner.Config = v.inner
	return this
}

func preparePair(input *api.IDataFrame[string], opts *ConcatOptions, id string) (*api.IDataFrame[ipair.IPair[string, string]], error) {
	libprepare, err := api.AddParam(libSource("pairPrepare"), "opts", OptionsToString(*opts))
	if err != nil {
		return nil, err
	}
	libprepare, err = api.AddParam(libprepare, "id", id)
	if err != nil {
		return nil, err
	}
	return api.MapPartitions[string, ipair.IPair[string, string]](input, libprepare)
}

func Pair(inputA *api.IDataFrame[string], inputB *api.IDataFrame[string], o *SeqKitConcatOptions) (*api.IDataFrame[ipair.IPair[string, string]], error) {
	if o == nil {
		o = &SeqKitConcatOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	p1, err := preparePair(inputA, &opts, "1")
	if err != nil {
		return nil, err
	}
	p2, err := preparePair(inputB, &opts, "2")
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

	return api.Flatmap[ipair.IPair[string, []string], ipair.IPair[string, string]](grouped.FromPair(), libSource("Pair"))
}
