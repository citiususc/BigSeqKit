package bigseqkit

import (
	"ignis/driver/api"
	"ignis/executor/api/ipair"
)

type SeqKitPairOptions struct {
	inner PairOptions
}

type PairOptions struct {
	Config       KitConfig
	SaveUnpaired *bool
}

func (this *PairOptions) setDefaults() *PairOptions {
	this.Config.setDefaults()
	setDefault(&this.SaveUnpaired, false)

	return this
}

func (this *SeqKitPairOptions) Config(v *SeqKitConfig) *SeqKitPairOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitPairOptions) SaveUnpaired(v bool) *SeqKitPairOptions {
	this.inner.SaveUnpaired = &v
	return this
}

func preparePair(input *api.IDataFrame[string], opts *PairOptions, id string) (*api.IDataFrame[ipair.IPair[string, string]], error) {
	libprepare, err := api.AddParam(libSource("PairPrepare"), "opts", OptionsToString(*opts))
	if err != nil {
		return nil, err
	}
	libprepare, err = api.AddParam(libprepare, "id", id)
	if err != nil {
		return nil, err
	}
	return api.MapPartitions[string, ipair.IPair[string, string]](input, libprepare)
}

func commonPair(inputA *api.IDataFrame[string], inputB *api.IDataFrame[string], opts *PairOptions) (*api.IPairDataFrame[string, []string], error) {
	p1, err := preparePair(inputA, opts, "1")
	if err != nil {
		return nil, err
	}
	p2, err := preparePair(inputB, opts, "2")
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
	return grouped, nil
}

func Pair(inputA *api.IDataFrame[string], inputB *api.IDataFrame[string], o *SeqKitPairOptions) (
	pairs *api.IDataFrame[ipair.IPair[string, string]],
	unpaired *api.IDataFrame[ipair.IPair[string, string]],
	cache *api.IPairDataFrame[string, []string],
	err error) {
	if o == nil {
		o = &SeqKitPairOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	cache, err = commonPair(inputA, inputB, &opts)
	if err != nil {
		return
	}
	grouped := cache

	pairs, err = api.Flatmap[ipair.IPair[string, []string], ipair.IPair[string, string]](grouped.FromPair(), libSource("Pair"))
	if err != nil {
		return
	}

	if !*opts.SaveUnpaired {
		return
	}
	libunpaired, err := api.AddParam(libSource("Pair"), "unpaired", true)
	if err != nil {
		return
	}
	unpaired, err = api.Flatmap[ipair.IPair[string, []string], ipair.IPair[string, string]](grouped.FromPair(), libunpaired)

	return
}

func PairIndex(p *api.IDataFrame[ipair.IPair[string, string]], i int) (*api.IDataFrame[string], error) {
	lib, err := api.AddParam(libSource("PairI"), "i", i)
	if err != nil {
		return nil, err
	}
	return api.Map[ipair.IPair[string, string], string](p, lib)
}

func UnpairedId(p *api.IDataFrame[ipair.IPair[string, string]], id string) (*api.IDataFrame[string], error) {
	lib, err := api.AddParam(libSource("PairF"), "id", id)
	if err != nil {
		return nil, err
	}
	f, err := p.Filter(lib)
	if err != nil {
		return nil, err
	}
	return PairIndex(f, 1)
}
