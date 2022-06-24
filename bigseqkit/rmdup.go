package bigseqkit

import (
	"fmt"
	"ignis/driver/api"
	"ignis/executor/api/ipair"
)

type SeqKitRmDupOptions struct {
	inner RmDupOptions
}

type RmDupOptions struct {
	Config             KitConfig
	ByName             *bool
	BySeq              *bool
	IgnoreCase         *bool
	DupSeqsFile        *string
	DupNumFile         *string
	OnlyPositiveStrand *bool
}

func (this *RmDupOptions) setDefaults() *RmDupOptions {
	this.Config.setDefaults()
	setDefault(&this.ByName, false)
	setDefault(&this.BySeq, false)
	setDefault(&this.IgnoreCase, false)
	setDefault(&this.DupSeqsFile, "")
	setDefault(&this.DupNumFile, "")
	setDefault(&this.OnlyPositiveStrand, false)

	return this
}

func (this *SeqKitRmDupOptions) Config(v *SeqKitConfig) *SeqKitRmDupOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitRmDupOptions) ByName(v bool) *SeqKitRmDupOptions {
	this.inner.ByName = &v
	return this
}

func (this *SeqKitRmDupOptions) BySeq(v bool) *SeqKitRmDupOptions {
	this.inner.BySeq = &v
	return this
}

func (this *SeqKitRmDupOptions) IgnoreCase(v bool) *SeqKitRmDupOptions {
	this.inner.IgnoreCase = &v
	return this
}

func (this *SeqKitRmDupOptions) DupSeqsFile(v string) *SeqKitRmDupOptions {
	this.inner.DupSeqsFile = &v
	return this
}

func (this *SeqKitRmDupOptions) DupNumFile(v string) *SeqKitRmDupOptions {
	this.inner.DupNumFile = &v
	return this
}

func (this *SeqKitRmDupOptions) OnlyPositiveStrand(v bool) *SeqKitRmDupOptions {
	this.inner.OnlyPositiveStrand = &v
	return this
}

func RmDup(input *api.IDataFrame[string], o *SeqKitRmDupOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitRmDupOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	revcom := !*opts.OnlyPositiveStrand

	if *opts.BySeq && *opts.ByName {
		return nil, fmt.Errorf("only one/none of the flags -s (--by-seq) and -n (--by-name) is allowed")
	}

	if !revcom && !*opts.BySeq {
		return nil, fmt.Errorf("flag -s (--by-seq) needed when using -P (--only-positive-strand)")
	}

	prepare, err := api.AddParam(libSource("RmDupPrepare"), "opts", OptionsToString(opts))
	if err != nil {
		return nil, err
	}

	prepared, err := api.MapPartitions[string, ipair.IPair[int64, string]](input, prepare)
	if err != nil {
		return nil, err
	}

	grouped, err := api.GroupByKey[int64, string](api.ToPair[int64, string](prepared), nil)
	if err != nil {
		return nil, err
	}

	check, err := api.AddParam(libSource("RmDupCheck"), "opts", OptionsToString(opts))
	if err != nil {
		return nil, err
	}

	return api.Flatmap[ipair.IPair[int64, []string], string](grouped.FromPair(), check)
}
